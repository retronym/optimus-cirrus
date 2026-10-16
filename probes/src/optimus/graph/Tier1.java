package optimus.graph;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;

/**
 * Tier 1 probes (TEST_CAMPAIGN.md): the real OGScheduler end to end, with state primed to the edge
 * of a reported hazard. Self-contained (no dependency on Probes/Probes2) so it compiles on its own.
 * Every probe runs its scenario in child JVMs: `--child <probe> <variant>`; the parent launches
 * them via PROBES_JAVA_CMD with -D flags inserted after "java".
 */
public final class Tier1 {

  enum Outcome {
    DEMONSTRATED,
    NOT_REPRODUCED,
    ERROR
  }

  record Result(String id, Outcome outcome, String detail) {}

  // ---------------------------------------------------------------- helpers

  static Field field(Class<?> c, String name) throws Exception {
    Field f = c.getDeclaredField(name);
    f.setAccessible(true);
    return f;
  }

  static OGScheduler defaultScheduler() {
    return (OGScheduler) EvaluationContext.scheduler();
  }

  static int threadIdOf(OGSchedulerContext ec) {
    try {
      Object ctx = field(OGSchedulerContext.class, "_context").get(ec);
      return field(OGScheduler.Context.class, "threadID").getInt(ctx);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void dumpAndExit(String why, OGScheduler s, int code) {
    System.out.println("CHILD-HANG " + why);
    try {
      System.out.println("---- OGScheduler.toString()");
      System.out.println(s.toString());
    } catch (Throwable t) {
      System.out.println("scheduler toString threw " + t);
    }
    Thread.getAllStackTraces()
        .forEach(
            (th, st) -> {
              String n = th.getName();
              if (n.startsWith("t1-") || n.startsWith("t2-") || n.startsWith("t8-") || n.equals("main") || n.toLowerCase().contains("optimus")) {
                System.out.println("--- " + n + " " + th.getState());
                for (int i = 0; i < Math.min(18, st.length); i++) System.out.println("    " + st[i]);
              }
            });
    System.out.flush();
    Runtime.getRuntime().halt(code);
  }

  /** Child process outcome. */
  record Child(int exit, List<String> lines, long wallMs, boolean timedOut) {
    String kv(String key) {
      for (String l : lines) {
        int i = l.indexOf(key + "=");
        if (l.startsWith("KV ") && i >= 0) {
          int e = l.indexOf(' ', i);
          return l.substring(i + key.length() + 1, e < 0 ? l.length() : e);
        }
      }
      return null;
    }

    long count(String needle) {
      return lines.stream().filter(l -> l.contains(needle)).count();
    }

    List<String> grep(String... needles) {
      List<String> r = new ArrayList<>();
      for (String l : lines) for (String n : needles) if (l.contains(n)) { r.add(l.length() > 220 ? l.substring(0, 220) + "..." : l); break; }
      return r;
    }
  }

  static Child runChild(String probe, String variant, String dflags, int timeoutSec) throws Exception {
    String cmd = System.getenv("PROBES_JAVA_CMD");
    if (cmd == null) throw new IllegalStateException("PROBES_JAVA_CMD not set; run via run-tier1.sh");
    if (!cmd.startsWith("java ")) throw new IllegalStateException("PROBES_JAVA_CMD must start with 'java '");
    String full = "java " + dflags + " " + cmd.substring(5) + " --child " + probe + " " + variant;
    long t0 = System.nanoTime();
    Process p = new ProcessBuilder("sh", "-c", full).redirectErrorStream(true).start();
    List<String> lines = java.util.Collections.synchronizedList(new ArrayList<>());
    Thread reader =
        new Thread(
            () -> {
              try (var r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String l;
                while ((l = r.readLine()) != null) lines.add(l);
              } catch (Exception ignored) {
              }
            });
    reader.start();
    boolean done = p.waitFor(timeoutSec, TimeUnit.SECONDS);
    if (!done) p.destroyForcibly().waitFor();
    reader.join(5000);
    long wall = (System.nanoTime() - t0) / 1_000_000;
    String log = System.getenv("TIER1_LOGDIR");
    if (log != null) {
      var path = java.nio.file.Path.of(log, probe + "-" + variant + "-" + dflags.replaceAll("[^A-Za-z0-9=.]+", "_") + ".log");
      java.nio.file.Files.write(path, new ArrayList<>(lines));
    }
    return new Child(done ? p.exitValue() : -1, new ArrayList<>(lines), wall, !done);
  }

  // ---------------------------------------------------------------- nodes

  /** Base node carrying a long value. Attached at construction. */
  abstract static class N extends NodeTask {
    long value;
    final String name;

    N(String name, ScenarioStack ss) {
      this.name = name;
      attach(ss);
    }

    void completeNow(EvaluationQueue eq) {
      complete(eq);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static final class Leaf extends N {
    Leaf(ScenarioStack ss) {
      super("Leaf", ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      value = 1;
      complete(ec);
    }
  }

  static final Set<Integer> seenThreadIds = ConcurrentHashMap.newKeySet();

  static N tree(int depth, int fanout, String shape, int level, ScenarioStack ss) {
    if (depth == 0) return new Leaf(ss);
    if (shape.equals("fan")) {
      // Sync -> CountFan (async, waits on all children at once) -> Fsm -> Sync ...
      return switch (level % 3) {
        case 0 -> new Sync(depth, fanout, shape, level, ss);
        case 1 -> new CountFan(depth, fanout, shape, level, ss);
        default -> new Fsm(depth, fanout, shape, level, ss);
      };
    }
    boolean sync = shape.equals("sync") || (shape.equals("mixed") ? level % 2 == 0 : level % 3 == 0);
    return sync ? new Sync(depth, fanout, shape, level, ss) : new Fsm(depth, fanout, shape, level, ss);
  }

  /** Enqueues all children, then runAndWait on each from inside run(): a sync stack per level. */
  static final class Sync extends N {
    final int depth, fanout, level;
    final String shape;

    Sync(int depth, int fanout, String shape, int level, ScenarioStack ss) {
      super("Sync" + depth, ss);
      this.depth = depth;
      this.fanout = fanout;
      this.shape = shape;
      this.level = level;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      seenThreadIds.add(threadIdOf(ec));
      N[] cs = new N[fanout];
      for (int i = 0; i < fanout; i++) {
        cs[i] = tree(depth - 1, fanout, shape, level + 1, scenarioStack());
        ec.enqueue(this, cs[i]);
      }
      long sum = 0;
      for (N c : cs) {
        ec.runAndWait(c);
        sum += c.value;
      }
      value = sum;
      complete(ec);
    }
  }

  /**
   * FSM-style async node: enqueues children, then waits for them one at a time with
   * continueWith(this); NodeTask's default onChildCompleted re-enqueues this node, which by then
   * carries the causality stamp of the thread that last ran it.
   */
  static final class Fsm extends N {
    final int depth, fanout, level;
    final String shape;
    N[] cs;
    int idx;

    Fsm(int depth, int fanout, String shape, int level, ScenarioStack ss) {
      super("Fsm" + depth, ss);
      this.depth = depth;
      this.fanout = fanout;
      this.shape = shape;
      this.level = level;
    }

    volatile boolean rejected;

    @Override
    public void run(OGSchedulerContext ec) {
      if (rejected && T1_TRACE && t1Traced.getAndIncrement() < 3)
        new Exception("T1 rejected Fsm re-run on thread " + threadIdOf(ec)).printStackTrace(System.out);
      rejected = false;
      if (cs == null) {
        cs = new N[fanout];
        for (int i = 0; i < fanout; i++) {
          cs[i] = tree(depth - 1, fanout, shape, level + 1, scenarioStack());
          ec.enqueue(this, cs[i]);
        }
      }
      while (idx < fanout) {
        if (!cs[idx].isDone()) {
          cs[idx].continueWith(this, ec);
          return;
        }
        idx++;
      }
      long sum = 0;
      for (N c : cs) sum += c.value;
      value = sum;
      complete(ec);
    }

    /** Counts re-enqueues where the enqueuing thread would refuse its own work (R1 symptom). */
    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      if (eq instanceof OGSchedulerContext ec) {
        try {
          int cid = field(OGSchedulerContext.class, "currentCausalityID").getInt(ec);
          int tid = threadIdOf(ec);
          if (cid != 0) {
            t1Reenqueues.incrementAndGet();
            if (!OGScheduler.canTake(this, tid, cid)) {
              t1SelfRejects.incrementAndGet();
              rejected = true;
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      super.onChildCompleted(eq, child);
    }
  }

  /**
   * Async fan-out registering on every child at once (Smoke's async mode). Its waitingOn ends up
   * pointing at one child only, so the other children are off the forward chain.
   */
  static final class CountFan extends N {
    final int depth, fanout, level;
    final String shape;
    final AtomicInteger pending;
    N[] cs;

    CountFan(int depth, int fanout, String shape, int level, ScenarioStack ss) {
      super("CountFan" + depth, ss);
      this.depth = depth;
      this.fanout = fanout;
      this.shape = shape;
      this.level = level;
      this.pending = new AtomicInteger(fanout);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      cs = new N[fanout];
      for (int i = 0; i < fanout; i++) {
        cs[i] = tree(depth - 1, fanout, shape, level + 1, scenarioStack());
        ec.enqueue(this, cs[i]);
      }
      for (N c : cs) c.continueWith(this, ec);
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      if (pending.decrementAndGet() == 0) {
        long sum = 0;
        for (N c : cs) sum += c.value;
        value = sum;
        complete(eq);
      }
    }
  }

  static final boolean T1_TRACE = Boolean.getBoolean("t1.trace");
  static final AtomicInteger t1Traced = new AtomicInteger();
  static final AtomicLong t1Reenqueues = new AtomicLong(), t1SelfRejects = new AtomicLong();

  static void captureQueue(OGSchedulerContext ec, Map<String, Object> st) {
    try {
      Object q = field(OGSchedulerContext.class, "localQ").get(ec);
      Class<?> wq = q.getClass();
      int top = field(wq, "_top").getInt(q), base = field(wq, "_base").getInt(q);
      int marker = field(OGSchedulerContext.class, "currentLocalQueueMarker").getInt(ec);
      int cid = field(OGSchedulerContext.class, "currentCausalityID").getInt(ec);
      st.put("top", top);
      st.put("base", base);
      st.put("size", top - base);
      st.put("marker", marker);
      st.put("causality", cid);
      st.put("ownerFenced", marker > top - 1);
    } catch (Exception e) {
      st.put("captureError", e.toString());
    }
  }

  // ---------------------------------------------------------------- T1

  static Result t1() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demonstrated = false;
    String[] primes = {"none", "32760", "32768", "65530", "65536"};
    // {ideal, clients, shape, repetitions}
    String[][] configs = {
      {"2", "4", "sync", "1"}, {"2", "4", "mixed", "1"}, {"2", "4", "fsm2", "1"}, {"2", "4", "fan", "3"},
      {"2", "8", "fan", "2"}, {"1", "1", "fan", "1"}, {"1", "4", "fan", "1"}
    };
    for (String asserts : new String[] {"false", "true"}) {
      for (String[] cfg : configs) {
        String flags = "-Doptimus.gthread.ideal=" + cfg[0] + " -Doptimus.graph.limitThreads=true -Doptimus.scheduler.asserts=" + asserts
            + " -Dt1.clients=" + cfg[1] + " -Dt1.timeoutSec=10";
        long baseMax = -1;
        for (String prime : primes) {
          StringBuilder runs = new StringBuilder();
          int hangs = 0, n = Integer.parseInt(cfg[3]);
          String ids = null, rej = null;
          for (int rep = 0; rep < n; rep++) {
            Child c = runChild("t1", prime + ":" + cfg[2], flags, 60);
            boolean hang = c.timedOut || c.exit == 3 || c.count("CHILD-HANG") > 0;
            boolean invalid = c.count("GraphInInvalidState") > 0;
            if (hang) {
              hangs++;
              runs.append(" HANG");
              if (hangs == 1) for (String l : c.grep("CHILD-HANG", "global quick queue", "Tier1.Fsm:", "waitThreads", "idleThreads", "workThreads")) runs.append("\n      | ").append(l.trim());
            } else {
              long mt = Long.parseLong(c.kv("maxTreeMs"));
              if (prime.equals("none")) baseMax = Math.max(baseMax, mt);
              runs.append(' ').append(c.kv("ms")).append("ms(maxTree ").append(mt).append(")");
              if (!"true".equals(c.kv("ok"))) runs.append(" WRONG");
              if (!prime.equals("none") && mt > 900 && mt > 5 * Math.max(baseMax, 1)) demonstrated = true;
              ids = c.kv("ids");
              rej = c.kv("selfRejects") + "/" + c.kv("reenq");
            }
            if (invalid) runs.append(" GraphInInvalidState");
            if (!prime.equals("none") && (hang || invalid || (c.kv("ok") != null && !"true".equals(c.kv("ok"))))) demonstrated = true;
          }
          sb.append(String.format("%n  [asserts=%s ideal=%s clients=%s %s prime=%s] hangs=%d/%d selfRejected/reenqueued(cid>0)=%s ids=%s runs:%s",
              asserts, cfg[0], cfg[1], cfg[2], prime, hangs, n, rej, ids, runs));
        }
      }
    }
    return new Result("T1", demonstrated ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  /** variant = prime:shape. prime: none | counter value. */
  static void t1Child(String variant) throws Exception {
    String[] v = variant.split(":");
    String prime = v[0], shape = v[1];
    int clients = Integer.getInteger("t1.clients", 4), iters = Integer.getInteger("t1.iters", 200);
    int depth = Integer.getInteger("t1.depth", 5), fanout = Integer.getInteger("t1.fanout", 4);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    AtomicInteger counter = (AtomicInteger) field(OGScheduler.class, "threadCounter").get(s);
    System.out.println("KV mainId=" + threadIdOf(OGSchedulerContext.current()) + " counterBefore=" + counter.get()
        + " ideal=" + field(OGScheduler.class, "_idealCount").get(s) + " limitThreads=" + Settings.limitThreads + " asserts=" + Settings.schedulerAsserts);
    if (!prime.equals("none")) counter.set(Integer.parseInt(prime));
    long expected = (long) Math.pow(fanout, depth);
    CountDownLatch latch = new CountDownLatch(clients);
    AtomicLong maxTreeNs = new AtomicLong(), bad = new AtomicLong();
    long t0 = System.nanoTime();
    for (int ci = 0; ci < clients; ci++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  EvaluationContext.initializeWithoutRuntime();
                  OGSchedulerContext ec = OGSchedulerContext.current();
                  ScenarioStack ss = EvaluationContext.scenarioStack();
                  for (int it = 0; it < iters; it++) {
                    long ts = System.nanoTime();
                    N root = new Sync(depth, fanout, shape, 0, ss);
                    ec.runAndWait(root);
                    long dt = System.nanoTime() - ts;
                    maxTreeNs.accumulateAndGet(dt, Math::max);
                    if (root.isDoneWithException()) {
                      bad.incrementAndGet();
                      System.out.println("tree failed: " + root.exception());
                    } else if (root.value != expected) {
                      bad.incrementAndGet();
                      System.out.println("wrong sum " + root.value + " != " + expected);
                    }
                  }
                } catch (Throwable e) {
                  bad.incrementAndGet();
                  System.out.println("client died: " + e);
                  e.printStackTrace(System.out);
                } finally {
                  latch.countDown();
                }
              },
              "t1-client-" + ci);
      t.setDaemon(true);
      t.start();
    }
    if (!latch.await(Integer.getInteger("t1.timeoutSec", 45), TimeUnit.SECONDS))
      dumpAndExit("t1 not done; reenq=" + t1Reenqueues.get() + " selfRejects=" + t1SelfRejects.get() + " max tree so far " + maxTreeNs.get() / 1_000_000 + " ms", s, 3);
    long ms = (System.nanoTime() - t0) / 1_000_000;
    System.out.println("KV ms=" + ms + " maxTreeMs=" + maxTreeNs.get() / 1_000_000 + " ok=" + (bad.get() == 0)
        + " ids=" + new TreeSet<>(seenThreadIds).toString().replace(" ", "") + " counterAfter=" + counter.get() + " reenq=" + t1Reenqueues.get() + " selfRejects=" + t1SelfRejects.get());
    System.out.flush();
    Runtime.getRuntime().halt(bad.get() == 0 ? 0 : 1);
  }

  // ---------------------------------------------------------------- T2

  static Result t2() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demonstrated = false;
    for (String ideal : new String[] {"1", "2"}) {
      for (String prime : new String[] {"none", "primed"}) {
        Child c = runChild("t2", prime, "-Doptimus.gthread.ideal=" + ideal + " -Doptimus.graph.limitThreads=true", 60);
        boolean hang = c.timedOut || c.exit == 3;
        sb.append(String.format("[ideal=%s %s] exit=%d ms=%s ok=%s queueAfterPush{top=%s base=%s size=%s marker=%s causality=%s ownerFenced=%s} leavesByOwner=%s/%s%s; ",
            ideal, prime, c.exit, c.kv("ms"), c.kv("ok"), c.kv("top"), c.kv("base"), c.kv("size"), c.kv("marker"), c.kv("causality"), c.kv("ownerFenced"),
            c.kv("byOwner"), c.kv("leaves"), hang ? " HANG(" + c.kv("hangAfterMs") + "ms, leavesRun=" + c.kv("leavesRunAtHang") + ")" : ""));
        if (prime.equals("primed") && (hang || "true".equals(c.kv("ownerFenced")))) demonstrated = true;
      }
    }
    return new Result("T2", demonstrated ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  static final AtomicInteger t2LeavesRun = new AtomicInteger(), t2ByOwner = new AtomicInteger();
  static volatile Thread t2Owner;

  static final class T2Leaf extends N {
    T2Leaf(ScenarioStack ss) {
      super("T2Leaf", ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      value = 1;
      t2LeavesRun.incrementAndGet();
      if (Thread.currentThread() == t2Owner && t2ByOwner.incrementAndGet() == 1 && Boolean.getBoolean("t2.trace"))
        new Exception("T2 first leaf run by owner").printStackTrace(System.out);
      complete(ec);
    }
  }

  /** root: enqueue K FanOut children (no crossing yet), then runAndWait each. */
  static final class T2Root extends N {
    final int k, g;
    final Map<String, Object> st;
    final List<NodeTask> children = new ArrayList<>();

    T2Root(int k, int g, ScenarioStack ss, Map<String, Object> st) {
      super("T2Root", ss);
      this.k = k;
      this.g = g;
      this.st = st;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      List<N> cs = new ArrayList<>();
      for (int i = 0; i < k; i++) {
        N c = new T2Fan(g, scenarioStack(), i == 0 ? st : null);
        cs.add(c);
        ec.enqueue(this, c);
      }
      long sum = 0;
      for (N c : cs) {
        ec.runAndWait(c); // sync frame: marker = top (still below MAX); c's pushes cross MAX
        sum += c.value;
      }
      value = sum;
      complete(ec);
    }
  }

  static final class T2Fan extends N {
    final int n;
    final AtomicInteger pending;
    final T2Leaf[] cs;
    final Map<String, Object> st;

    T2Fan(int n, ScenarioStack ss, Map<String, Object> st) {
      super("T2Fan", ss);
      this.n = n;
      this.pending = new AtomicInteger(n);
      this.cs = new T2Leaf[n];
      this.st = st;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      for (int i = 0; i < n; i++) {
        cs[i] = new T2Leaf(scenarioStack());
        ec.enqueue(this, cs[i]);
      }
      if (st != null) captureQueue(ec, st);
      for (T2Leaf c : cs) c.continueWith(this, ec);
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      if (pending.decrementAndGet() == 0) {
        long s = 0;
        for (T2Leaf c : cs) s += c.value;
        value = s;
        complete(eq);
      }
    }
  }

  static void t2Child(String variant) throws Exception {
    int k = 4, g = Integer.getInteger("t2.fan", 200);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    OGSchedulerContext ec = OGSchedulerContext.current();
    t2Owner = Thread.currentThread();
    Object q = field(OGSchedulerContext.class, "localQ").get(ec);
    if (variant.equals("primed")) {
      // root pushes k=4 (top = MAX-6), runAndWait(child 0) captures marker = MAX-6, child 0's 6th push hits MAX -> resetBounds
      field(q.getClass(), "_top").setInt(q, Integer.MAX_VALUE - 10);
      field(q.getClass(), "_base").setInt(q, Integer.MAX_VALUE - 10);
    }
    System.out.println("KV ideal=" + field(OGScheduler.class, "_idealCount").get(s) + " limitThreads=" + Settings.limitThreads);
    Map<String, Object> st = new ConcurrentHashMap<>();
    T2Root root = new T2Root(k, g, EvaluationContext.scenarioStack(), st);
    long t0 = System.nanoTime();
    Thread wd =
        new Thread(
            () -> {
              try {
                Thread.sleep(Integer.getInteger("t2.timeoutSec", 20) * 1000L);
              } catch (InterruptedException e) {
                return;
              }
              StringBuilder kv = new StringBuilder("KV");
              st.forEach((key, val) -> kv.append(' ').append(key).append('=').append(val));
              System.out.println(kv + " hangAfterMs=" + (System.nanoTime() - t0) / 1_000_000 + " leavesRunAtHang=" + t2LeavesRun.get() + " byOwner=" + t2ByOwner.get() + " leaves=" + (k * g));
              dumpAndExit("t2 root not done", s, 3);
            });
    wd.setDaemon(true);
    wd.start();
    ec.runAndWait(root);
    long ms = (System.nanoTime() - t0) / 1_000_000;
    StringBuilder kv = new StringBuilder("KV");
    st.forEach((key, val) -> kv.append(' ').append(key).append('=').append(val));
    System.out.println(kv + " ms=" + ms + " ok=" + (root.value == (long) k * g) + " byOwner=" + t2ByOwner.get() + " leaves=" + (k * g));
    Object q2 = field(OGSchedulerContext.class, "localQ").get(ec);
    System.out.println("KV finalTop=" + field(q2.getClass(), "_top").getInt(q2) + " finalBase=" + field(q2.getClass(), "_base").getInt(q2));
    System.out.flush();
    Runtime.getRuntime().halt(0);
  }

  // ---------------------------------------------------------------- T7

  static final class CompleteThenThrow extends N {
    CompleteThenThrow(String name, ScenarioStack ss) {
      super(name, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      value = 42;
      complete(ec);
      throw new RuntimeException("T7: thrown after complete(ec) in " + name);
    }
  }

  static Result t7() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demonstrated = false;
    Child pc = runChild("t7", "panic", "", 60);
    sb.append("[direct InfoDumper.graphPanic] exit=" + pc.exit + " returned=" + pc.kv("panicReturned") + " threw=" + pc.kv("panicThrew") + " rootCause=" + pc.kv("rootCause"));
    for (String l : pc.grep("panic root cause message")) sb.append("\n    | ").append(l);
    sb.append("\n    ");
    for (String variant : new String[] {"once", "twice"}) {
      Child c = runChild("t7", variant, "", 60);
      long twice = c.count("Cannot complete node twice"), rec = c.count("logAndDie called recursively");
      sb.append(String.format("[%s] exit=%d doubleCompleteDetected=%d logAndDieRecursive=%d survived1=%s survived2=%s inLogAndDieAfter1=%s state1=%s recursiveDuringSecond=%s; ",
          variant, c.exit, twice, rec, c.kv("survived1"), c.kv("survived2"), c.kv("inLogAndDie"), c.kv("state1"), recursiveBetween(c.lines, "T7-SECOND-BEGIN", "T7-SECOND-END")));
      for (String l : c.grep("ExceptionInInitializerError", "NoClassDefFoundError", "exiting (121)", "logAndDie called", "Cannot complete")) if (sb.length() < 6000) sb.append("\n    | ").append(l);
      sb.append("\n    ");
      if (c.exit == 121 || "true".equals(c.kv("survived1"))) demonstrated = true;
    }
    return new Result("T7", demonstrated ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  static long recursiveBetween(List<String> lines, String from, String to) {
    int a = lines.indexOf(from), b = lines.indexOf(to);
    if (a < 0 || b < 0) return -1;
    return lines.subList(a, b).stream().filter(l -> l.contains("logAndDie called recursively")).count();
  }

  static void t7Child(String variant) throws Exception {
    EvaluationContext.initializeWithoutRuntime();
    if (variant.equals("panic")) { // what does the kill path do here?
      try {
        optimus.graph.diagnostics.InfoDumper$.MODULE$.graphPanic("t7", "probe panic", 121, new RuntimeException("probe"), null);
        System.out.println("KV panicReturned=true");
      } catch (Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) root = root.getCause();
        System.out.println("KV panicThrew=" + t.getClass().getName() + " rootCause=" + root.getClass().getName());
        System.out.println("T7 panic root cause message: " + root.getMessage());
      }
      System.out.flush();
      Runtime.getRuntime().halt(0);
    }
    OGSchedulerContext ec = OGSchedulerContext.current();
    var n1 = new CompleteThenThrow("t7-first", EvaluationContext.scenarioStack());
    System.out.println("T7-FIRST-BEGIN");
    try {
      ec.runAndWait(n1);
    } catch (Throwable t) {
      System.out.println("T7 runAndWait(first) threw " + t);
    }
    System.out.println("T7-FIRST-END");
    boolean inLAD = field(NodeTask.class, "inLogAndDie").getBoolean(null);
    System.out.println("KV survived1=true state1=" + n1.stateAsString().replace(' ', '_') + " value1=" + n1.value + " inLogAndDie=" + inLAD);
    if (variant.equals("twice")) {
      System.out.println("T7-SECOND-BEGIN");
      var n2 = new CompleteThenThrow("t7-second", EvaluationContext.scenarioStack());
      try {
        ec.runAndWait(n2);
      } catch (Throwable t) {
        System.out.println("T7 runAndWait(second) threw " + t);
      }
      System.out.println("T7-SECOND-END");
      System.out.println("KV survived2=true state2=" + n2.stateAsString().replace(' ', '_'));
    }
    System.out.flush();
    Runtime.getRuntime().halt(0);
  }

  // ---------------------------------------------------------------- T8

  static final class SelfRemover implements OutOfWorkListener {
    final String name;
    final AtomicInteger checks = new AtomicInteger();

    SelfRemover(String name) {
      this.name = name;
    }

    @Override
    public int checkForNewWork(Scheduler scheduler) {
      checks.incrementAndGet();
      System.out.println("T8 " + name + ".checkForNewWork -> removeOutOfWorkListener(this)");
      ((OGScheduler) scheduler).removeOutOfWorkListener(this);
      return 0;
    }

    @Override
    public void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> outstandingTasks) {
      System.out.println("T8 " + name + ".onGraphStalled");
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static final class Completer implements OutOfWorkListener {
    final N target;

    Completer(N target) {
      this.target = target;
    }

    @Override
    public void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> outstandingTasks) {
      System.out.println("T8 completer.onGraphStalled: completing blocked node");
      target.value = 7;
      target.completeNow(scheduler);
    }

    @Override
    public String toString() {
      return "completer";
    }
  }

  static final class Blocking extends N {
    Blocking(ScenarioStack ss) {
      super("T8Blocking", ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {}
  }

  static Result t8() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demonstrated = false;
    for (String removers : new String[] {"1", "2", "3"}) {
      Child c = runChild("t8", removers, "-Xlog:exceptions=info", 60);
      long cme = c.count("ConcurrentModificationException");
      List<String> cmeLines = c.grep("ConcurrentModificationException");
      sb.append(String.format("[selfRemovers=%s +completer] exit=%d CME-lines=%d result=%s clientOutcome=%s wallMs=%d%s; ",
          removers, c.exit, cme, c.kv("value"), c.kv("client"), c.wallMs, c.timedOut || c.exit == 3 ? " HANG" : ""));
      if (!cmeLines.isEmpty()) sb.append("\n    | first CME: ").append(cmeLines.get(0));
      for (String l : c.grep("panic", "Panic", "graphPanic", "ExceptionInInitializerError", "NoClassDefFound", "Caught an error in runAndWait", "client threw", "T8 ", "CHILD-HANG", "exiting"))
        if (sb.length() < 12000) sb.append("\n    | ").append(l);
      sb.append("\n    ");
      if (c.exit == 66 || cme > 0) demonstrated = true;
    }
    return new Result("T8", demonstrated ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  static void t8Child(String variant) throws Exception {
    int removers = Integer.parseInt(variant);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    Blocking root = new Blocking(EvaluationContext.scenarioStack());
    for (int i = 0; i < removers; i++) s.addOutOfWorkListener(new SelfRemover("remover" + i));
    s.addOutOfWorkListener(new Completer(root));
    System.out.println("T8 registered " + removers + " self-removers + completer; limitThreads=" + Settings.limitThreads + " ideal=" + field(OGScheduler.class, "_idealCount").get(s));
    String[] outcome = {"none"};
    CountDownLatch done = new CountDownLatch(1);
    Thread client =
        new Thread(
            () -> {
              try {
                EvaluationContext.initializeWithoutRuntime();
                OGSchedulerContext.current().runAndWait(root);
                outcome[0] = "returned";
              } catch (Throwable t) {
                outcome[0] = "threw:" + t.getClass().getSimpleName();
                System.out.println("T8 client threw " + t);
                t.printStackTrace(System.out);
              } finally {
                done.countDown();
              }
            },
            "t8-client");
    client.setDaemon(true);
    client.start();
    if (!done.await(20, TimeUnit.SECONDS)) {
      System.out.println("KV client=hung value=" + root.value);
      dumpAndExit("t8 client not done", s, 3);
    }
    System.out.println("KV client=" + outcome[0] + " value=" + root.value + " done=" + root.isDone());
    // is the scheduler still usable afterwards?
    var probe = new Leaf(EvaluationContext.scenarioStack());
    Thread after =
        new Thread(
            () -> {
              try {
                EvaluationContext.initializeWithoutRuntime();
                OGSchedulerContext.current().runAndWait(probe);
                System.out.println("KV afterUsable=" + probe.isDone());
              } catch (Throwable t) {
                System.out.println("KV afterUsable=threw:" + t);
              }
            },
            "t8-after");
    after.setDaemon(true);
    after.start();
    after.join(10_000);
    System.out.flush();
    Runtime.getRuntime().halt(0);
  }

  // ---------------------------------------------------------------- main

  public static void main(String[] args) throws Exception {
    if (args.length >= 3 && args[0].equals("--child")) {
      switch (args[1]) {
        case "t1" -> t1Child(args[2]);
        case "t2" -> t2Child(args[2]);
        case "t7" -> t7Child(args[2]);
        case "t8" -> t8Child(args[2]);
        default -> throw new IllegalArgumentException(args[1]);
      }
      return;
    }
    Set<String> only = args.length == 0 ? null : Set.of(args);
    List<Result> results = new ArrayList<>();
    for (var e : List.<Map.Entry<String, java.util.concurrent.Callable<Result>>>of(
        Map.entry("T2", Tier1::t2), Map.entry("T1", Tier1::t1), Map.entry("T7", Tier1::t7), Map.entry("T8", Tier1::t8))) {
      if (only != null && !only.contains(e.getKey())) continue;
      Result r;
      try {
        r = e.getValue().call();
      } catch (Throwable t) {
        r = new Result(e.getKey(), Outcome.ERROR, t.toString());
      }
      results.add(r);
      System.out.println("RESULT " + r.id() + " " + r.outcome() + " " + r.detail());
      System.out.flush();
    }
    System.out.println("==== summary");
    for (Result r : results) System.out.println(r.id() + ": " + r.outcome());
    System.exit(0);
  }
}
