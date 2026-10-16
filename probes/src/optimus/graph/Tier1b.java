package optimus.graph;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import optimus.core.TPDMask;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import optimus.platform.util.PrettyStringBuilder;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Tier 1 probes T3, T4, T5, T6, T9, T10, T11, T12 (TEST_CAMPAIGN.md). Same conventions as Tier1:
 * the real OGScheduler end to end, one child JVM per configuration (`--child <probe> <variant>`,
 * launched via PROBES_JAVA_CMD with -D/-X flags inserted after "java"), one RESULT line per probe.
 * Self-contained; T3 additionally uses {@link Tier1bAgent} (instruments IndexedArrayList.foreach).
 */
public final class Tier1b {

  enum Outcome {
    DEMONSTRATED,
    NOT_REPRODUCED,
    QUANTIFIED,
    ERROR
  }

  record Result(String id, Outcome outcome, String detail) {}

  // ---------------------------------------------------------------- helpers

  static Field field(Class<?> c, String name) {
    try {
      Field f = c.getDeclaredField(name);
      f.setAccessible(true);
      return f;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Object get(Field f, Object o) {
    try {
      return f.get(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final Field EC_CONTEXT = field(OGSchedulerContext.class, "_context");
  static final Field EC_SYNCSTACK = field(OGSchedulerContext.class, "syncStack");
  static final Field EC_CAUSALITY = field(OGSchedulerContext.class, "currentCausalityID");
  static final Field NT_WAITERS = field(NodeTask.class, "_waiters");
  static final Field NT_VISITLOCK = field(NodeTask.class, "_visitLock");

  static OGScheduler.Context ctxOf(OGSchedulerContext ec) {
    return (OGScheduler.Context) get(EC_CONTEXT, ec);
  }

  static int threadIdOf(OGSchedulerContext ec) {
    return ctxOf(ec).threadID;
  }

  static Object[] framesOf(OGSchedulerContext ec) {
    Object stack = get(EC_SYNCSTACK, ec);
    return (Object[]) get(field(stack.getClass(), "frames"), stack);
  }

  static OGScheduler defaultScheduler() {
    return (OGScheduler) EvaluationContext.scheduler();
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static boolean await(java.util.function.BooleanSupplier c, long timeoutMs) {
    long end = System.nanoTime() + timeoutMs * 1_000_000;
    while (!c.getAsBoolean()) {
      if (System.nanoTime() > end) return false;
      LockSupport.parkNanos(200_000);
    }
    return true;
  }

  static Thread daemon(String name, Runnable r) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    t.start();
    return t;
  }

  static ScheduledThreadPoolExecutor timer(String name, int n) {
    AtomicInteger k = new AtomicInteger();
    var t = new ScheduledThreadPoolExecutor(n, r -> {
      Thread th = new Thread(r, name + "-" + k.incrementAndGet());
      th.setDaemon(true);
      return th;
    });
    t.setRemoveOnCancelPolicy(true);
    return t;
  }

  static void kv(Object... kvs) {
    StringBuilder sb = new StringBuilder("KV");
    for (int i = 0; i + 1 < kvs.length; i += 2) sb.append(' ').append(kvs[i]).append('=').append(String.valueOf(kvs[i + 1]).replace(' ', '_'));
    System.out.println(sb);
    System.out.flush();
  }

  /** jstack equivalent with lock owners, for the threads whose names match one of the prefixes. */
  static void dumpThreads(String... prefixes) {
    ThreadMXBean mx = ManagementFactory.getThreadMXBean();
    long[] dl = mx.findDeadlockedThreads();
    System.out.println("---- findDeadlockedThreads: " + (dl == null ? "none" : dl.length + " threads " + Arrays.toString(dl)));
    for (ThreadInfo ti : mx.dumpAllThreads(true, true)) {
      String n = ti.getThreadName();
      boolean want = dl != null && Arrays.stream(dl).anyMatch(x -> x == ti.getThreadId());
      for (String p : prefixes) want |= n.startsWith(p);
      if (!want) continue;
      System.out.println("--- \"" + n + "\" id=" + ti.getThreadId() + " " + ti.getThreadState() + (ti.getLockName() != null ? " waiting on " + ti.getLockName() + " owned by \"" + ti.getLockOwnerName() + "\"" : ""));
      StackTraceElement[] st = ti.getStackTrace();
      for (int i = 0; i < Math.min(40, st.length); i++) {
        System.out.println("    at " + st[i]);
        for (var m : ti.getLockedMonitors()) if (m.getLockedStackDepth() == i) System.out.println("      - locked " + m);
      }
      for (var s : ti.getLockedSynchronizers()) System.out.println("    holds synchronizer " + s);
    }
    System.out.flush();
  }

  static void dumpAndHalt(String why, OGScheduler s, int code, String... prefixes) {
    System.out.println("CHILD-HANG " + why);
    try {
      System.out.println("---- OGScheduler.toString()");
      System.out.println(s.toString());
    } catch (Throwable t) {
      System.out.println("scheduler toString threw " + t);
    }
    dumpThreads(prefixes);
    Runtime.getRuntime().halt(code);
  }

  static void watchdog(long ms, Runnable onFire) {
    daemon("tier1b-watchdog", () -> {
      sleep(ms);
      onFire.run();
    });
  }

  record Child(int exit, List<String> lines, long wallMs, boolean timedOut) {
    String kv(String key) {
      String r = null;
      for (String l : lines) {
        if (!l.startsWith("KV ")) continue;
        for (String tok : l.substring(3).split(" ")) if (tok.startsWith(key + "=")) r = tok.substring(key.length() + 1);
      }
      return r;
    }

    long count(String needle) {
      return lines.stream().filter(l -> l.contains(needle)).count();
    }

    List<String> grep(int max, String... needles) {
      List<String> r = new ArrayList<>();
      for (String l : lines)
        for (String n : needles)
          if (l.contains(n)) {
            if (r.size() < max) r.add(l.length() > 260 ? l.substring(0, 260) + "..." : l);
            break;
          }
      return r;
    }
  }

  static final List<String> commands = new ArrayList<>();

  static Child runChild(String probe, String variant, String flags, int timeoutSec) throws Exception {
    String cmd = System.getenv("PROBES_JAVA_CMD");
    if (cmd == null) throw new IllegalStateException("PROBES_JAVA_CMD not set; run via run-tier1b.sh");
    String full = "java " + flags + " " + cmd.substring(5) + " --child " + probe + " " + variant;
    commands.add("[" + probe + " " + variant + "] java " + flags + " <PROBES_JAVA_CMD...> --child " + probe + " " + variant);
    long t0 = System.nanoTime();
    Process p = new ProcessBuilder("sh", "-c", full).redirectErrorStream(true).start();
    List<String> lines = java.util.Collections.synchronizedList(new ArrayList<>());
    Thread reader = new Thread(() -> {
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
    String log = System.getenv("TIER1B_LOGDIR");
    if (log != null) {
      java.nio.file.Files.createDirectories(java.nio.file.Path.of(log));
      var path = java.nio.file.Path.of(log, probe + "-" + variant.replace(':', '_') + "-" + flags.replaceAll("[^A-Za-z0-9=.]+", "_") + ".log");
      java.nio.file.Files.write(path, new ArrayList<>(lines));
    }
    return new Child(done ? p.exitValue() : -1, new ArrayList<>(lines), wall, !done);
  }

  // ---------------------------------------------------------------- nodes

  abstract static class N extends NodeTask {
    volatile long value;
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

  /** Runs once and does not complete: completed later by the probe (promise style). */
  static final class Hold extends N {
    final AtomicInteger runs = new AtomicInteger();
    volatile Thread ranOn;

    Hold(String name, ScenarioStack ss) {
      super(name, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      ranOn = Thread.currentThread();
      runs.incrementAndGet();
    }
  }

  /** Completed by a timer thread (eq = scheduler, as NodePromise does) delayUs after it runs. */
  static final class Timed extends N {
    final OGScheduler s;
    final ScheduledThreadPoolExecutor timer;
    final long delayUs;

    Timed(String name, ScenarioStack ss, OGScheduler s, ScheduledThreadPoolExecutor timer, long delayUs) {
      super(name, ss);
      this.s = s;
      this.timer = timer;
      this.delayUs = delayUs;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      timer.schedule(() -> complete(s), delayUs, TimeUnit.MICROSECONDS);
    }
  }

  // ================================================================ T3 (R3)

  /** Called from code that Tier1bAgent inserts into optimus.core.IndexedArrayList.foreach. */
  public static final class Hooks {
    public static volatile Object tracked;
    static final long maxNanos = Long.getLong("probes.delay.maxNanos", 0);
    static boolean inTracked; // written and read by the iterating thread only (findWork, under _lock)
    static long pass, tEnter;
    static final AtomicLong clock = new AtomicLong(), passes = new AtomicLong(), points = new AtomicLong(), checked = new AtomicLong(),
        skipped = new AtomicLong(), skippedPresentAtExit = new AtomicLong(), doubled = new AtomicLong(), concurrentIter = new AtomicLong();
    static final Set<T3Sentinel> live = ConcurrentHashMap.newKeySet();
    static final AtomicInteger depth = new AtomicInteger();
    static volatile String firstSkip;
    static long maxLen;

    public static void feEnter(Object l) {
      if (l != tracked) return;
      if (depth.incrementAndGet() > 1) concurrentIter.incrementAndGet();
      inTracked = true;
      pass++;
      passes.incrementAndGet();
      tEnter = clock.incrementAndGet();
    }

    public static void fePoint(Object l) {
      if (l != tracked) return;
      points.incrementAndGet();
      if (maxNanos > 0) {
        long n = ThreadLocalRandom.current().nextLong(maxNanos);
        if (n < 1000) Thread.yield();
        else LockSupport.parkNanos(n);
      }
    }

    public static void feExit(Object l) {
      if (l != tracked) return;
      depth.decrementAndGet();
      if (!inTracked) return;
      inTracked = false;
      int len = ((optimus.core.IndexedArrayList<?>) l).count;
      if (len > maxLen) maxLen = len;
      for (T3Sentinel s : live) {
        long a = s.tAdd, r = s.tRem;
        if (a == 0 || a > tEnter || r != 0 || !s.ran) continue; // present (and runnable-checked) for the whole pass only
        checked.incrementAndGet();
        int v = s.visitPass == pass ? s.visits : 0;
        if (v == 0) {
          boolean present = false;
          for (Object o : ((optimus.core.IndexedArrayList<?>) l).getUnderlyingList()) if (o instanceof OGScheduler.TrackedTask tt && tt.task == s) present = true;
          if (present) skippedPresentAtExit.incrementAndGet();
          if (skipped.incrementAndGet() == 1)
            firstSkip = "pass " + pass + ": " + s + " added@" + a + " pass entered@" + tEnter + " listCount=" + len;
        } else if (v > 1) doubled.incrementAndGet();
      }
    }
  }

  static final AtomicLong t3Outstanding = new AtomicLong(), t3Adds = new AtomicLong(), t3SentinelAdds = new AtomicLong(), t3ClientWaits = new AtomicLong(),
      t3ExtCompletions = new AtomicLong();

  static final class T3Leaf extends N {
    final OGScheduler s;
    final ExecutorService completers;
    final boolean inline;

    T3Leaf(ScenarioStack ss, OGScheduler s, ExecutorService completers, boolean inline) {
      super("T3Leaf", ss);
      this.s = s;
      this.completers = completers;
      this.inline = inline;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      if (inline) {
        complete(ec);
        t3Outstanding.decrementAndGet();
      } else
        completers.execute(() -> {
          complete(s); // tracked-list removal happens here, on a non-graph thread
          t3ExtCompletions.incrementAndGet();
          t3Outstanding.decrementAndGet();
        });
    }
  }

  static final class T3Sentinel extends N {
    final OGScheduler s;
    final ScheduledThreadPoolExecutor timer;
    volatile long tAdd, tRem;
    volatile boolean ran;
    long visitPass;
    int visits;

    T3Sentinel(ScenarioStack ss, OGScheduler s, ScheduledThreadPoolExecutor timer) {
      super("T3Sentinel", ss);
      this.s = s;
      this.timer = timer;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      ran = true;
      timer.schedule(() -> {
        tRem = Hooks.clock.incrementAndGet();
        Hooks.live.remove(this);
        complete(s);
        t3Outstanding.decrementAndGet();
      }, 1 + ThreadLocalRandom.current().nextInt(20), TimeUnit.MILLISECONDS);
    }

    @Override
    void detectCyclesAndRecover(EvaluationQueue eq) {
      if (Hooks.inTracked) {
        if (visitPass == Hooks.pass) visits++;
        else {
          visitPass = Hooks.pass;
          visits = 1;
        }
      }
      super.detectCyclesAndRecover(eq);
    }
  }

  static final class T3Root extends N {
    final long endNs;
    final OGScheduler s;
    final ScheduledThreadPoolExecutor timer;

    T3Root(ScenarioStack ss, long endNs, OGScheduler s, ScheduledThreadPoolExecutor timer) {
      super("T3Root", ss);
      this.endNs = endNs;
      this.s = s;
      this.timer = timer;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      while (System.nanoTime() < endNs) {
        Timed p = new Timed("T3Promise", scenarioStack(), s, timer, 2000);
        ec.runAndWait(p);
        t3ClientWaits.incrementAndGet();
      }
      complete(ec);
    }
  }

  static void t3Child(String variant) throws Exception {
    long ms = Long.getLong("t3.ms", 8000);
    int hammers = 8, maxOutstanding = 512;
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    Object tracked = get(field(OGScheduler.class, "_evalAsyncTrackedNodes"), s);
    Hooks.tracked = tracked;
    kv("ideal", get(field(OGScheduler.class, "_idealCount"), s), "limitThreads", Settings.limitThreads, "asserts", Settings.schedulerAsserts, "delayMaxNanos", Hooks.maxNanos,
        "agent", System.getProperty("tier1b.agent.instrumented"));
    var timer = timer("t3-timer", 2);
    var completers = Executors.newFixedThreadPool(4, r -> {
      Thread t = new Thread(r, "t3-completer");
      t.setDaemon(true);
      return t;
    });
    long end = System.nanoTime() + ms * 1_000_000;
    AtomicLong errors = new AtomicLong();
    CountDownLatch done = new CountDownLatch(1 + hammers);
    daemon("t3-client", () -> {
      try {
        EvaluationContext.initializeWithoutRuntime();
        OGSchedulerContext.current().runAndWait(new T3Root(EvaluationContext.scenarioStack(), end, s, timer));
      } catch (Throwable t) {
        errors.incrementAndGet();
        System.out.println("T3 client threw " + t);
        t.printStackTrace(System.out);
      } finally {
        done.countDown();
      }
    });
    for (int h = 0; h < hammers; h++) {
      final int hh = h;
      daemon("t3-hammer-" + h, () -> {
        try {
          ThreadLocalRandom r = ThreadLocalRandom.current();
          long k = 0;
          while (System.nanoTime() < end) {
            if (t3Outstanding.get() > maxOutstanding) {
              Thread.onSpinWait();
              continue;
            }
            t3Outstanding.incrementAndGet();
            if (++k % 16 == hh % 16) {
              T3Sentinel sn = new T3Sentinel(ss, s, timer);
              Hooks.live.add(sn);
              s.enqueueAndTrackCompletion(sn);
              sn.tAdd = Hooks.clock.incrementAndGet();
              t3SentinelAdds.incrementAndGet();
            } else {
              s.enqueueAndTrackCompletion(new T3Leaf(ss, s, completers, r.nextBoolean()));
            }
            t3Adds.incrementAndGet();
          }
        } catch (Throwable t) {
          errors.incrementAndGet();
          System.out.println("T3 hammer threw " + t);
          t.printStackTrace(System.out);
        } finally {
          done.countDown();
        }
      });
    }
    watchdog(ms + 30_000, () -> {
      kv("hang", true, "passes", Hooks.passes.get(), "adds", t3Adds.get(), "clientWaits", t3ClientWaits.get(), "outstanding", t3Outstanding.get());
      dumpAndHalt("t3 not done", s, 3, "t3-");
    });
    done.await();
    boolean drained = await(() -> t3Outstanding.get() == 0, 10_000);
    int trackedLeft = ((optimus.core.IndexedArrayList<?>) tracked).count;
    kv("passes", Hooks.passes.get(), "points", Hooks.points.get(), "adds", t3Adds.get(), "sentinelAdds", t3SentinelAdds.get(), "extCompletions", t3ExtCompletions.get(),
        "clientWaits", t3ClientWaits.get(), "checked", Hooks.checked.get(), "skipped", Hooks.skipped.get(), "skippedStillTrackedAtExit", Hooks.skippedPresentAtExit.get(), "doubled", Hooks.doubled.get(),
        "concurrentIter", Hooks.concurrentIter.get(), "maxLen", Hooks.maxLen, "errors", errors.get(), "drained", drained, "trackedLeft", trackedLeft);
    if (Hooks.firstSkip != null) System.out.println("T3 first skip: " + Hooks.firstSkip);
    Runtime.getRuntime().halt(0);
  }

  static Result t3() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false;
    long totPasses = 0, totAdds = 0, totSkipped = 0, totChecked = 0;
    String[][] cfgs = {
      {"vol", "-Doptimus.gthread.ideal=1"},
      {"delay", "-Doptimus.gthread.ideal=1 -Dprobes.delay.maxNanos=50000"},
      {"delay-asserts", "-Doptimus.gthread.ideal=1 -Dprobes.delay.maxNanos=50000 -Doptimus.scheduler.asserts=true"},
      {"vol-ideal2", "-Doptimus.gthread.ideal=2"},
    };
    for (String[] c : cfgs) {
      Child ch = runChild("t3", c[0], c[1], 120);
      long exc = ch.count("Exception") + ch.count("panic") + ch.count("Error");
      sb.append(String.format("%n  [%s %s] exit=%d%s passes=%s points=%s adds=%s (sentinels %s, ext-completed leaves %s) clientWaits=%s sentinelChecks=%s skipped=%s (still tracked after the pass: %s) doubled=%s concurrentIter=%s maxLen=%s errors=%s exceptionLines=%d agent=%s",
          c[0], c[1], ch.exit, ch.timedOut || ch.exit == 3 ? " HANG" : "", ch.kv("passes"), ch.kv("points"), ch.kv("adds"), ch.kv("sentinelAdds"), ch.kv("extCompletions"), ch.kv("clientWaits"),
          ch.kv("checked"), ch.kv("skipped"), ch.kv("skippedStillTrackedAtExit"), ch.kv("doubled"), ch.kv("concurrentIter"), ch.kv("maxLen"), ch.kv("errors"), exc, ch.kv("agent") == null ? "?" : "ok"));
      for (String l : ch.grep(4, "T3 first skip", "Exception", "panic", "CHILD-HANG")) sb.append("\n    | ").append(l);
      if (exc > 0 || ch.exit != 0) demo = true;
      try {
        totPasses += Long.parseLong(ch.kv("passes"));
        totAdds += Long.parseLong(ch.kv("adds"));
        totSkipped += Long.parseLong(ch.kv("skipped"));
        totChecked += Long.parseLong(ch.kv("checked"));
      } catch (Exception ignored) {
      }
    }
    sb.insert(0, String.format("no exception in %d stalled passes over _evalAsyncTrackedNodes x %d enqueueAndTrackCompletion adds; %d of %d (sentinel, pass) pairs where the sentinel was tracked for the whole pass were skipped by the iteration;",
        totPasses, totAdds, totSkipped, totChecked));
    Outcome o = demo ? Outcome.DEMONSTRATED : totSkipped > 0 ? Outcome.QUANTIFIED : Outcome.NOT_REPRODUCED;
    return new Result("T3", o, sb.toString());
  }

  // ================================================================ T4 (R4)

  static final AtomicInteger t4Recoveries = new AtomicInteger(), t4CrossEnqueues = new AtomicInteger(), t4CrossDone = new AtomicInteger();
  static final ConcurrentHashMap<String, AtomicInteger> t4RecoveriesBy = new ConcurrentHashMap<>();

  static final class CycNode extends N {
    CycNode partner;
    OGScheduler other;
    String otherName;
    boolean first, started;
    final AtomicInteger recoveries = new AtomicInteger();
    final int maxRecoveries;
    final long delayMs;

    CycNode(String name, ScenarioStack ss, int maxRecoveries, long delayMs) {
      super(name, ss);
      this.maxRecoveries = maxRecoveries;
      this.delayMs = delayMs;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      if (partner.isDoneEx()) {
        complete(ec);
        return;
      }
      if (!started) {
        started = true;
        if (first) ec.enqueue(this, partner);
        partner.continueWith(this, ec); // P waits on Q (first), Q waits on P: a waitingOn cycle
      }
    }

    /** Mimics NCProxies' recovery: enqueue a retry onto (here: the OTHER) scheduler, report success. */
    @Override
    public boolean tryToRecoverFromCycle(TPDMask mask, EvaluationQueue eq) {
      int k = t4Recoveries.incrementAndGet();
      t4RecoveriesBy.computeIfAbsent(Thread.currentThread().getName(), x -> new AtomicInteger()).incrementAndGet();
      ReentrantLock vl = (ReentrantLock) get(NT_VISITLOCK, null);
      boolean holds = vl.isHeldByCurrentThread();
      if (recoveries.incrementAndGet() > maxRecoveries) return false; // let the cycle be broken with CircularReferenceException
      if (delayMs > 0) sleep(delayMs);
      t4CrossEnqueues.incrementAndGet();
      other.enqueue(new Leaf(scenarioStack())); // takes other._lock while holding this scheduler's _lock and _visitLock
      if (k <= 4 || k % 1000 == 0) // logged after the enqueue so that I/O does not widen the window
        System.out.println("T4-RECOVER k=" + k + " node=" + name + " thread=" + Thread.currentThread().getName() + " heldVisitLock=" + holds
            + " eq=" + eq.getClass().getSimpleName() + " -> enqueued onto the other scheduler (" + otherName + ")");
      t4CrossDone.incrementAndGet();
      return true;
    }
  }

  static void t4Child(String variant) throws Exception {
    // variant = delayMs:maxRecoveriesPerNode:rounds
    String[] v = variant.split(":");
    long delayMs = Long.parseLong(v[0]);
    int maxRec = Integer.parseInt(v[1]), rounds = Integer.parseInt(v[2]);
    boolean solo = v.length > 3 && v[3].equals("solo"); // control: only scheduler A has a cycle
    EvaluationContext.initializeWithoutRuntime();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    OGScheduler a = new OGScheduler("t4A-", 2, Thread.NORM_PRIORITY), b = new OGScheduler("t4B-", 2, Thread.NORM_PRIORITY);
    kv("keepTaskSchedulerAffinity", Settings.keepTaskSchedulerAffinity, "limitThreads", Settings.limitThreads, "delayMs", delayMs, "maxRec", maxRec, "rounds", rounds);
    CyclicBarrier barrier = new CyclicBarrier(solo ? 1 : 2);
    AtomicInteger finished = new AtomicInteger(), circular = new AtomicInteger();
    AtomicLong errors = new AtomicLong();
    CountDownLatch done = new CountDownLatch(solo ? 1 : 2);
    for (OGScheduler[] pair : solo ? new OGScheduler[][] {{a, b}} : new OGScheduler[][] {{a, b}, {b, a}}) {
      OGScheduler mine = pair[0], other = pair[1];
      String side = mine == a ? "A" : "B";
      daemon("t4-client-" + side, () -> {
        try {
          OGSchedulerContext ec = mine.newEvaluationContext(ss);
          OGSchedulerContext.setCurrent(ec);
          for (int r = 0; r < rounds; r++) {
            CycNode p = new CycNode("P" + side + r, ss, maxRec, delayMs), q = new CycNode("Q" + side + r, ss, maxRec, delayMs);
            p.partner = q;
            q.partner = p;
            p.first = true;
            p.other = q.other = other;
            p.otherName = q.otherName = side.equals("A") ? "t4B" : "t4A";
            barrier.await(30, TimeUnit.SECONDS);
            ec.runAndWait(p);
            if (p.isDoneWithException() || q.isDoneWithException()) circular.incrementAndGet();
            finished.incrementAndGet();
          }
        } catch (Throwable t) {
          errors.incrementAndGet();
          System.out.println("T4 client " + side + " threw " + t);
          t.printStackTrace(System.out);
        } finally {
          done.countDown();
        }
      });
    }
    if (!done.await(Long.getLong("t4.timeoutSec", 20), TimeUnit.SECONDS)) {
      kv("hang", true, "finished", finished.get(), "recoveries", t4Recoveries.get(), "crossEnqueues", t4CrossEnqueues.get(), "crossDone", t4CrossDone.get(), "by", t4RecoveriesBy);
      ThreadMXBean mx = ManagementFactory.getThreadMXBean();
      long[] dl = mx.findDeadlockedThreads();
      kv("deadlocked", dl == null ? 0 : dl.length);
      System.out.println("CHILD-HANG t4 clients not done");
      dumpThreads("t4-client", "og.worker-");
      Runtime.getRuntime().halt(3);
    }
    kv("hang", false, "finished", finished.get(), "circular", circular.get(), "recoveries", t4Recoveries.get(), "crossEnqueues", t4CrossEnqueues.get(), "crossDone", t4CrossDone.get(),
        "errors", errors.get(), "by", t4RecoveriesBy);
    Runtime.getRuntime().halt(0);
  }

  static Result t4() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false, reached = false;
    String[][] cfgs = {{"0:20:50:solo", ""}, {"50:3:3", ""}, {"0:20:300", ""}, {"0:20:300", "-Doptimus.scheduler.asserts=true"}};
    for (String[] c : cfgs) {
      String flags = "-Doptimus.graph.keepTaskSchedulerAffinity=true " + c[1];
      Child ch = runChild("t4", c[0], flags, 90);
      boolean hang = ch.timedOut || ch.exit == 3;
      String dl = ch.kv("deadlocked");
      sb.append(String.format("%n  [delayMs:maxRecoveriesPerNode:rounds=%s %s] exit=%d %s finished=%s circular=%s recoveries=%s crossEnqueues=%s crossEnqueuesReturned=%s deadlockedThreads=%s by=%s",
          c[0], flags, ch.exit, hang ? "HANG" : "completed", ch.kv("finished"), ch.kv("circular"), ch.kv("recoveries"), ch.kv("crossEnqueues"), ch.kv("crossDone"), dl, ch.kv("by")));
      for (String l : ch.grep(2, "T4-RECOVER")) sb.append("\n    | ").append(l);
      if (hang)
        for (String l : ch.grep(40, "findDeadlockedThreads", "--- \"t4-client", "OGScheduler.enqueue(", "detectCyclesAndRecover", "findWorkOnForwardChain", "tryToRecoverFromCycle", "- locked", "holds synchronizer"))
          sb.append("\n    | ").append(l.trim());
      if (ch.count("T4-RECOVER") > 0) reached = true;
      if (hang && dl != null && !dl.equals("0")) demo = true;
    }
    if (!reached) sb.append("\n  detectCyclesAndRecover never called tryToRecoverFromCycle");
    return new Result("T4", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  // ================================================================ T5 (R5)

  static volatile long t5NodeStart, t5NodeEnd, t5ChainBuilt;

  static final class ChainNode extends N {
    final int i, n;
    final long holdMs;
    final OGScheduler s;
    final ScheduledThreadPoolExecutor timer;
    ChainNode next;
    boolean scheduled;

    ChainNode(int i, int n, ScenarioStack ss, OGScheduler s, ScheduledThreadPoolExecutor timer, long holdMs) {
      super("Chain" + i, ss);
      this.i = i;
      this.n = n;
      this.s = s;
      this.timer = timer;
      this.holdMs = holdMs;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      if (i == n) {
        if (!scheduled) {
          scheduled = true;
          t5ChainBuilt = System.nanoTime();
          timer.schedule(() -> complete(s), holdMs, TimeUnit.MILLISECONDS);
        }
        return;
      }
      if (next == null) {
        next = new ChainNode(i + 1, n, scenarioStack(), s, timer, holdMs);
        ec.enqueue(this, next);
        next.continueWith(this, ec);
        return;
      }
      complete(ec);
    }
  }

  static final class CancelMe extends N {
    final int chain;
    final long busyMs;
    final ScenarioStack chainSS;
    final OGScheduler s;
    final ScheduledThreadPoolExecutor timer;
    volatile ChainNode head;

    CancelMe(ScenarioStack cancellable, ScenarioStack chainSS, int chain, long busyMs, OGScheduler s, ScheduledThreadPoolExecutor timer) {
      super("CancelMe", cancellable);
      this.chain = chain;
      this.busyMs = busyMs;
      this.chainSS = chainSS;
      this.s = s;
      this.timer = timer;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      t5NodeStart = System.nanoTime();
      if (chain == 0) {
        long end = t5NodeStart + busyMs * 1_000_000;
        double x = 1;
        while (System.nanoTime() < end) x = Math.sin(x) + 1.0001;
        value = (long) x;
      } else {
        head = new ChainNode(1, chain, chainSS, s, timer, busyMs);
        ec.runAndWait(head); // sync frame whose ntsk is this cancelled node; forward chain is `chain` long
      }
      t5NodeEnd = System.nanoTime();
      complete(ec);
    }
  }

  static void t5Child(String variant) throws Exception {
    int chain = variant.startsWith("chain") ? Integer.parseInt(variant.split(":")[1]) : 0;
    long busyMs = 2000;
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    ScenarioStack css = ss.withChildCancellationScope();
    CancellationScope cs = css.cancelScope();
    var timer = timer("t5-timer", 1);
    CancelMe node = new CancelMe(css, ss, chain, busyMs, s, timer);
    kv("variant", variant, "jit", System.getProperty("java.vm.info"), "inputArgsXcomp", ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-Xcomp"));
    CountDownLatch clientDone = new CountDownLatch(1);
    daemon("t5-client", () -> {
      try {
        EvaluationContext.initializeWithoutRuntime();
        OGSchedulerContext.current().runAndWait(node);
      } catch (Throwable t) {
        System.out.println("T5 client threw " + t);
      } finally {
        clientDone.countDown();
      }
    });
    if (chain == 0) await(() -> t5NodeStart != 0, 60_000);
    else await(() -> t5ChainBuilt != 0, 300_000);
    long buildMs = chain == 0 ? 0 : (t5ChainBuilt - t5NodeStart) / 1_000_000;
    // per-iteration cost of the poll loop's deadlock check (chain variant)
    double chainWalkUs = -1;
    if (chain > 0) {
      int reps = 20;
      long t0 = System.nanoTime();
      for (int i = 0; i < reps; i++) node.visitForwardChain().containsEQ(null);
      chainWalkUs = (System.nanoTime() - t0) / 1000.0 / reps;
    }
    cs.cancel("t5 cancel");
    long[] res = new long[4];
    Thread waiter = daemon("t5-waiter", () -> {
      ThreadMXBean mx = ManagementFactory.getThreadMXBean();
      long cpu0 = mx.getCurrentThreadCpuTime(), u0 = mx.getCurrentThreadUserTime(), w0 = System.nanoTime();
      s.waitForCancelledNodesToStop(cs);
      res[0] = System.nanoTime() - w0;
      res[1] = mx.getCurrentThreadCpuTime() - cpu0;
      res[2] = mx.getCurrentThreadUserTime() - u0;
      res[3] = w0;
    });
    waiter.join(120_000);
    if (waiter.isAlive()) {
      kv("hang", true);
      dumpAndHalt("t5 waitForCancelledNodesToStop did not return in 120 s", s, 3, "t5-");
    }
    clientDone.await(30, TimeUnit.SECONDS);
    long remainingMs = (t5NodeEnd - res[3]) / 1_000_000;
    kv("hang", false, "waitMs", res[0] / 1_000_000, "waiterCpuMs", res[1] / 1_000_000, "waiterUserMs", res[2] / 1_000_000, "nodeRemainingAtWaitMs", remainingMs,
        "nodeRunMs", (t5NodeEnd - t5NodeStart) / 1_000_000, "chain", chain, "chainBuildMs", buildMs, "chainWalkUs", String.format("%.1f", chainWalkUs),
        "nodeDone", node.isDone(), "waitReturnedAfterNodeEnd", res[3] + res[0] >= t5NodeEnd);
    Runtime.getRuntime().halt(0);
  }

  static Result t5() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false;
    String[][] cfgs = {
      {"busy", ""}, {"chain:100000", ""}, {"busy", "-XX:-UseOnStackReplacement -Xcomp"}, {"chain:100000", "-XX:-UseOnStackReplacement -Xcomp"},
    };
    for (String[] c : cfgs) {
      Child ch = runChild("t5", c[0], c[1], 1200);
      boolean hang = ch.timedOut || ch.exit == 3;
      sb.append(String.format("%n  [%s %s] exit=%d%s wait=%sms waiterCPU=%sms (user %sms) nodeRemainingAtCall=%sms nodeRun=%sms chainBuild=%sms chainWalkPerIter=%sus childWall=%ds",
          c[0], c[1].isEmpty() ? "default JIT" : c[1], ch.exit, hang ? " HANG" : "", ch.kv("waitMs"), ch.kv("waiterCpuMs"), ch.kv("waiterUserMs"), ch.kv("nodeRemainingAtWaitMs"), ch.kv("nodeRunMs"),
          ch.kv("chainBuildMs"), ch.kv("chainWalkUs"), ch.wallMs / 1000));
      for (String l : ch.grep(3, "CHILD-HANG", "threw", "GraphInInvalidState")) sb.append("\n    | ").append(l);
      if (hang) demo = true;
      try {
        long w = Long.parseLong(ch.kv("waitMs")), rem = Long.parseLong(ch.kv("nodeRemainingAtWaitMs"));
        if (w > 3 * Math.max(rem, 1) + 1000) demo = true;
      } catch (Exception ignored) {
      }
    }
    return new Result("T5", demo ? Outcome.DEMONSTRATED : Outcome.QUANTIFIED, sb.toString());
  }

  // ================================================================ T6 (R6)

  static volatile OGSchedulerContext t6Ec;
  static volatile long t6ExitA, t6StartB, t6ExitB, t6BCompleted;
  static volatile String t6AStateSeenByRA;

  static final class Waits extends N {
    final NodeTask target;

    Waits(String name, ScenarioStack ss, NodeTask target) {
      super(name, ss);
      this.target = target;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      ec.runAndWait(target); // frame 1 of T's sync stack, ntsk = this
      if (name.equals("RA")) t6AStateSeenByRA = target.stateAsString() + " isDone=" + target.isDone() + " isDoneEx=" + target.isDoneEx();
      complete(ec);
    }
  }

  static String waitersDesc(NodeTask n, Object frame) {
    Object w = get(NT_WAITERS, n);
    if (w == null) return "null";
    if (w == frame) return "theFrame";
    return w.getClass().getSimpleName();
  }

  static void t6Child(String variant) throws Exception {
    boolean nudge = variant.equals("nudge");
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    Hold a = new Hold("A", ss), b = new Hold("B", ss);
    Waits ra = new Waits("RA", ss, a), rb = new Waits("RB", ss, b);
    AtomicLong tErrors = new AtomicLong();
    Thread t = daemon("t6-T", () -> {
      try {
        EvaluationContext.initializeWithoutRuntime();
        t6Ec = OGSchedulerContext.current();
        try {
          t6Ec.runAndWait(ra);
        } catch (Throwable e) {
          tErrors.incrementAndGet();
          System.out.println("T6 T: wait on RA/A threw " + e);
          e.printStackTrace(System.out);
        }
        t6ExitA = System.nanoTime();
        t6StartB = System.nanoTime();
        try {
          t6Ec.runAndWait(rb);
        } catch (Throwable e) {
          tErrors.incrementAndGet();
          System.out.println("T6 T: wait on RB/B threw " + e);
          e.printStackTrace(System.out);
        }
        t6ExitB = System.nanoTime();
      } catch (Throwable e) {
        tErrors.incrementAndGet();
        System.out.println("T6 T threw " + e);
        e.printStackTrace(System.out);
      }
    });
    watchdog(30_000, () -> dumpAndHalt("t6 not done", s, 3, "t6-"));
    if (!await(() -> a.runs.get() == 1 && t6Ec != null && ctxOf(t6Ec).parked, 5000)) System.out.println("T6 T did not park on A");
    Object[] frames = framesOf(t6Ec);
    Object frameA = frames[1];
    String before = "frame1.ntsk=" + ((OGSchedulerContext.SyncFrame) frameA).ntsk + " frame1.waitingOn=" + ((NodeAwaiter) frameA).getWaitingOn() + " A._waiters=" + waitersDesc(a, frameA);
    a.invalidateCache(null);
    String afterInv = "A=" + a.stateAsString() + " isDoneEx=" + a.isDoneEx() + " A._waiters=" + waitersDesc(a, frameA) + " T.parked=" + ctxOf(t6Ec).parked;
    long tInv = System.nanoTime();
    if (nudge) s.enqueue(new Leaf(ss)); // any global enqueue: the waiter scan hands it to T, which then re-checks A.isDoneEx()
    boolean exitedOnInvalidate = await(() -> t6ExitA != 0, 3000);
    String aCompletedWhen = "notYet";
    if (!exitedOnInvalidate) {
      // T is still parked on A although A is isDoneEx: nothing notifies on invalidateCache
      aCompletedWhen = "beforeB(to release T)";
      a.completeNow(s);
      await(() -> t6ExitA != 0, 5000);
    }
    long exitMs = t6ExitA == 0 ? -1 : (t6ExitA - tInv) / 1_000_000;
    await(() -> b.runs.get() == 1 && ctxOf(t6Ec).parked, 5000);
    Object frameB = framesOf(t6Ec)[1];
    OGSchedulerContext.SyncFrame fb = (OGSchedulerContext.SyncFrame) frameB;
    String reuse = "sameFrameObject=" + (frameB == frameA) + " frame1.ntsk=" + fb.ntsk + " frame1.waitingOn=" + fb.getWaitingOn() + " A._waiters=" + waitersDesc(a, frameA)
        + " B._waiters=" + waitersDesc(b, frameB) + " T.awaitedTask=" + ctxOf(t6Ec).awaitedTask;
    String afterAComplete = "n/a";
    boolean disturbed = false, clobbered = false;
    if (aCompletedWhen.equals("notYet")) {
      aCompletedWhen = "duringB";
      Throwable ce = null;
      try {
        a.completeNow(s); // A's notifyWaiters runs on the stale registration: frame1, which now describes T's wait on B
      } catch (Throwable e) {
        ce = e;
      }
      sleep(300);
      disturbed = t6ExitB != 0 || !ctxOf(t6Ec).parked || ctxOf(t6Ec).awaitedTask != b;
      clobbered = fb.getWaitingOn() != b;
      afterAComplete = "completeThrew=" + ce + " A=" + a.stateAsString() + " frame1.ntsk=" + fb.ntsk + " frame1.waitingOn=" + fb.getWaitingOn() + " T.parked=" + ctxOf(t6Ec).parked
          + " T.awaitedTask=" + ctxOf(t6Ec).awaitedTask + " T.returnedFromB=" + (t6ExitB != 0) + " B.done=" + b.isDone();
    }
    t6BCompleted = System.nanoTime();
    b.completeNow(s);
    boolean bDone = await(() -> t6ExitB != 0, 5000);
    long bLagMs = bDone ? (t6ExitB - t6BCompleted) / 1_000_000 : -1;
    System.out.println("T6 before invalidate: " + before);
    System.out.println("T6 after invalidate: " + afterInv);
    System.out.println("T6 RA saw: " + t6AStateSeenByRA);
    System.out.println("T6 second wait: " + reuse);
    System.out.println("T6 after A completes: " + afterAComplete);
    kv("exitedOnInvalidate", exitedOnInvalidate, "exitAfterInvalidateMs", exitMs, "aCompleted", aCompletedWhen, "sameFrame", frameB == frameA, "disturbed", disturbed, "frameClobbered", clobbered,
        "bReturnedAfterBCompleteMs", bLagMs, "tErrors", tErrors.get(), "frameWaitingOnAfterBDone", fb.getWaitingOn());
    Runtime.getRuntime().halt(0);
  }

  static Result t6() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false;
    for (String asserts : new String[] {"false", "true"})
      for (String v : new String[] {"nonudge", "nudge"}) {
        Child ch = runChild("t6", v, "-Doptimus.scheduler.asserts=" + asserts, 60);
        sb.append(String.format("%n  [%s asserts=%s] exit=%d%s exitedOnInvalidate=%s (after %sms) A completed %s; second wait reuses frame=%s; A's late notification cleared the reused frame's waitingOn(B)=%s; T woken early / B's wait disturbed=%s; T returned %sms after B completed; T errors=%s; frame1.waitingOn after B done=%s",
            v, asserts, ch.exit, ch.timedOut || ch.exit == 3 ? " HANG" : "", ch.kv("exitedOnInvalidate"), ch.kv("exitAfterInvalidateMs"), ch.kv("aCompleted"), ch.kv("sameFrame"),
            ch.kv("frameClobbered"), ch.kv("disturbed"), ch.kv("bReturnedAfterBCompleteMs"), ch.kv("tErrors"), ch.kv("frameWaitingOnAfterBDone")));
        for (String l : ch.grep(8, "T6 ", "GraphInInvalidState", "Cannot complete")) sb.append("\n    | ").append(l);
        if ("true".equals(ch.kv("disturbed")) || !"0".equals(ch.kv("tErrors")) || ch.count("GraphInInvalidState") > 0) demo = true;
        if ("duringB".equals(ch.kv("aCompleted")) && "true".equals(ch.kv("sameFrame"))) demo = true; // A's notification reached the reused frame
      }
    return new Result("T6", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  // ================================================================ T9 (F3)

  static final AtomicInteger t9Started = new AtomicInteger(), t9Done = new AtomicInteger();

  static final class LatchNode extends N {
    final CountDownLatch latch;

    LatchNode(ScenarioStack ss, CountDownLatch latch) {
      super("LatchNode", ss);
      this.latch = latch;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      t9Started.incrementAndGet();
      while (true) {
        try {
          latch.await();
          break;
        } catch (InterruptedException ignored) {
        }
      }
      complete(ec);
      t9Done.incrementAndGet();
    }
  }

  static int countOgThreads() {
    int n = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) if (t.getName().startsWith(OGScheduler.ogThreadNamePrefix) && t.isAlive()) n++;
    return n;
  }

  static void t9Child(String variant) throws Exception {
    int calls = Integer.parseInt(variant);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    ThreadMXBean mx = ManagementFactory.getThreadMXBean();
    int base = mx.getThreadCount();
    int ideal = (int) get(field(OGScheduler.class, "_idealCount"), s), idlePoolCount = (int) get(field(OGScheduler.class, "_idlePoolCount"), s);
    kv("ideal", ideal, "idlePoolCount", idlePoolCount, "baseThreads", base, "Xss", ManagementFactory.getRuntimeMXBean().getInputArguments().stream().filter(x -> x.startsWith("-Xss")).findFirst().orElse("default"));
    AtomicInteger peak = new AtomicInteger();
    daemon("t9-sampler", () -> {
      while (true) {
        peak.accumulateAndGet(mx.getThreadCount(), Math::max);
        LockSupport.parkNanos(10_000_000);
      }
    });
    CountDownLatch latch = new CountDownLatch(1);
    int submitted = 0;
    Throwable fail = null;
    long t0 = System.nanoTime();
    for (int i = 0; i < calls; i++) {
      try {
        s.evaluateOnGraphThreadAndTrackCompletion(new LatchNode(ss, latch));
        submitted++;
      } catch (Throwable t) {
        fail = t;
        break;
      }
    }
    long submitMs = (System.nanoTime() - t0) / 1_000_000;
    final int subm = submitted;
    await(() -> t9Started.get() >= subm, 20_000);
    peak.accumulateAndGet(mx.getThreadCount(), Math::max);
    int ogBlocked = countOgThreads();
    int workersCount = ((OGScheduler.Contexts) get(field(OGScheduler.class, "_workers"), s)).count;
    kv("submitted", submitted, "submitMs", submitMs, "fail", fail == null ? "none" : fail.toString(), "startedNodes", t9Started.get(), "ogThreadsWhileBlocked", ogBlocked,
        "peakThreads", peak.get(), "workersCount", workersCount);
    if (fail != null) fail.printStackTrace(System.out);
    latch.countDown();
    final int sub = submitted;
    boolean allDone = await(() -> t9Done.get() >= Math.min(sub, t9Started.get()) && t9Started.get() >= sub, 60_000);
    sleep(3000);
    int after3 = countOgThreads();
    sleep(5000);
    int after8 = countOgThreads();
    int idle = ((OGScheduler.Contexts) get(field(OGScheduler.class, "_idlePool"), s)).count;
    OGScheduler.Contexts workers = (OGScheduler.Contexts) get(field(OGScheduler.class, "_workers"), s);
    // is the scheduler still usable? (a) plain sync get; (b) a get that only an onGraphStalled callback can complete
    Leaf probe = new Leaf(ss);
    Thread after = daemon("t9-after", () -> {
      EvaluationContext.initializeWithoutRuntime();
      OGSchedulerContext.current().runAndWait(probe);
    });
    after.join(10_000);
    Hold stalled = new Hold("T9Stalled", ss);
    s.addOutOfWorkListener(new OutOfWorkListener() {
      @Override
      public void onGraphStalled(Scheduler sch, ArrayList<NodeTask> outstanding) {
        if (!stalled.isDone()) stalled.completeNow(sch);
      }
    });
    Thread after2 = daemon("t9-after-stall", () -> {
      EvaluationContext.initializeWithoutRuntime();
      OGSchedulerContext.current().runAndWait(stalled);
    });
    after2.join(5_000);
    sleep(1000);
    StringBuilder left = new StringBuilder();
    Map<Thread, Long> cpuA = new HashMap<>();
    for (int i = 0; i < workers.count; i++) {
      OGScheduler.Context c = workers.getUnderlyingList()[i];
      if (c != null && c.thread != null) cpuA.put(c.thread, mx.getThreadCpuTime(c.thread.threadId()));
    }
    sleep(1000);
    for (int i = 0; i < workers.count; i++) {
      OGScheduler.Context c = workers.getUnderlyingList()[i];
      if (c != null && c.thread != null && cpuA.containsKey(c.thread))
        left.append("[cpu ").append((mx.getThreadCpuTime(c.thread.threadId()) - cpuA.get(c.thread)) / 1_000_000).append("ms/s spinning=").append(get(field(OGScheduler.class, "_spinningThreadID"), s).equals(c.threadID)).append("]");
      if (c != null) left.append(c.thread == null ? "noThread" : c.thread.getName() + (c.thread.isAlive() ? "" : "(dead)")).append("#").append(c.threadID).append(c.userEntry ? "(user)" : "")
          .append(c.awaitedTask != null ? "(awaits " + c.awaitedTask + (c.awaitedTask.isDone() ? " done" : " never-run") + ")" : "").append(',');
    }
    kv("allDone", allDone, "doneNodes", t9Done.get(), "ogThreadsAfter3s", after3, "ogThreadsAfter8s", after8, "idlePool", idle,
        "workersAfter", workers.count, "workersLeft", left.length() == 0 ? "-" : left, "usableAfter", probe.isDone(), "stallCallbackFired", stalled.isDone());
    Runtime.getRuntime().halt(0);
  }

  static Result t9() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false;
    String[][] cfgs = {{"10000", "-Xss512k -Xmx256m"}, {"10000", "-Xss512k -Xmx256m -Doptimus.gthread.ideal=2"}, {"15000", "-Xss512k -Xmx256m -Doptimus.gthread.ideal=2"}};
    for (String[] c : cfgs) {
      Child ch = runChild("t9", c[0], c[1], 300);
      sb.append(String.format("%n  [%s calls, %s] exit=%d%s ideal=%s idlePoolCount=%s submitted=%s in %sms failure=%s nodesRunning=%s og.worker threads while blocked=%s peak JVM threads=%s (baseline %s) _workers.count=%s; after release: allDone=%s done=%s og.worker threads after 3 s=%s, after 8 s=%s, idlePool=%s, _workers.count=%s [%s], scheduler usable afterwards: sync get=%s, get completed only by onGraphStalled=%s",
          c[0], c[1], ch.exit, ch.timedOut ? " TIMEOUT" : "", ch.kv("ideal"), ch.kv("idlePoolCount"), ch.kv("submitted"), ch.kv("submitMs"), ch.kv("fail"), ch.kv("startedNodes"), ch.kv("ogThreadsWhileBlocked"),
          ch.kv("peakThreads"), ch.kv("baseThreads"), ch.kv("workersCount"), ch.kv("allDone"), ch.kv("doneNodes"), ch.kv("ogThreadsAfter3s"), ch.kv("ogThreadsAfter8s"), ch.kv("idlePool"),
          ch.kv("workersAfter"), ch.kv("workersLeft"), ch.kv("usableAfter"), ch.kv("stallCallbackFired")));
      try {
        if (Integer.parseInt(ch.kv("ogThreadsWhileBlocked")) > 10 * Integer.parseInt(ch.kv("ideal")) || ch.count("OutOfMemoryError") > 0) demo = true;
      } catch (Exception ignored) {
      }
    }
    return new Result("T9", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  // ================================================================ T10 (F4)

  static volatile OGSchedulerContext t10Ec1;
  static volatile String t10EStampAtEnqueue, t10EStampAfterHost, t10ERun, t10WStamp;
  static volatile boolean t10RootDone;

  static final class T10E extends N {
    final NodeTask lower;

    T10E(ScenarioStack ss, NodeTask lower) {
      super("E", ss);
      this.lower = lower;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      // run() has just stamped this node with the running thread's (causality, threadID)
      t10ERun = "ranOn=" + Thread.currentThread().getName() + " tid=" + threadIdOf(ec) + " stamp=" + causalityThreadID() + "/" + causalityID() + " ecCausality=" + get(EC_CAUSALITY, ec)
          + " syncDepth=" + ec.syncStackCount();
      if (lower != null) ec.runAndWait(lower); // sync-wait on a node lower on thread 1's sync stack
      complete(ec);
    }
  }

  static final class T10Seq extends N {
    final NodeTask[] waitOn;

    T10Seq(String name, ScenarioStack ss, NodeTask... waitOn) {
      super(name, ss);
      this.waitOn = waitOn;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      if (name.equals("Root")) t10Ec1 = ec;
      if (name.equals("W")) {
        t10WStamp = "W stamp=" + causalityThreadID() + "/" + causalityID() + " on tid=" + threadIdOf(ec);
        return; // completed by the timer
      }
      for (NodeTask n : waitOn) ec.runAndWait(n);
      complete(ec);
      if (name.equals("Root")) t10RootDone = true;
    }
  }

  static void t10Child(String variant) throws Exception {
    boolean inv = variant.startsWith("inv"), fresh = variant.endsWith("fresh");
    EvaluationContext.initializeWithoutRuntime();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    OGScheduler s = new OGScheduler("t10-", 2, Thread.NORM_PRIORITY); // fresh threadCounter: first injected worker gets id 1
    var timer = timer("t10-timer", 1);
    T10Seq w = new T10Seq("W", ss);
    T10Seq l = new T10Seq("L", ss, w);
    T10Seq root = new T10Seq("Root", ss, l);
    s.evaluateOnGraphThreadAndTrackCompletion(root); // injectThread(root): Context id 1 runs Root -> L -> W
    if (!await(() -> t10Ec1 != null && t10WStamp != null && ctxOf(t10Ec1).parked, 5000)) System.out.println("T10 thread 1 did not park on W");
    OGScheduler.Context c1 = ctxOf(t10Ec1);
    String t1 = "thread1=" + c1.thread.getName() + " id=" + c1.threadID + " awaited=" + c1.awaitedTask + " awaitedCausality=" + c1.awaitedCausalityID + " syncDepth=" + t10Ec1.syncStackCount();
    T10E e = new T10E(ss, inv ? l : null);
    NodeTask host = new Hold("Host", ss);
    Thread x = daemon("t10-external", () -> {
      if (!fresh) {
        s.asIfRunning(host, () -> {
          OGSchedulerContext.current().enqueue(e);
          t10EStampAtEnqueue = e.causalityThreadID() + "/" + e.causalityID() + " (asIfRunning ctx id=" + threadIdOf(OGSchedulerContext.current()) + ")";
          return null;
        });
      } else {
        // asIfRunning with a fresh id instead of 1
        OGScheduler.Context ctx = new OGScheduler.Context(Thread.currentThread(), 30001);
        var tmp = new OGSchedulerContext(host, s, ctx);
        try {
          OGSchedulerContext.setCurrent(tmp);
          tmp.enqueue(e);
          t10EStampAtEnqueue = e.causalityThreadID() + "/" + e.causalityID() + " (emulated ctx id=30001)";
          tmp.localCleanup(s);
        } finally {
          OGSchedulerContext.setCurrent(null);
        }
      }
      t10EStampAfterHost = e.causalityThreadID() + "/" + e.causalityID();
    });
    x.join(5000);
    timer.schedule(() -> w.completeNow(s), 1000, TimeUnit.MILLISECONDS); // W completes externally after 1 s
    boolean rootDone = await(() -> t10RootDone, 6000);
    String c1After = "thread1.awaited=" + c1.awaitedTask + " parked=" + c1.parked + " syncStack=" + get(EC_SYNCSTACK, t10Ec1);
    System.out.println("T10 " + t1);
    System.out.println("T10 " + t10WStamp);
    System.out.println("T10 E stamp in asIfRunning after enqueue: " + t10EStampAtEnqueue + "; after host returned: " + t10EStampAfterHost + "; at run: " + t10ERun);
    System.out.println("T10 at end: " + c1After + " E.done=" + e.isDone() + " L.done=" + l.isDone() + " W.done=" + w.isDone());
    kv("rootDone", rootDone, "eStampAtEnqueue", t10EStampAtEnqueue, "eStampAfter", t10EStampAfterHost, "eRanOnTid1", t10ERun != null && t10ERun.contains("tid=1 "));
    if (!rootDone) dumpAndHalt("t10 root not done (thread 1 waits on L, which is on its own sync stack)", s, 3, "og.worker-", "t10-");
    Runtime.getRuntime().halt(0);
  }

  static Result t10() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean stamped1 = false;
    boolean idDependent = false;
    String invHang = null, freshHang = null;
    for (String v : new String[] {"stamp", "inv", "inv-fresh"}) {
      Child ch = runChild("t10", v, "-Doptimus.gthread.ideal=2", 60);
      boolean hang = ch.exit == 3 || ch.timedOut;
      sb.append(String.format("%n  [%s] exit=%d %s eStampAtEnqueue=%s eStampAfterHost=%s eRanOnThread1=%s", v, ch.exit, hang ? "DEADLOCK" : "completed", ch.kv("eStampAtEnqueue"), ch.kv("eStampAfter"),
          ch.kv("eRanOnTid1")));
      for (String l : ch.grep(6, "T10 ")) sb.append("\n    | ").append(l);
      String st = ch.kv("eStampAtEnqueue");
      if (st != null && st.startsWith("1/")) stamped1 = true;
      st = ch.kv("eStampAfter");
      if (st != null && st.startsWith("1/")) stamped1 = true;
      if (v.equals("inv")) invHang = String.valueOf(hang);
      if (v.equals("inv-fresh")) freshHang = String.valueOf(hang);
    }
    idDependent = invHang != null && !invHang.equals(freshHang);
    sb.append(String.format("%n  asIfRunning stamped id 1 on an enqueued node: %s; deadlock with id 1: %s, with fresh id 30001: %s (id-dependent: %s)", stamped1, invHang, freshHang, idDependent));
    return new Result("T10", stamped1 || idDependent ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  // ================================================================ T11 (I1)

  static final class BenchNode extends N {
    final boolean expensive;
    volatile boolean ran;
    final Object[] args;

    BenchNode(ScenarioStack ss, boolean expensive, Object[] args) {
      super("BenchNode", ss);
      this.expensive = expensive;
      this.args = args;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      ran = true;
    }

    @Override
    public PrettyStringBuilder writePrettyString(PrettyStringBuilder sb) {
      super.writePrettyString(sb);
      if (expensive) {
        sb.append("(");
        for (int i = 0; i < args.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(String.valueOf(args[i]));
        }
        sb.append(")");
      }
      return sb;
    }

    @Override
    public String toString() {
      return toPrettyName(false, false, true); // NodeTask.toString, i.e. what fullWait calls
    }
  }

  static Object[] benchArgs() {
    Object[] a = new Object[20];
    for (int i = 0; i < 20; i++)
      a[i] = switch (i % 4) {
        case 0 -> 1234.5678 * i;
        case 1 -> "instrument-" + i + "-USD-XLON";
        case 2 -> java.time.LocalDate.of(2026, 1 + i % 12, 1 + i);
        default -> List.of(i, i + 1, i + 2);
      };
    return a;
  }

  static long median(long[] xs) {
    long[] c = xs.clone();
    Arrays.sort(c);
    return c[c.length / 2];
  }

  static long pct(long[] xs, double p) {
    long[] c = xs.clone();
    Arrays.sort(c);
    return c[(int) Math.min(c.length - 1, c.length * p)];
  }

  static volatile BenchNode t11Cur;
  static final long T11_DELAY_NS = Long.getLong("t11.delayUs", 0) * 1000;

  static long[] t11Run(OGSchedulerContext ec, OGScheduler.Context ctx, ScenarioStack ss, boolean expensive, int depth, int iters) {
    long[] r = new long[iters];
    Object[] args = benchArgs();
    for (int i = 0; i < iters; i++) {
      BenchNode n = new BenchNode(ss, expensive, args);
      t11Cur = n;
      long t0 = System.nanoTime();
      atDepth(depth, () -> ec.runAndWait(n));
      r[i] = System.nanoTime() - t0;
    }
    return r;
  }

  static void atDepth(int d, Runnable r) {
    if (d <= 0) r.run();
    else atDepth(d - 1, r);
  }

  static long t11Sink;

  static void t11Child(String variant) throws Exception {
    int iters = Integer.getInteger("t11.iters", 20000), warm = 5000;
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    Map<String, Object> out = new LinkedHashMap<>();
    AtomicLong completedParked = new AtomicLong(), completedUnparked = new AtomicLong();
    // completer: hands off as soon as the client has run the node and parked in fullWait
    OGScheduler.Context[] clientCtx = new OGScheduler.Context[1];
    daemon("t11-completer", () -> {
      while (true) {
        BenchNode n = t11Cur;
        OGScheduler.Context c = clientCtx[0];
        if (n != null && n.ran && c != null) {
          if (c.parked) {
            if (T11_DELAY_NS > 0) {
              long until = System.nanoTime() + T11_DELAY_NS;
              while (System.nanoTime() < until) Thread.onSpinWait();
            }
            t11Cur = null;
            n.completeNow(s);
            completedParked.incrementAndGet();
          }
        } else Thread.onSpinWait();
      }
    });
    CountDownLatch done = new CountDownLatch(1);
    daemon("t11-client", () -> {
      try {
        EvaluationContext.initializeWithoutRuntime();
        OGSchedulerContext ec = OGSchedulerContext.current();
        clientCtx[0] = ctxOf(ec);
        ScenarioStack ss = EvaluationContext.scenarioStack();
        for (boolean exp : new boolean[] {false, true})
          for (int depth : new int[] {0, 200}) t11Run(ec, clientCtx[0], ss, exp, depth, warm);
        for (int rep = 0; rep < 2; rep++)
          for (boolean exp : new boolean[] {false, true})
            for (int depth : new int[] {0, 200}) {
              long[] r = t11Run(ec, clientCtx[0], ss, exp, depth, iters);
              String k = (exp ? "expensive" : "cheap") + "-d" + depth;
              out.put(k + "-r" + rep, String.format("median=%.2fus p10=%.2fus p90=%.2fus", median(r) / 1000.0, pct(r, 0.1) / 1000.0, pct(r, 0.9) / 1000.0));
              out.put("med:" + k + ":" + rep, median(r));
            }
        // the two allocations fullWait makes unconditionally, timed in isolation
        Object[] args = benchArgs();
        for (boolean exp : new boolean[] {false, true}) {
          BenchNode n = new BenchNode(ss, exp, args);
          long[] r = new long[iters];
          for (int i = 0; i < warm; i++) t11Sink += n.toString().length();
          for (int i = 0; i < iters; i++) {
            long t0 = System.nanoTime();
            t11Sink += n.toString().length();
            r[i] = System.nanoTime() - t0;
          }
          out.put("toString-" + (exp ? "expensive" : "cheap"), median(r));
          if (exp) out.put("expensiveToStringChars", (long) n.toString().length());
        }
        for (int depth : new int[] {0, 200}) {
          long[] r = new long[iters];
          for (int i = 0; i < warm; i++) atDepth(depth, () -> t11Sink += new Exception("Blocking Wait").hashCode());
          for (int i = 0; i < iters; i++) {
            long[] t = new long[1];
            atDepth(depth, () -> {
              long t0 = System.nanoTime();
              t11Sink += new Exception("Blocking Wait").hashCode();
              t[0] = System.nanoTime() - t0;
            });
            r[i] = t[0];
          }
          out.put("exception-d" + depth, median(r));
          out.put("stackDepth-d" + depth, atDepthStack(depth));
        }
      } catch (Throwable t) {
        System.out.println("T11 client threw " + t);
        t.printStackTrace(System.out);
      } finally {
        done.countDown();
      }
    });
    if (!done.await(600, TimeUnit.SECONDS)) dumpAndHalt("t11 not done", s, 3, "t11-");
    out.forEach((k, v) -> System.out.println("T11 " + k + " " + v));
    List<Object> kvs = new ArrayList<>();
    out.forEach((k, v) -> {
      if (!(v instanceof String)) {
        kvs.add(k.replace(':', '_'));
        kvs.add(v);
      }
    });
    kvs.add("handoffs");
    kvs.add(completedParked.get());
    kv(kvs.toArray());
    Runtime.getRuntime().halt(0);
  }

  static int atDepthStack(int d) {
    int[] n = new int[1];
    atDepth(d, () -> n[0] = new Exception().getStackTrace().length);
    return n[0];
  }

  static Result t11() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean ok = true;
    for (String delay : new String[] {"0", "50"}) {
      Child ch = runChild("t11", "bench", "-Doptimus.gthread.ideal=2 -Dt11.delayUs=" + delay, 900);
      ok &= ch.exit == 0;
      sb.append(String.format("%n  [completer hands off %s us after the client sets parked] exit=%d handoffs=%s", delay, ch.exit, ch.kv("handoffs")));
      try {
        double tsC = Long.parseLong(ch.kv("toString-cheap")) / 1000.0, tsE = Long.parseLong(ch.kv("toString-expensive")) / 1000.0;
        double ex0 = Long.parseLong(ch.kv("exception-d0")) / 1000.0, ex200 = Long.parseLong(ch.kv("exception-d200")) / 1000.0;
        sb.append(String.format("; isolated medians: toString cheap %.3f us, expensive (20 args, %s chars) %.3f us; new Exception at stack depth %s %.3f us, at depth %s %.3f us",
            tsC, ch.kv("expensiveToStringChars"), tsE, ch.kv("stackDepth-d0"), ex0, ch.kv("stackDepth-d200"), ex200));
        for (String k : new String[] {"cheap-d0", "expensive-d0", "cheap-d200", "expensive-d200"}) {
          double med = Long.parseLong(ch.kv("med_" + k + "_1")) / 1000.0, med0 = Long.parseLong(ch.kv("med_" + k + "_0")) / 1000.0;
          double ts = k.startsWith("cheap") ? tsC : tsE, ex = k.endsWith("d0") ? ex0 : ex200;
          sb.append(String.format("%n    %-15s median blocking sync get %.2f us (rep0 %.2f us); toString + new Exception = %.2f us = %.0f%% of it", k, med, med0, ts + ex, 100 * (ts + ex) / med));
        }
        double dE = (Long.parseLong(ch.kv("med_expensive-d200_1")) - Long.parseLong(ch.kv("med_cheap-d0_1"))) / 1000.0;
        sb.append(String.format("%n    end to end: expensive node at depth 200 minus cheap node at depth 0 = %+.2f us per blocking wait", dE));
      } catch (Exception e) {
        sb.append("\n  could not compute shares: " + e);
      }
    }
    return new Result("T11", ok ? Outcome.QUANTIFIED : Outcome.ERROR, sb.toString());
  }

  // ================================================================ T12 (I2)

  static volatile long t12Start, t12End;

  static final class Busy extends N {
    final long ms;
    volatile String ranOnName;

    Busy(ScenarioStack ss, long ms) {
      super("Busy", ss);
      this.ms = ms;
    }

    @Override
    public void run(OGSchedulerContext ec) {
      ranOnName = Thread.currentThread().getName();
      t12Start = System.nanoTime();
      long end = t12Start + ms * 1_000_000;
      double x = 1;
      while (System.nanoTime() < end) x = Math.sin(x) + 1.0001;
      value = (long) x;
      t12End = System.nanoTime();
      complete(ec);
    }
  }

  static void t12Child(String variant) throws Exception {
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = defaultScheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    ThreadMXBean mx = ManagementFactory.getThreadMXBean();
    Busy busy = new Busy(ss, 5000);
    kv("ideal", get(field(OGScheduler.class, "_idealCount"), s), "limitThreads", Settings.limitThreads, "variant", variant);
    Map<Long, long[]> cpu0 = new HashMap<>();
    Map<Long, String> names = new HashMap<>();
    Map<Long, Integer> hotSamples = new HashMap<>();
    Map<Long, List<Long>> deltas = new HashMap<>();
    int[] samples = {0};
    CountDownLatch done = new CountDownLatch(1);
    if (variant.equals("client")) {
      daemon("t12-client", () -> {
        try {
          EvaluationContext.initializeWithoutRuntime();
          OGSchedulerContext.current().runAndWait(busy); // the client runs the busy node itself
        } finally {
          done.countDown();
        }
      });
    } else {
      // the busy node is picked up by an og worker; nothing waits on it in-graph
      busy.continueWith(new NodeAwaiter() {
        @Override
        protected void onChildCompleted(EvaluationQueue eq, NodeTask node) {
          done.countDown();
        }
      }, s);
      s.enqueue(busy);
    }
    await(() -> t12Start != 0, 10_000);
    Map<Long, Long> prev = new HashMap<>();
    long sampleStart = System.nanoTime();
    while (done.getCount() > 0 && System.nanoTime() - sampleStart < 10_000_000_000L) {
      for (Thread t : Thread.getAllStackTraces().keySet()) {
        String n = t.getName();
        if (!n.startsWith("og.worker") && !n.startsWith("t12-")) continue;
        long id = t.threadId(), c = mx.getThreadCpuTime(id);
        if (c < 0) continue;
        names.put(id, n);
        cpu0.computeIfAbsent(id, k -> new long[] {c, c});
        cpu0.get(id)[1] = c;
        Long p = prev.put(id, c);
        if (p != null && c - p >= 80_000_000L) hotSamples.merge(id, 1, Integer::sum); // >= 80% of a 100 ms interval
        if (p != null) deltas.computeIfAbsent(id, k -> new ArrayList<>()).add(c - p);
      }
      samples[0]++;
      LockSupport.parkNanos(100_000_000L);
    }
    done.await(10, TimeUnit.SECONDS);
    long busyMs = (t12End - t12Start) / 1_000_000;
    StringBuilder per = new StringBuilder();
    int hotOthers = 0;
    for (var e : cpu0.entrySet()) {
      long id = e.getKey();
      double sec = (e.getValue()[1] - e.getValue()[0]) / 1e9;
      int hot = hotSamples.getOrDefault(id, 0);
      boolean isBusyRunner = busy.ranOnName != null && busy.ranOnName.equals(names.get(id));
      long[] d = deltas.getOrDefault(id, List.of(0L)).stream().mapToLong(Long::longValue).toArray();
      per.append(names.get(id)).append(isBusyRunner ? "(busy-node)" : "").append('=').append(String.format("%.2fs", sec)).append("/hot").append(hot)
          .append(String.format("/per100ms_p10_med_p90=%d%%,%d%%,%d%%", pct(d, 0.1) / 1_000_000, median(d) / 1_000_000, pct(d, 0.9) / 1_000_000)).append(';');
      if (!isBusyRunner && hot >= samples[0] * 0.8 - 2) hotOthers++;
    }
    kv("busyMs", busyMs, "samples", samples[0], "perThreadCpu", per, "otherThreadsHot80pct", hotOthers, "busyRanOn", busy.ranOnName);
    Runtime.getRuntime().halt(0);
  }

  static Result t12() throws Exception {
    StringBuilder sb = new StringBuilder();
    boolean demo = false;
    for (String v : new String[] {"client", "worker"}) {
      Child ch = runChild("t12", v, "-Doptimus.gthread.ideal=4", 60);
      sb.append(String.format("%n  [%s runs the 5 s busy node, ideal=4] exit=%d busy=%sms ranOn=%s samples(100ms)=%s cpu (thread=cpuSeconds/samples>=80%%): %s other threads >=80%% CPU for the duration: %s",
          v, ch.exit, ch.kv("busyMs"), ch.kv("busyRanOn"), ch.kv("samples"), ch.kv("perThreadCpu"), ch.kv("otherThreadsHot80pct")));
      try {
        if (Integer.parseInt(ch.kv("otherThreadsHot80pct")) > 0) demo = true;
      } catch (Exception ignored) {
      }
    }
    return new Result("T12", demo ? Outcome.DEMONSTRATED : Outcome.QUANTIFIED, sb.toString());
  }

  // ---------------------------------------------------------------- main

  public static void main(String[] args) throws Exception {
    if (args.length >= 3 && args[0].equals("--child")) {
      switch (args[1]) {
        case "t3" -> t3Child(args[2]);
        case "t4" -> t4Child(args[2]);
        case "t5" -> t5Child(args[2]);
        case "t6" -> t6Child(args[2]);
        case "t9" -> t9Child(args[2]);
        case "t10" -> t10Child(args[2]);
        case "t11" -> t11Child(args[2]);
        case "t12" -> t12Child(args[2]);
        default -> throw new IllegalArgumentException(args[1]);
      }
      return;
    }
    Set<String> only = args.length == 0 ? null : Set.of(args);
    List<Result> results = new ArrayList<>();
    for (var e : List.<Map.Entry<String, java.util.concurrent.Callable<Result>>>of(
        Map.entry("T3", Tier1b::t3), Map.entry("T4", Tier1b::t4), Map.entry("T5", Tier1b::t5), Map.entry("T6", Tier1b::t6), Map.entry("T9", Tier1b::t9),
        Map.entry("T10", Tier1b::t10), Map.entry("T11", Tier1b::t11), Map.entry("T12", Tier1b::t12))) {
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
    System.out.println("==== child commands (PROBES_JAVA_CMD from run-tier1b.sh --print-cmd)");
    for (String c : commands) System.out.println(c);
    System.exit(0);
  }
}

/**
 * Load-time agent for T3: rewrites optimus.core.IndexedArrayList.foreach to call
 * Tier1b.Hooks.feEnter(this) on entry, Tier1b.Hooks.fePoint(this) after each element load (where a
 * random park is inserted with -Dprobes.delay.maxNanos), and Tier1b.Hooks.feExit(this) before
 * return. The hooks ignore every list except OGScheduler._evalAsyncTrackedNodes.
 */
final class Tier1bAgent {
  static final String TARGET = "optimus/core/IndexedArrayList";
  static final String HOOKS = "optimus/graph/Tier1b$Hooks";

  public static void premain(String args, Instrumentation inst) {
    inst.addTransformer(new ClassFileTransformer() {
      @Override
      public byte[] transform(ClassLoader loader, String name, Class<?> cls, ProtectionDomain pd, byte[] bytes) {
        if (!TARGET.equals(name)) return null;
        try {
          return instrument(bytes);
        } catch (Throwable t) {
          System.err.println("Tier1bAgent: failed to instrument " + name + ": " + t);
          t.printStackTrace();
          return null;
        }
      }
    });
  }

  static byte[] instrument(byte[] bytes) {
    ClassReader cr = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
    StringBuilder log = new StringBuilder();
    cr.accept(new ClassVisitor(Opcodes.ASM9, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String m, String desc, String sig, String[] exc) {
        MethodVisitor mv = super.visitMethod(access, m, desc, sig, exc);
        if (!m.equals("foreach")) return mv;
        return new MethodVisitor(Opcodes.ASM9, mv) {
          void hook(String h) {
            super.visitVarInsn(Opcodes.ALOAD, 0);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOKS, h, "(Ljava/lang/Object;)V", false);
            log.append(' ').append(h);
          }

          @Override
          public void visitCode() {
            super.visitCode();
            hook("feEnter");
          }

          @Override
          public void visitInsn(int op) {
            if (op == Opcodes.RETURN) hook("feExit");
            super.visitInsn(op);
            if (op == Opcodes.AALOAD) hook("fePoint");
          }
        };
      }
    }, 0);
    System.err.println("Tier1bAgent: instrumented " + TARGET + ".foreach:" + log);
    System.setProperty("tier1b.agent.instrumented", log.toString().trim().replace(' ', ','));
    return cw.toByteArray();
  }
}
