package optimus.graph;

import static optimus.graph.Probes.Outcome.DEMONSTRATED;
import static optimus.graph.Probes.Outcome.ERROR;
import static optimus.graph.Probes.Outcome.NOT_REPRODUCED;
import static optimus.graph.Probes.field;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import optimus.core.IndexedArrayList;
import optimus.graph.Probes.Result;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import probes.Steps;

/** Remaining Tier 0 probes that need no delay injection. */
final class Probes2 {

  /** Task that completes itself when run. Attached to a scenario stack so complete() is safe. */
  static class Completing extends NodeTask {
    final int prio;
    final String name;

    Completing(String name, int prio, ScenarioStack ss) {
      this.name = name;
      this.prio = prio;
      attach(ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      complete(ec);
    }

    @Override
    public int priority() {
      return prio;
    }

    void completeNow(EvaluationQueue eq) {
      complete(eq);
    }

    void primeRunning(ScenarioStack ss) {
      initAsRunning(ss);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** Task whose run() blocks on a latch, then completes: keeps its worker thread busy. */
  static final class Holding extends Completing {
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch started = new CountDownLatch(1);

    Holding(ScenarioStack ss) {
      super("Holding", 0, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      started.countDown();
      try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) {}
      complete(ec);
    }
  }

  /** Task that does nothing when run: it must be completed by another thread. */
  static final class Blocking extends Completing {
    Blocking(ScenarioStack ss) {
      super("Blocking", 0, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {}
  }

  static final class CountingAwaiter extends NodeAwaiter {
    final AtomicInteger calls = new AtomicInteger();
    final String name;
    final List<String> order;

    CountingAwaiter(String name, List<String> order) {
      this.name = name;
      this.order = order;
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask node) {
      calls.incrementAndGet();
      if (order != null) order.add(name);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static ScenarioStack ss() {
    return EvaluationContext.scenarioStack();
  }

  static OGSchedulerContext ec() {
    return OGSchedulerContext.current();
  }

  /** Runs body on a fresh graph-initialised thread and waits. */
  static void onGraphThread(String name, Runnable body) throws Exception {
    Throwable[] err = new Throwable[1];
    Thread t =
        new Thread(
            () -> {
              try {
                EvaluationContext.initializeWithoutRuntime();
                body.run();
              } catch (Throwable e) {
                err[0] = e;
              }
            },
            name);
    t.start();
    t.join(20_000);
    if (t.isAlive()) throw new IllegalStateException(name + " did not finish");
    if (err[0] != null) throw new RuntimeException(err[0]);
  }

  // ---------- W1g: growArray with a sync marker keeps positions ----------
  static Result w1g() throws Exception {
    OGSchedulerContext ctx = ec();
    java.lang.reflect.Field causality = field(OGSchedulerContext.class, "currentCausalityID");
    for (int cid : new int[] {1, 0}) {
      var q = Probes.newQueue(0, 0);
      q.schedulerContext = ctx;
      causality.setInt(ctx, cid);
      try {
        List<NodeTask> a = new ArrayList<>(), b = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
          var t = new Probes.T(i);
          a.add(t);
          q.localPush(t);
        }
        int marker = q.getTop(); // sync frame starts here
        for (int i = 0; i < 20; i++) q.localPush(new Probes.DoneT());
        for (int i = 0; i < 23; i++) { // 63rd push triggers growArray
          var t = new Probes.T(100 + i);
          b.add(t);
          q.localPush(t);
        }
        List<NodeTask> popped = new ArrayList<>();
        NodeTask t;
        while ((t = q.localPop(marker)) != null) popped.add(t);
        boolean leaked = popped.stream().anyMatch(a::contains);
        if (cid == 1 && leaked)
          return new Result("W1g", DEMONSTRATED, "causality=1: localPop(marker) returned a pre-marker task: " + popped);
        if (cid == 1 && !popped.containsAll(b))
          return new Result("W1g", DEMONSTRATED, "causality=1: not all post-marker tasks poppable: " + popped);
        if (cid == 0 && !leaked)
          return new Result(
              "W1g",
              NOT_REPRODUCED,
              "causality=1: marker fence held across growArray (placeholders kept positions); causality=0: dones dropped but no pre-marker task surfaced either");
        if (cid == 0)
          return new Result(
              "W1g",
              NOT_REPRODUCED,
              "causality=1: marker fence held across growArray; causality=0 (no sync frame, marker meaningless by design): dropping dones shifted pre-marker tasks above the old marker, as expected");
      } finally {
        causality.setInt(ctx, 0);
      }
    }
    return new Result("W1g", ERROR, "unreachable");
  }

  // ---------- W2b: foreach vs add-with-grow ----------
  static Result w2b() {
    var list =
        new IndexedArrayList<Probes.Item>() {
          @Override
          protected Probes.Item[] createInitialArray() {
            return new Probes.Item[4];
          }
        };
    for (int i = 0; i < 4; i++) list.add(new Probes.Item("i" + i));
    List<Probes.Item> visited = Collections.synchronizedList(new ArrayList<>());
    Steps.schedule("A1 B1 B2 A2")
        .thread(ctl -> list.foreach(it -> { visited.add(it); ctl.step(); }))
        .thread(
            ctl -> {
              ctl.step();
              for (int i = 4; i < 8; i++) list.add(new Probes.Item("i" + i)); // grows 4 -> 8
              ctl.step();
            })
        .run();
    return new Result(
        "W2b",
        visited.size() == 4 ? DEMONSTRATED : NOT_REPRODUCED,
        "visited " + visited.size() + " of " + list.size() + " (iterator kept the pre-grow array): " + visited);
  }

  // ---------- W2c: tryStealing vs concurrent add/pop on Contexts ----------
  static Result w2c() throws Exception {
    OGScheduler s = new OGScheduler("w2c", 2, Thread.NORM_PRIORITY);
    var contexts = new OGScheduler.Contexts("probe");
    Method tryStealing = OGScheduler.class.getDeclaredMethod("tryStealing", OGScheduler.Context.class, OGScheduler.Contexts.class);
    tryStealing.setAccessible(true);
    var requester = new OGScheduler.Context(Thread.currentThread(), 9000);
    List<OGScheduler.Context> pool = new ArrayList<>();
    for (int i = 0; i < 8; i++) pool.add(new OGScheduler.Context(Thread.currentThread(), 9001 + i));
    var stop = new java.util.concurrent.atomic.AtomicBoolean();
    Throwable[] err = new Throwable[1];
    Thread mutator =
        new Thread(
            () -> {
              int k = 0;
              while (!stop.get()) {
                synchronized (contexts) {
                  if (contexts.count < 8) contexts.add(pool.get(contexts.count));
                  else while (contexts.count > 0) contexts.pop();
                }
                k++;
              }
            });
    mutator.start();
    long calls = 0;
    long end = System.nanoTime() + 500_000_000L;
    try {
      while (System.nanoTime() < end) {
        tryStealing.invoke(null, requester, contexts);
        calls++;
      }
    } catch (Throwable t) {
      err[0] = t.getCause() != null ? t.getCause() : t;
    } finally {
      stop.set(true);
      mutator.join();
    }
    return new Result(
        "W2c",
        err[0] == null ? NOT_REPRODUCED : DEMONSTRATED,
        calls + " tryStealing calls racing add/pop: " + (err[0] == null ? "no exception (Math.min(count, length) + null check hold)" : err[0]));
  }

  // ---------- W3a: continueWith vs complete, every waiter exactly once ----------
  static Result w3a() throws Exception {
    int rounds = 2000, adders = 4;
    int[] worst = {0};
    for (int r = 0; r < rounds; r++) {
      var node = new Completing("n" + r, 0, ss());
      node.primeRunning(ss());
      var waiters = new ArrayList<CountingAwaiter>();
      for (int i = 0; i < adders; i++) waiters.add(new CountingAwaiter("w" + i, null));
      var barrier = new CyclicBarrier(adders + 1);
      var threads = new ArrayList<Thread>();
      for (int i = 0; i < adders; i++) {
        var w = waiters.get(i);
        var t =
            new Thread(
                () -> {
                  EvaluationContext.initializeWithoutRuntime();
                  try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                  node.continueWith(w, ec());
                });
        t.start();
        threads.add(t);
      }
      var completer =
          new Thread(
              () -> {
                EvaluationContext.initializeWithoutRuntime();
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                node.completeNow(ec());
              });
      completer.start();
      threads.add(completer);
      for (Thread t : threads) t.join();
      for (var w : waiters) {
        int c = w.calls.get();
        if (c != 1) return new Result("W3a", DEMONSTRATED, "round " + r + ": waiter " + w + " called " + c + " times");
      }
    }
    return new Result("W3a", NOT_REPRODUCED, rounds + " rounds × " + adders + " racing continueWith vs complete: every waiter called exactly once");
  }

  // ---------- W3b: continueWithLast ordering ----------
  static Result w3b() throws Exception {
    List<String> order = Collections.synchronizedList(new ArrayList<>());
    var node = new Completing("w3b", 0, ss());
    node.primeRunning(ss());
    node.continueWith(new CountingAwaiter("first", order), ec());
    node.continueWithLast(new CountingAwaiter("last", order), ec());
    node.continueWith(new CountingAwaiter("second", order), ec());
    node.continueWithLast(new CountingAwaiter("last2", order), ec());
    node.completeNow(ec());
    boolean ok = order.equals(List.of("second", "first", "last", "last2"));
    return new Result("W3b", ok ? NOT_REPRODUCED : DEMONSTRATED, "notification order: " + order + " (expected [second, first, last, last2]: LIFO for continueWith, FIFO at the tail for continueWithLast)");
  }

  // ---------- W3c: double completion kills the JVM (child process) ----------
  static Result w3c() throws Exception {
    String cmd = System.getenv("PROBES_JAVA_CMD");
    if (cmd == null) return new Result("W3c", ERROR, "PROBES_JAVA_CMD not set; run via run.sh");
    StringBuilder sb = new StringBuilder();
    boolean swallowed = false, killed = false;
    for (String variant : new String[] {"complete-complete", "try-complete"}) {
      Process p = new ProcessBuilder("sh", "-c", cmd + " --child w3c " + variant).redirectErrorStream(true).start();
      boolean sawTwice = false, sawRecursive = false, survived = false;
      try (var r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
        String l;
        while ((l = r.readLine()) != null) {
          sawTwice |= l.contains("Cannot complete node twice");
          sawRecursive |= l.contains("logAndDie called recursively");
          survived |= l.contains("W3c-child survived");
        }
      }
      boolean done = p.waitFor(60, TimeUnit.SECONDS);
      int code = done ? p.exitValue() : -1;
      if (!done) p.destroyForcibly();
      sb.append(variant).append(": exit=").append(code).append(" doubleCompleteDetected=").append(sawTwice)
          .append(" killSwallowedByRecursionGuard=").append(sawRecursive).append(" survived=").append(survived).append("; ");
      killed |= code == 121;
      swallowed |= sawTwice && sawRecursive && survived;
    }
    sb.append(killed ? "(exit 121 as designed)" : swallowed
        ? "(InfoDumper.<clinit> threw in this environment (missing internal utils.conf); the exception escaped logAndDie, complete()'s catch re-entered logAndDie, the static inLogAndDie guard returned, and the JVM continued with a double-completed node; the guard is never reset so every later logAndDie is also a no-op)"
        : "(no double completion observed: ordering made the second completer see isDone)");
    return new Result("W3c", killed || swallowed ? DEMONSTRATED : NOT_REPRODUCED, sb.toString());
  }

  static void w3cChild(String variant) throws Exception {
    EvaluationContext.initializeWithoutRuntime();
    if (variant.equals("panic")) {
      System.out.println("W3c-child calling InfoDumper.graphPanic directly");
      try {
        optimus.graph.diagnostics.InfoDumper$.MODULE$.graphPanic("probe", "probe panic", 121, new RuntimeException("probe"), null);
        System.out.println("W3c-child graphPanic returned normally");
      } catch (Throwable t) {
        System.out.println("W3c-child graphPanic threw: " + t);
        t.printStackTrace(System.out);
      }
      System.exit(0);
    }
    var node = new Completing("w3c", 0, ss());
    node.primeRunning(ss());
    System.out.println("W3c-child start " + variant);
    var barrier = new CyclicBarrier(2);
    Runnable a = () -> { EvaluationContext.initializeWithoutRuntime(); try { barrier.await(); } catch (Exception e) {} node.completeNow(ec()); };
    Runnable b =
        variant.equals("try-complete")
            ? () -> { EvaluationContext.initializeWithoutRuntime(); try { barrier.await(); } catch (Exception e) {} node.tryCompleteWithException(new RuntimeException("try"), ec()); }
            : a;
    Thread ta = new Thread(a, "w3c-a"), tb = new Thread(b, "w3c-b");
    ta.start(); tb.start(); ta.join(10_000); tb.join(10_000);
    System.out.println("W3c-child survived: state=" + node.stateAsString() + " done=" + node.isDone());
    System.exit(0);
  }

  // ---------- W3e: invalidateCache on an in-flight node leaves waiters registered ----------
  static Result w3e() {
    var node = new Completing("w3e", 0, ss());
    node.primeRunning(ss());
    var w = new CountingAwaiter("w", null);
    node.continueWithIfEverRuns(w, ec());
    node.invalidateCache(null);
    boolean doneEx = node.isDoneEx();
    int callsAfterInvalidate = w.calls.get();
    boolean waitingOnStillSet = w.getWaitingOn() == node;
    node.completeNow(ec());
    int callsAfterComplete = w.calls.get();
    boolean stale = doneEx && callsAfterInvalidate == 0 && callsAfterComplete == 1;
    return new Result(
        "W3e",
        stale ? DEMONSTRATED : NOT_REPRODUCED,
        "after invalidateCache: isDoneEx=" + doneEx + " waiter calls=" + callsAfterInvalidate + " waiter.waitingOn==node=" + waitingOnStillSet
            + "; after a later complete(): waiter calls=" + callsAfterComplete + " (a runAndWait that exited on isDoneEx would get this stale notification)");
  }

  // ---------- W3f: markAsRunnable on a completed node ----------
  static Result w3f() {
    var node = new Completing("w3f", 0, ss());
    node.primeRunning(ss());
    node.completeNow(ec());
    String before = node.stateAsString();
    try {
      node.markAsRunnable();
    } catch (GraphInInvalidState e) {
      return new Result("W3f", NOT_REPRODUCED, "asserts on: threw " + e.getMessage());
    }
    String after = node.stateAsString();
    return new Result(
        "W3f",
        before.equals(after) && !node.isRunnable() ? DEMONSTRATED : ERROR,
        "schedulerAsserts=" + Settings.schedulerAsserts + ": markAsRunnable on a done node silently ignored (state " + before + " -> " + after + ")");
  }

  // ---------- W3g: tryRun idempotence ----------
  static Result w3g() throws Exception {
    int rounds = 20_000, both = 0, none = 0;
    for (int r = 0; r < rounds; r++) {
      var node = new Completing("w3g", 0, ss());
      int[] prev = new int[2];
      var barrier = new CyclicBarrier(2);
      Thread t = new Thread(() -> { try { barrier.await(); } catch (Exception e) {} prev[1] = node.tryRun(); });
      t.start();
      barrier.await();
      prev[0] = node.tryRun();
      t.join();
      boolean a = (prev[0] & NodeTask.STATE_NOT_RUNNABLE) == 0, b = (prev[1] & NodeTask.STATE_NOT_RUNNABLE) == 0;
      if (a && b) both++;
      if (!a && !b) none++;
    }
    return new Result("W3g", both + none == 0 ? NOT_REPRODUCED : DEMONSTRATED, rounds + " racing tryRun pairs: both-won=" + both + " neither-won=" + none);
  }

  // ---------- W4a: SyncFrame reuse keeps a stale waitingOn ----------
  static Result w4a() throws Exception {
    OGSchedulerContext fresh = ((OGScheduler) EvaluationContext.scheduler()).newEvaluationContext(ss());
    Object stack = field(OGSchedulerContext.class, "syncStack").get(fresh);
    Method push = stack.getClass().getDeclaredMethod("push", NodeTask.class);
    Method pop = stack.getClass().getDeclaredMethod("pop");
    push.setAccessible(true);
    pop.setAccessible(true);
    var a = new Completing("A", 0, ss());
    a.primeRunning(ss());
    var b = new Completing("B", 0, ss());
    var frame1 = (NodeAwaiter) push.invoke(stack, a);
    boolean registered = a.tryAddToWaiterList(frame1); // what runAndWait does
    pop.invoke(stack); // runAndWait exits (e.g. on isDoneEx) without A having completed
    var frame2 = (NodeAwaiter) push.invoke(stack, b);
    boolean same = frame1 == frame2;
    NodeTask stale = frame2.getWaitingOn();
    pop.invoke(stack);
    return new Result(
        "W4a",
        same && stale == a ? DEMONSTRATED : NOT_REPRODUCED,
        "registered=" + registered + " frame reused=" + same + " reused frame.waitingOn=" + stale + " (still points at A while the frame now represents the wait on B)");
  }

  // ---------- W4b: concurrent visit during pop ----------
  static Result w4b() throws Exception {
    OGSchedulerContext fresh = ((OGScheduler) EvaluationContext.scheduler()).newEvaluationContext(ss());
    Object stack = field(OGSchedulerContext.class, "syncStack").get(fresh);
    Method push = stack.getClass().getDeclaredMethod("push", NodeTask.class);
    Method pop = stack.getClass().getDeclaredMethod("pop");
    push.setAccessible(true);
    pop.setAccessible(true);
    var a = new Completing("A", 0, ss());
    var b = new Completing("B", 0, ss());
    push.invoke(stack, a);
    push.invoke(stack, b);
    List<String> seen = Collections.synchronizedList(new ArrayList<>());
    Throwable[] err = new Throwable[1];
    Steps.schedule("A1 B1 B2 A2")
        .thread(
            ctl -> {
              try {
                fresh.concurrentlyVisitNodesOnStack(
                    (frame, owner) -> {
                      NodeTask n = frame.getCurrentNodeTask();
                      seen.add(String.valueOf(n));
                      ctl.step();
                      return true;
                    });
              } catch (Throwable t) {
                err[0] = t;
              }
            })
        .thread(
            ctl -> {
              ctl.step();
              try { pop.invoke(stack); } catch (Exception e) { throw new RuntimeException(e); }
              ctl.step();
            })
        .run();
    pop.invoke(stack);
    return new Result(
        "W4b",
        err[0] != null ? DEMONSTRATED : NOT_REPRODUCED,
        "visitor saw " + seen + (err[0] != null ? "; threw " + err[0] : "; popped frame observed as null and tolerated (visitors must null-check)"));
  }

  // ---------- W5 helpers ----------
  static final class Listener implements OutOfWorkListener {
    final String name;
    final int prio;
    final Runnable onStalled;
    int stalled;

    Listener(String name, int prio, Runnable onStalled) {
      this.name = name;
      this.prio = prio;
      this.onStalled = onStalled;
    }

    @Override
    public int priority() {
      return prio;
    }

    @Override
    public void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> outstandingTasks) {
      stalled++;
      if (onStalled != null) onStalled.run();
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static void runStalledCallbacks(OGScheduler s) throws Exception {
    Method m = OGScheduler.class.getDeclaredMethod("runOnStalledCallbacks");
    m.setAccessible(true);
    Object lk = field(OGScheduler.class, "_lock").get(s);
    synchronized (lk) {
      try {
        m.invoke(s);
      } catch (java.lang.reflect.InvocationTargetException e) {
        throw new RuntimeException(e.getCause());
      }
    }
  }

  // ---------- W5b: re-register during onGraphStalled ----------
  static Result w5b() throws Exception {
    OGScheduler s = new OGScheduler(true);
    Listener[] self = new Listener[1];
    self[0] = new Listener("readd", 0, () -> s.addOutOfWorkListener(self[0]));
    s.addOutOfWorkListener(self[0]);
    runStalledCallbacks(s);
    int afterFirst = s.dbgOnStallCallBacksCount();
    int stalledFirst = self[0].stalled;
    runStalledCallbacks(s);
    boolean ok = afterFirst == 1 && stalledFirst == 1 && self[0].stalled == 2 && s.dbgOnStallCallBacksCount() == 1;
    s.removeOutOfWorkListener(self[0]);
    return new Result("W5b", ok ? NOT_REPRODUCED : DEMONSTRATED, "after pass 1: registered=" + afterFirst + " calls=" + stalledFirst + "; after pass 2: calls=" + self[0].stalled + " (pending-list re-add works, one call per pass)");
  }

  // ---------- W5c: priority gating ----------
  static Result w5c() throws Exception {
    OGScheduler s = new OGScheduler(true);
    ScenarioStack ss = ss();
    var l0a = new Listener("p0a", 0, () -> s.enqueue(new Completing("work", 1, ss)));
    var l0b = new Listener("p0b", 0, null);
    var l1 = new Listener("p1", 1, null);
    s.addOutOfWorkListener(l1);
    s.addOutOfWorkListener(l0a);
    s.addOutOfWorkListener(l0b);
    runStalledCallbacks(s);
    String pass1 = "pass1 calls p0a=" + l0a.stalled + " p0b=" + l0b.stalled + " p1=" + l1.stalled;
    boolean gated = l0a.stalled == 1 && l0b.stalled == 1 && l1.stalled == 0;
    runStalledCallbacks(s);
    String pass2 = "; pass2 p1=" + l1.stalled;
    boolean ok = gated && l1.stalled == 1;
    s.shutdown();
    return new Result("W5c", ok ? NOT_REPRODUCED : DEMONSTRATED, pass1 + pass2 + " (priority-1 listener deferred while priority-0 added work, then called)");
  }

  // ---------- W5d: registration cost ----------
  static Result w5d() throws Exception {
    OGScheduler s = new OGScheduler(true);
    int n = 20_000;
    var ls = new ArrayList<Listener>();
    for (int i = 0; i < n; i++) ls.add(new Listener("l" + i, i % 3, null));
    long t0 = System.nanoTime();
    for (var l : ls) s.addOutOfWorkListener(l);
    long add = System.nanoTime() - t0;
    t0 = System.nanoTime();
    for (var l : ls) s.removeOutOfWorkListener(l);
    long rem = System.nanoTime() - t0;
    return new Result(
        "W5d",
        add / n > 2_000 || rem / n > 2_000 ? DEMONSTRATED : NOT_REPRODUCED,
        String.format("%d listeners: add %.1f ms total (%.1f us each), remove %.1f ms total (%.1f us each)", n, add / 1e6, add / 1e3 / n, rem / 1e6, rem / 1e3 / n));
  }

  // ---------- W6 helpers ----------
  static OGScheduler.Context contextOf(OGSchedulerContext ec) throws Exception {
    return (OGScheduler.Context) field(OGSchedulerContext.class, "_context").get(ec);
  }

  @SuppressWarnings("unchecked")
  static IndexedArrayList<OGScheduler.Context> workers(OGScheduler s) throws Exception {
    return (IndexedArrayList<OGScheduler.Context>) field(OGScheduler.class, "_workers").get(s);
  }

  static int spinning(OGScheduler s) throws Exception {
    return field(OGScheduler.class, "_spinningThreadID").getInt(s);
  }

  static int ogThreads(String prefix) {
    int n = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) if (t.getName().contains("-" + prefix) && t.isAlive()) n++;
    return n;
  }

  // ---------- W6a: tryWakeupThread on a context that is not waiting ----------
  static Result w6a() throws Exception {
    OGScheduler s = new OGScheduler("w6a", 2, Thread.NORM_PRIORITY);
    OGSchedulerContext ec = s.newEvaluationContext(ss());
    ec.prfCtx = OGLocalTables.acquireProfilingContext(false);
    OGScheduler.Context ctx = contextOf(ec);
    Method wake = OGScheduler.class.getDeclaredMethod("tryWakeupThread", NodeTask.class, OGScheduler.Context.class, boolean.class);
    wake.setAccessible(true);
    Object lk = field(OGScheduler.class, "_lock").get(s);
    var task = new Completing("t", 0, ss());
    boolean removed;
    s.enterGraph(ctx, false); // now in _workers
    synchronized (lk) {
      removed = (boolean) wake.invoke(s, task, ctx, true);
    }
    NodeTask awaited = ctx.awaitedTask;
    s.exitGraph(ctx, false);
    s.shutdown();
    return new Result("W6a", !removed && awaited == null ? NOT_REPRODUCED : DEMONSTRATED, "tryWakeupThread on a working (non-waiting) context: returned " + removed + ", awaitedTask=" + awaited);
  }

  // ---------- W6b: does asIfRunning stamp causality with thread id 1? ----------
  static Result w6b() throws Exception {
    OGScheduler s = (OGScheduler) EvaluationContext.scheduler();
    var host = new Completing("host", 0, ss());
    var enqueued = new Completing("enq", 0, ss());
    Thread t =
        new Thread(
            () -> { // non-graph thread, as in EvaluationContext.asIfCalledFrom's else branch
              s.asIfRunning(host, () -> { OGSchedulerContext.current().enqueue(enqueued); return null; });
            });
    t.start();
    t.join();
    int tid = enqueued.causalityThreadID(), cid = enqueued.causalityID();
    long deadline = System.nanoTime() + 2_000_000_000L;
    while (!enqueued.isDone() && System.nanoTime() < deadline) Thread.onSpinWait();
    return new Result(
        "W6b",
        tid == 1 ? DEMONSTRATED : NOT_REPRODUCED,
        "task enqueued via asIfRunning: causalityThreadID=" + tid + " causalityID=" + cid + ", ran=" + enqueued.isDone()
            + (tid == 1 ? "" : " (asIfRunning never runs nodes, so its Context id 1 is never stamped: F4 is cosmetic)"));
  }

  // ---------- W6c: enqueue after shutdown still injects threads ----------
  static Result w6c() throws Exception {
    OGScheduler s = new OGScheduler("w6c", 2, Thread.NORM_PRIORITY);
    int before = ogThreads("w6c");
    s.shutdown();
    var task = new Holding(ss());
    s.enqueue(task);
    boolean started = task.started.await(2, TimeUnit.SECONDS);
    int during = ogThreads("w6c");
    task.release.countDown();
    long deadline = System.nanoTime() + 2_000_000_000L;
    while (!task.isDone() && System.nanoTime() < deadline) Thread.onSpinWait();
    Thread.sleep(100);
    int after = ogThreads("w6c");
    return new Result("W6c", during > before ? DEMONSTRATED : NOT_REPRODUCED, "og threads for this scheduler: before shutdown=" + before + ", while running a task enqueued after shutdown=" + during + " (started=" + started + "), after it finished=" + after + " (thread exits instead of parking because _schedulerShutdown)");
  }

  // ---------- W6d: spinner invariant across transitions ----------
  static Result w6d() throws Exception {
    OGScheduler s = new OGScheduler("w6d", 3, Thread.NORM_PRIORITY);
    Object lk = field(OGScheduler.class, "_lock").get(s);
    var workers = workers(s);
    List<String> violations = new ArrayList<>();
    Runnable check =
        () -> {
          synchronized (lk) {
            try {
              int sp = spinning(s);
              if (sp != 0) {
                boolean found = false;
                for (OGScheduler.Context c : workers.getUnderlyingList()) if (c != null && c.threadID == sp) found = true;
                if (!found) violations.add("spinner " + sp + " not in workers (count " + workers.count + ")");
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
    OGSchedulerContext e1 = s.newEvaluationContext(ss()), e2 = s.newEvaluationContext(ss());
    e1.prfCtx = OGLocalTables.acquireProfilingContext(false);
    e2.prfCtx = OGLocalTables.acquireProfilingContext(false);
    OGScheduler.Context c1 = contextOf(e1), c2 = contextOf(e2);
    for (int i = 0; i < 200; i++) {
      s.enterGraph(c1, false); check.run();
      s.enqueue(new Completing("w", 0, ss())); check.run();
      s.enterGraph(c2, false); check.run();
      s.exitGraph(c1, false); check.run();
      s.enqueue(new Completing("w", 0, ss())); check.run();
      s.exitGraph(c2, false); check.run();
      Thread.yield();
    }
    s.shutdown();
    return new Result("W6d", violations.isEmpty() ? NOT_REPRODUCED : DEMONSTRATED, violations.isEmpty() ? "1200 transition checkpoints: _spinningThreadID always names a context in _workers (or 0)" : violations.get(0) + " (" + violations.size() + " violations)");
  }

  // ---------- W6e: enqueue latency when it must start a thread, under _lock ----------
  static Result w6e() throws Exception {
    long[] inject = new long[20];
    for (int i = 0; i < inject.length; i++) {
      OGScheduler s = new OGScheduler("w6e" + i, 2, Thread.NORM_PRIORITY); // no threads yet
      var task = new Completing("t", 0, ss());
      long t0 = System.nanoTime();
      s.enqueue(task); // injectThread -> new OGThread().start() inside synchronized(_lock)
      inject[i] = System.nanoTime() - t0;
      long deadline = System.nanoTime() + 1_000_000_000L;
      while (!task.isDone() && System.nanoTime() < deadline) Thread.onSpinWait();
      s.shutdown();
    }
    OGScheduler s = new OGScheduler("w6e-warm", 2, Thread.NORM_PRIORITY);
    s.enqueue(new Completing("warm", 0, ss()));
    Thread.sleep(50); // worker now idle in the pool
    long[] reuse = new long[20];
    for (int i = 0; i < reuse.length; i++) {
      var task = new Completing("t", 0, ss());
      long t0 = System.nanoTime();
      s.enqueue(task);
      reuse[i] = System.nanoTime() - t0;
      long deadline = System.nanoTime() + 1_000_000_000L;
      while (!task.isDone() && System.nanoTime() < deadline) Thread.onSpinWait();
      Thread.sleep(5);
    }
    s.shutdown();
    java.util.Arrays.sort(inject);
    java.util.Arrays.sort(reuse);
    return new Result(
        "W6e",
        inject[10] > 5 * Math.max(1, reuse[10]) ? DEMONSTRATED : NOT_REPRODUCED,
        String.format("enqueue() wall time holding _lock: median %.1f us when it creates a platform thread vs %.1f us when it unparks an idle one (max %.1f / %.1f us)", inject[10] / 1e3, reuse[10] / 1e3, inject[19] / 1e3, reuse[19] / 1e3));
  }

  // ---------- W7: per-blocking-wait fixed cost ----------
  static Result w7() throws Exception {
    int n = 2000;
    ScenarioStack ss = ss();
    OGSchedulerContext ec = ec();
    long[] waits = new long[n];
    for (int i = 0; i < n; i++) {
      var node = new Blocking(ss);
      var latch = new CountDownLatch(1);
      Thread completer =
          new Thread(
              () -> {
                EvaluationContext.initializeWithoutRuntime();
                try { latch.await(); } catch (InterruptedException e) {}
                node.completeNow(ec());
              });
      completer.start();
      ec.enqueue(node);
      long t0 = System.nanoTime();
      latch.countDown();
      ec.runAndWait(node);
      waits[i] = System.nanoTime() - t0;
      completer.join();
    }
    // the two unconditional allocations in fullWait, measured on the same node type
    var sample = new Blocking(ss);
    long t0 = System.nanoTime();
    for (int i = 0; i < n; i++) { if (sample.toString().isEmpty()) throw new IllegalStateException(); }
    long toStr = (System.nanoTime() - t0) / n;
    t0 = System.nanoTime();
    Exception keep = null;
    for (int i = 0; i < n; i++) keep = new Exception("Blocking Wait");
    long exc = (System.nanoTime() - t0) / n;
    java.util.Arrays.sort(waits);
    return new Result(
        "W7",
        NOT_REPRODUCED,
        String.format("median cross-thread blocking wait %.1f us; unconditional fullWait allocations on this node: toString %.2f us + new Exception %.2f us (shallow stack; deeper stacks cost more)%s", waits[n / 2] / 1e3, toStr / 1e3, exc / 1e3, keep == null ? "" : ""));
  }
}
