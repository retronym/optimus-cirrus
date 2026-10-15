package optimus.graph;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicInteger;
import optimus.core.IndexedArrayList;
import optimus.platform.EvaluationContext;
import probes.Steps;

/**
 * Tier 0 whitebox probes (see TEST_CAMPAIGN.md). Lives in optimus.graph for package-private access.
 * Each probe returns a Result; DEMONSTRATED means the hazard reproduced as predicted.
 */
public final class Probes {
  enum Outcome {
    DEMONSTRATED,
    NOT_REPRODUCED,
    ERROR
  }

  record Result(String id, Outcome outcome, String detail) {}

  /** Minimal live task: state NEW, never done. */
  static final class T extends NodeTask {
    final int n;

    T(int n) {
      this.n = n;
    }

    @Override
    public String toString() {
      return "T" + n;
    }
  }

  /** A task primed as completed. */
  static final class DoneT extends NodeTask {
    DoneT() {
      initAsCompleted();
    }

    @Override
    public String toString() {
      return "DoneT";
    }
  }

  // ---------- reflection helpers ----------
  static Field field(Class<?> c, String name) throws Exception {
    Field f = c.getDeclaredField(name);
    f.setAccessible(true);
    return f;
  }

  static Class<?> wq() throws Exception {
    return Class.forName("optimus.graph.OGSchedulerContext$WorkQueue");
  }

  static OGSchedulerContext.WorkQueue newQueue(int top, int base) throws Exception {
    OGSchedulerContext.WorkQueue q = new OGSchedulerContext.WorkQueue();
    field(wq(), "_top").setInt(q, top);
    field(wq(), "_base").setInt(q, base);
    return q;
  }

  static int top(OGSchedulerContext.WorkQueue q) throws Exception {
    return field(wq(), "_top").getInt(q);
  }

  static int base(OGSchedulerContext.WorkQueue q) throws Exception {
    return field(wq(), "_base").getInt(q);
  }

  static String state(OGSchedulerContext.WorkQueue q) throws Exception {
    return "top=" + top(q) + " base=" + base(q) + " size()=" + q.size() + " isEmpty()=" + q.isEmpty();
  }

  // ---------- W1a: wrap at Integer.MAX_VALUE ----------
  static Result w1a() throws Exception {
    final int MAX = Integer.MAX_VALUE;
    var q = newQueue(MAX - 3, MAX - 3);
    T t1 = new T(1), t2 = new T(2), t3 = new T(3), t4 = new T(4);
    q.localPush(t1);
    q.localPush(t2);
    q.localPush(t3); // top = MAX
    String afterReset = state(q);
    NodeTask stolen = q.tryToSteal();
    q.localPush(t4);
    List<NodeTask> popped = new ArrayList<>();
    NodeTask p;
    while ((p = q.localPop(0)) != null) popped.add(p);
    Set<NodeTask> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    if (stolen != null) seen.add(stolen);
    seen.addAll(popped);
    boolean lost = !(seen.contains(t1) && seen.contains(t2) && seen.contains(t3) && seen.contains(t4));
    String detail =
        "after 3rd push (top hits MAX): " + afterReset + "; stolen=" + stolen + " popped=" + popped
            + "; final " + state(q);
    return new Result("W1a", lost || !q.isEmpty() ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, detail);
  }

  // ---------- W1a': can _base overrun MAX? (R2 case 1) ----------
  static Result w1aPrime() throws Exception {
    // Try to construct the "_base = top + 1" state the class comment describes, using only the
    // public operations: owner pops while thieves steal a single element. If base never exceeds
    // top, R2 case 1 is unreachable in the current code.
    int worst = 0;
    for (int iter = 0; iter < 20_000; iter++) {
      var q = newQueue(1000, 1000);
      T t = new T(iter);
      q.localPush(t);
      NodeTask[] res = new NodeTask[2];
      Thread th =
          new Thread(
              () -> {
                res[1] = q.tryToSteal();
              });
      th.start();
      res[0] = q.localPop(0);
      th.join();
      int overrun = base(q) - top(q);
      if (overrun > worst) worst = overrun;
      if (res[0] == null && res[1] == null) return new Result("W1a'", Outcome.DEMONSTRATED, "task lost at iter " + iter);
    }
    return new Result(
        "W1a'",
        worst > 0 ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED,
        "max (base - top) overrun observed over 20k pop/steal races: " + worst
            + (worst > 0 ? " (so base can pass MAX_VALUE)" : " (base overrun not reachable via pop/steal race in current code)"));
  }

  // ---------- W1b: sync-frame marker captured before reset ----------
  static Result w1b() throws Exception {
    final int MAX = Integer.MAX_VALUE;
    var q = newQueue(MAX - 6, MAX - 6);
    for (int i = 0; i < 5; i++) q.localPush(new T(i));
    int marker = q.getTop(); // what runAndWait stores in currentLocalQueueMarker
    q.localPush(new T(5)); // top -> MAX, resetBounds fires
    String afterReset = state(q);
    List<NodeTask> pushedAfter = new ArrayList<>();
    for (int i = 6; i < 11; i++) {
      T t = new T(i);
      pushedAfter.add(t);
      q.localPush(t);
    }
    NodeTask got = q.localPop(marker);
    String detail =
        "marker=" + marker + "; " + afterReset + "; 5 tasks pushed after reset; localPop(marker) -> " + got
            + " (expected " + pushedAfter.get(4) + ")";
    return new Result("W1b", got == null ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, detail);
  }

  // ---------- W1f: localCleanup(null) with an incomplete task ----------
  static Result w1f() throws Exception {
    var q = newQueue(0, 0);
    q.localPush(new DoneT());
    q.localPush(new DoneT());
    T live = new T(99);
    q.localPush(live);
    q.localPush(new DoneT());
    q.localCleanup(null);
    List<NodeTask> remaining = new ArrayList<>();
    q.foreach(remaining::add);
    boolean donesRetained = remaining.stream().anyMatch(t -> t instanceof DoneT);
    return new Result(
        "W1f",
        donesRetained ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED,
        "after localCleanup(null): " + remaining + " (done tasks below the live one are retained)");
  }

  // ---------- W1s: owner push/pop vs thieves across the MAX wrap, no task lost ----------
  static Result w1s() throws Exception {
    final int MAX = Integer.MAX_VALUE;
    int rounds = 200;
    for (int r = 0; r < rounds; r++) {
      var q = newQueue(MAX - 50, MAX - 50);
      int n = 2000;
      AtomicInteger seen = new AtomicInteger();
      boolean[] observed = new boolean[n];
      Object lock = new Object();
      var thieves = new ArrayList<Thread>();
      var stop = new java.util.concurrent.atomic.AtomicBoolean();
      for (int k = 0; k < 3; k++) {
        Thread th =
            new Thread(
                () -> {
                  while (!stop.get()) {
                    NodeTask t = q.tryToSteal();
                    if (t != null) synchronized (lock) { observed[((T) t).n] = true; }
                    else Thread.onSpinWait();
                  }
                });
        th.start();
        thieves.add(th);
      }
      for (int i = 0; i < n; i++) {
        q.localPush(new T(i));
        if ((i & 3) == 0) {
          NodeTask t = q.localPop(0);
          if (t != null) synchronized (lock) { observed[((T) t).n] = true; }
        }
      }
      NodeTask t;
      while ((t = q.localPop(0)) != null) synchronized (lock) { observed[((T) t).n] = true; }
      // let thieves drain
      long deadline = System.nanoTime() + 200_000_000L;
      while (!q.isEmpty() && System.nanoTime() < deadline) Thread.onSpinWait();
      stop.set(true);
      for (Thread th : thieves) th.join();
      while ((t = q.tryToSteal()) != null) synchronized (lock) { observed[((T) t).n] = true; }
      int missing = 0;
      for (boolean b : observed) if (!b) missing++;
      if (missing > 0 || !q.isEmpty())
        return new Result(
            "W1s",
            Outcome.DEMONSTRATED,
            "round " + r + ": " + missing + " tasks never observed; final " + state(q));
    }
    return new Result("W1s", Outcome.NOT_REPRODUCED, rounds + " rounds of 2000 pushes across the MAX wrap with 3 thieves: nothing lost");
  }

  // ---------- W3d: causality packing ----------
  static Result w3d() {
    StringBuilder sb = new StringBuilder();
    boolean bad = false;
    int[] ids = {1, 32767, 32768, 65535, 65536, 65537};
    for (int id : ids) {
      T t = new T(0);
      t.setCausality(1, id);
      int back = t.causalityThreadID();
      boolean ok = back == id;
      bad |= !ok;
      sb.append("tid ").append(id).append(" -> ").append(back).append(ok ? " ok" : " MISMATCH").append("; ");
    }
    // aliasing: a task stamped by thread 65537 under sync-stack causality 1; can thread 1 take it?
    T aliased = new T(0);
    aliased.setCausality(1, 65537);
    boolean thread1CanTake = OGScheduler.canTake(aliased, 1, 1);
    T own = new T(0);
    own.setCausality(1, 32768);
    boolean ownerCanTake = OGScheduler.canTake(own, 32768, 1);
    T poisoned = new T(0);
    poisoned.poisonCausality();
    sb.append("canTake(stamped by 65537, thread 1, causality 1) = ").append(thread1CanTake)
        .append(" (should be false); canTake(stamped by 32768, thread 32768, causality 1) = ").append(ownerCanTake)
        .append(" (should be true); poisoned tid = ").append(poisoned.causalityThreadID());
    bad |= thread1CanTake || !ownerCanTake;
    return new Result("W3d", bad ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, sb.toString());
  }

  // ---------- W2a: IndexedArrayList foreach vs tryRemove ----------
  static final class Item extends IndexedArrayList.IndexedItem {
    final String name;

    Item(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static Result w2a() {
    var list =
        new IndexedArrayList<Item>() {
          @Override
          protected Item[] createInitialArray() {
            return new Item[4];
          }
        };
    Item a = new Item("a"), b = new Item("b"), c = new Item("c");
    list.add(a);
    list.add(b);
    list.add(c);
    List<Item> visited = Collections.synchronizedList(new ArrayList<>());
    // A iterates, stepping once per item; B removes `a` (the item being visited) after A's first step.
    Steps.schedule("A1 B1 B2 A2")
        .thread(
            ctl ->
                list.foreach(
                    it -> {
                      visited.add(it);
                      ctl.step();
                    }))
        .thread(
            ctl -> {
              ctl.step();
              list.tryRemove(a);
              ctl.step();
            })
        .run();
    boolean skippedLive = !visited.contains(c);
    return new Result(
        "W2a",
        skippedLive ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED,
        "visited=" + visited + " after removing 'a' mid-iteration; live item 'c' " + (skippedLive ? "was skipped" : "was visited"));
  }

  // ---------- W5a: listener removing itself in checkForNewWork ----------
  static Result w5a() throws Exception {
    OGScheduler s = new OGScheduler(true);
    var l1 = new SelfRemoving();
    var l2 = new SelfRemoving();
    var l3 = new SelfRemoving();
    s.addOutOfWorkListener(l1);
    s.addOutOfWorkListener(l2);
    s.addOutOfWorkListener(l3);
    Method m = OGScheduler.class.getDeclaredMethod("runOnStalledCallbacks");
    m.setAccessible(true);
    Field lock = field(OGScheduler.class, "_lock");
    Object lk = lock.get(s);
    try {
      synchronized (lk) {
        m.invoke(s);
      }
      return new Result("W5a", Outcome.NOT_REPRODUCED, "runOnStalledCallbacks completed; onGraphStalled calls=" + (l1.stalled + l2.stalled + l3.stalled));
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable t = e.getCause();
      boolean cme = t instanceof ConcurrentModificationException;
      return new Result("W5a", cme ? Outcome.DEMONSTRATED : Outcome.ERROR, t.toString());
    }
  }

  static final class SelfRemoving implements OutOfWorkListener {
    int stalled;

    @Override
    public int checkForNewWork(Scheduler scheduler) {
      scheduler.removeOutOfWorkListener(this);
      return 0;
    }

    @Override
    public void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> outstandingTasks) {
      stalled++;
    }
  }

  // ---------- runner ----------
  public static void main(String[] args) throws Exception {
    if (args.length >= 2 && args[0].equals("--child")) {
      if (args[1].equals("w3c")) Probes2.w3cChild(args[2]);
      return;
    }
    EvaluationContext.initializeWithoutRuntime();
    List<Result> results = new ArrayList<>();
    onlyIds = args.length == 0 ? null : java.util.Set.of(args);
    results.add(run("W1a", Probes::w1a));
    results.add(run("W1a'", Probes::w1aPrime));
    results.add(run("W1b", Probes::w1b));
    results.add(run("W1f", Probes::w1f));
    results.add(run("W1s", Probes::w1s));
    results.add(run("W3d", Probes::w3d));
    results.add(run("W2a", Probes::w2a));
    results.add(run("W5a", Probes::w5a));
    results.add(run("W1g", Probes2::w1g));
    results.add(run("W2b", Probes2::w2b));
    results.add(run("W2c", Probes2::w2c));
    results.add(run("W3a", Probes2::w3a));
    results.add(run("W3b", Probes2::w3b));
    results.add(run("W3c", Probes2::w3c));
    results.add(run("W3e", Probes2::w3e));
    results.add(run("W3f", Probes2::w3f));
    results.add(run("W3g", Probes2::w3g));
    results.add(run("W4a", Probes2::w4a));
    results.add(run("W4b", Probes2::w4b));
    results.add(run("W5b", Probes2::w5b));
    results.add(run("W5c", Probes2::w5c));
    results.add(run("W5d", Probes2::w5d));
    results.add(run("W6a", Probes2::w6a));
    results.add(run("W6b", Probes2::w6b));
    results.add(run("W6c", Probes2::w6c));
    results.add(run("W6d", Probes2::w6d));
    results.add(run("W6e", Probes2::w6e));
    results.add(run("W7", Probes2::w7));
    results.removeIf(r -> r == null);
    System.out.println();
    System.out.println("=== Tier 0 results ===");
    for (Result r : results) System.out.printf("%-5s %-15s %s%n", r.id(), r.outcome(), r.detail());
    System.exit(0);
  }

  interface Probe {
    Result run() throws Exception;
  }

  static java.util.Set<String> onlyIds;

  static Result run(String id, Probe p) {
    if (onlyIds != null && !onlyIds.contains(id)) return null;
    try {
      Result r = p.run();
      System.out.println(r.id() + ": " + r.outcome());
      return r;
    } catch (Throwable t) {
      t.printStackTrace();
      return new Result(id, Outcome.ERROR, t.toString());
    }
  }
}
