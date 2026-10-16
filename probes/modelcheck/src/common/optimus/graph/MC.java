package optimus.graph;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;

/**
 * Shared helpers for the H1/H2 model-checking tests (jcstress and Lincheck). Lives in optimus.graph
 * for package-private access to WorkQueue and NodeTask internals.
 */
public final class MC {
  private MC() {}

  /** True if the entity agent's premain ran in this VM (so NodeTask is the agent-rewritten class). */
  public static final boolean AGENT_LOADED = agentLoaded();

  static {
    if (!AGENT_LOADED && !Boolean.getBoolean("mc.allowNoAgent"))
      throw new IllegalStateException("entity agent not loaded in this VM (pass -javaagent, or -Dmc.allowNoAgent=true)");
  }

  private static boolean agentLoaded() {
    try {
      var f = Class.forName("optimus.EntityAgent").getDeclaredField("instrumentation");
      f.setAccessible(true);
      return f.get(null) != null;
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }

  // ---------- graph initialisation ----------
  private static volatile ScenarioStack SS;

  /** Initialise the graph on this thread if needed; returns this thread's OGSchedulerContext. */
  public static OGSchedulerContext ec() {
    OGSchedulerContext c = OGSchedulerContext.current();
    if (c == null) {
      EvaluationContext.initializeWithoutRuntime();
      c = OGSchedulerContext.current();
    }
    return c;
  }

  /** A scenario stack captured once per VM (immutable, shared by all test nodes). */
  public static ScenarioStack ss() {
    ScenarioStack s = SS;
    if (s == null) {
      synchronized (MC.class) {
        if (SS == null) {
          ec();
          SS = EvaluationContext.scenarioStack();
        }
        s = SS;
      }
    }
    return s;
  }

  // ---------- WorkQueue access ----------
  static final VarHandle TOP, BASE, Q;

  static {
    try {
      var l = MethodHandles.privateLookupIn(OGSchedulerContext.WorkQueue.class, MethodHandles.lookup());
      TOP = l.findVarHandle(OGSchedulerContext.WorkQueue.class, "_top", int.class);
      BASE = l.findVarHandle(OGSchedulerContext.WorkQueue.class, "_base", int.class);
      Q = l.findVarHandle(OGSchedulerContext.WorkQueue.class, "_q", NodeTask[].class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static OGSchedulerContext.WorkQueue newQueue(int top, int base) {
    var q = new OGSchedulerContext.WorkQueue();
    TOP.set(q, top);
    BASE.setVolatile(q, base);
    return q;
  }

  /** As newQueue, but replace the 64-slot array with one of the given power-of-two capacity. */
  public static OGSchedulerContext.WorkQueue newQueue(int top, int base, int capacity) {
    var q = newQueue(top, base);
    Q.set(q, new NodeTask[capacity]);
    return q;
  }

  public static int top(OGSchedulerContext.WorkQueue q) { return (int) TOP.get(q); }
  public static int base(OGSchedulerContext.WorkQueue q) { return (int) BASE.getVolatile(q); }
  public static NodeTask[] array(OGSchedulerContext.WorkQueue q) { return (NodeTask[]) Q.get(q); }

  /** Number of occurrences of t in the live region [base, top) of q's current array. */
  public static int countLive(OGSchedulerContext.WorkQueue q, NodeTask t) {
    NodeTask[] a = array(q);
    int m = a.length - 1, n = 0;
    for (int i = base(q); i < top(q); i++) if (a[i & m] == t) n++;
    return n;
  }

  /** Number of null slots in the live region [base, top). */
  public static int nullsLive(OGSchedulerContext.WorkQueue q) {
    NodeTask[] a = array(q);
    int m = a.length - 1, n = 0;
    for (int i = base(q); i < top(q); i++) if (a[i & m] == null) n++;
    return n;
  }

  // ---------- tasks ----------
  /** Minimal live queue task (state NEW, never run). */
  public static final class T extends NodeTask {
    static { ss(); } // NodeTask needs an initialised graph runtime
    public final int n;
    public T(int n) { this.n = n; }
    @Override public String toString() { return "T" + n; }
  }

  /** Node attached to the shared scenario stack, primable as running, completable on demand. */
  public static final class Node extends NodeTask {
    static { ss(); }
    public Node() { attach(ss()); }
    @Override public void run(OGSchedulerContext ec) { complete(ec); }
    public Node primeRunning() { initAsRunning(ss()); return this; }
    public void completeNow(EvaluationQueue eq) { complete(eq); }
    public int state() { return getState(); }
    public int tryRunPublic() { return tryRun(); }
    public void markAsRunnablePublic() { markAsRunnable(); }
    public boolean tryMarkAsRunnablePublic() { return tryMarkAsRunnable(); }
    @Override public String toString() { return "MCNode"; }
  }

  /** Awaiter recording calls: by notifyWaiters (onChildCompleted) and by its own registering thread. */
  public static final class Awaiter extends NodeAwaiter {
    public final char tag;
    public final Order order;
    public final AtomicInteger notified = new AtomicInteger();
    public final AtomicInteger selfCalled = new AtomicInteger();
    public Awaiter(char tag, Order order) { this.tag = tag; this.order = order; }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask node) {
      notified.incrementAndGet();
      if (order != null) order.add(Character.toUpperCase(tag));
    }

    /** The continueWith contract: caller invokes the callback itself when registration fails. */
    public void selfCall() {
      selfCalled.incrementAndGet();
      if (order != null) order.add(Character.toLowerCase(tag));
    }

    @Override public String toString() { return "Awaiter(" + tag + ")"; }
  }

  /** Lock-free append-only record of callback order. */
  public static final class Order {
    private final char[] buf = new char[16];
    private final AtomicInteger n = new AtomicInteger();
    public void add(char c) { buf[n.getAndIncrement()] = c; }
    public String get() { return new String(buf, 0, n.get()); }
  }

  private static final java.lang.invoke.MethodHandle ADD_LAST;

  static {
    try {
      var l = MethodHandles.privateLookupIn(NodeTask.class, MethodHandles.lookup());
      ADD_LAST = l.findVirtual(NodeTask.class, "tryAddToWaiterListLast",
          java.lang.invoke.MethodType.methodType(boolean.class, NodeAwaiter.class));
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** The private NodeTask.tryAddToWaiterListLast (what continueWithLast calls). */
  public static boolean tryAddToWaiterListLast(NodeTask n, NodeAwaiter w) {
    try {
      return (boolean) ADD_LAST.invokeExact(n, w);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public static final int NOT_RUNNABLE = NodeTask.STATE_NOT_RUNNABLE;
  public static final int DONE_FLAG = NodeTask.STATE_DONE_FLAG;
}
