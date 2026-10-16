package optimus.graph;

import static org.openjdk.jcstress.annotations.Expect.*;

import optimus.graph.MC.Awaiter;
import optimus.graph.MC.Node;
import optimus.graph.MC.Order;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * H2: jcstress tests for the NodeTask _waiters / _state protocol. Nodes are attached to a shared
 * scenario stack and primed running (initAsRunning), as Probes2.Completing does; each actor thread
 * initialises the graph on first use (MC.ec()) and completes with its own OGSchedulerContext.
 *
 * <p>complete() vs complete() is deliberately absent: the losing call goes to logAndDie, which
 * either exits the forked VM (121) or, with InfoDumper failing to initialise here, sets the static
 * inLogAndDie guard for the rest of the VM (F1b). Either way jcstress cannot use the fork after the
 * first race, so that case stays a child-JVM probe (W3c, T7).
 */
public final class H2Waiters {
  private H2Waiters() {}

  /**
   * (a) tryAddToWaiterList(w) vs complete (notifyWaiters). The registering actor follows the
   * continueWith contract: if registration returns false it calls w itself. r1 = returned true, r2 =
   * calls from notifyWaiters, r3 = calls by the registering thread.
   */
  @JCStressTest
  @Outcome(id = "1, 1, 0", expect = ACCEPTABLE, desc = "registered, notified once")
  @Outcome(id = "0, 0, 1", expect = ACCEPTABLE, desc = "too late, caller called it itself")
  @Outcome(expect = FORBIDDEN, desc = "called twice, never, or registered but not notified")
  @State
  public static class AddVsComplete {
    final Node n = new Node().primeRunning();
    final Awaiter w = new Awaiter('a', null);

    @Actor public void add(III_Result r) {
      boolean ok = n.tryAddToWaiterList(w);
      r.r1 = ok ? 1 : 0;
      if (!ok) w.selfCall();
    }
    @Actor public void complete() { n.completeNow(MC.ec()); }
    @Arbiter public void after(III_Result r) { r.r2 = w.notified.get(); r.r3 = w.selfCalled.get(); }
  }

  /** (b) Two continueWith vs complete: each waiter called exactly once. r1, r2 = total calls. */
  @JCStressTest
  @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "each waiter called once")
  @Outcome(expect = FORBIDDEN, desc = "a waiter called != 1 times")
  @State
  public static class TwoContinueWithVsComplete {
    final Node n = new Node().primeRunning();
    final Awaiter w1 = new Awaiter('a', null), w2 = new Awaiter('b', null);

    @Actor public void add1() { n.continueWith(w1, MC.ec()); }
    @Actor public void add2() { n.continueWith(w2, MC.ec()); }
    @Actor public void complete() { n.completeNow(MC.ec()); }
    @Arbiter public void after(II_Result r) {
      r.r1 = w1.notified.get() + w1.selfCalled.get();
      r.r2 = w2.notified.get() + w2.selfCalled.get();
    }
  }

  /**
   * (c) tryAddToWaiterListLast(L) vs tryAddToWaiterList(A) vs complete, with waiter 0 registered
   * before the race. Result: "OK:" or "BAD:" followed by the callback order; upper case = called by
   * notifyWaiters, lower case = called by the registering thread after it saw `completed`. BAD if
   * any waiter is not called exactly once, if 0 is ever self-called, or if a notified waiter comes
   * after the notified L (L must be after every waiter on the list).
   */
  @JCStressTest
  @Outcome(id = "OK:.*", expect = ACCEPTABLE, desc = "Last waiter after every listed waiter")
  @Outcome(id = "BAD:.*", expect = FORBIDDEN, desc = "ordering or exactly-once violated")
  @State
  public static class LastOrdering {
    final Order order = new Order();
    final Node n = new Node().primeRunning();
    final Awaiter w0 = new Awaiter('0', order), wa = new Awaiter('a', order), wl = new Awaiter('l', order);
    { n.tryAddToWaiterList(w0); }

    @Actor public void addLast() { if (!MC.tryAddToWaiterListLast(n, wl)) wl.selfCall(); }
    @Actor public void add() { if (!n.tryAddToWaiterList(wa)) wa.selfCall(); }
    @Actor public void complete() { n.completeNow(MC.ec()); }
    @Arbiter public void after(L_Result r) {
      String s = order.get();
      boolean ok = s.length() == 3
          && w0.notified.get() == 1 && w0.selfCalled.get() == 0
          && wa.notified.get() + wa.selfCalled.get() == 1
          && wl.notified.get() + wl.selfCalled.get() == 1;
      int iL = s.indexOf('L');
      if (iL >= 0) for (int i = iL + 1; i < s.length(); i++) if (Character.isUpperCase(s.charAt(i)) || s.charAt(i) == '0') ok = false;
      r.r1 = (ok ? "OK:" : "BAD:") + s;
    }
  }

  /** (d) tryRun vs tryRun on a fresh (runnable) node: exactly one sees NOT_RUNNABLE clear. */
  @JCStressTest
  @Outcome(id = {"1, 0", "0, 1"}, expect = ACCEPTABLE, desc = "exactly one winner")
  @Outcome(expect = FORBIDDEN, desc = "zero or two winners")
  @State
  public static class TryRunVsTryRun {
    final Node n = new Node();

    @Actor public void a(II_Result r) { r.r1 = (n.tryRunPublic() & MC.NOT_RUNNABLE) == 0 ? 1 : 0; }
    @Actor public void b(II_Result r) { r.r2 = (n.tryRunPublic() & MC.NOT_RUNNABLE) == 0 ? 1 : 0; }
  }

  /**
   * (e) markAsRunnable vs complete on a running node. r1 = DONE_FLAG set, r2 = NOT_RUNNABLE set, r3
   * = isDone(), r4 = a later tryRun wins (sees NOT_RUNNABLE clear, so the scheduler would run the
   * node again). markAsRunnable is check-then-act: `if (!isDoneOrRunnable()) updateState(0,
   * NOT_RUNNABLE)` with an unconditional CAS loop, so a complete() between the check and the CAS is
   * predicted to leave DONE_FLAG set with NOT_RUNNABLE cleared.
   */
  @JCStressTest
  @Outcome(id = "1, 1, 1, 0", expect = ACCEPTABLE, desc = "done, not runnable")
  @Outcome(id = "1, 0, 0, 1", expect = FORBIDDEN, desc = "DONE_FLAG set but runnable: isDone() false, tryRun wins on a completed node")
  @Outcome(expect = FORBIDDEN, desc = "other")
  @State
  public static class MarkRunnableVsComplete {
    final Node n = new Node().primeRunning();

    @Actor public void mark() { n.markAsRunnablePublic(); }
    @Actor public void complete() { n.completeNow(MC.ec()); }
    @Arbiter public void after(IIII_Result r) { stateResult(n, r); }
  }

  static void stateResult(Node n, IIII_Result r) {
    int s = n.state();
    r.r1 = (s & MC.DONE_FLAG) != 0 ? 1 : 0;
    r.r2 = (s & MC.NOT_RUNNABLE) != 0 ? 1 : 0;
    r.r3 = n.isDone() ? 1 : 0;
    r.r4 = (n.tryRunPublic() & MC.NOT_RUNNABLE) == 0 ? 1 : 0;
  }

  /** (e') Control for (e): tryMarkAsRunnable (tryUpdateState, re-checks DONE inside the CAS loop). */
  @JCStressTest
  @Outcome(id = "1, 1, 1, 0", expect = ACCEPTABLE, desc = "done, not runnable")
  @Outcome(expect = FORBIDDEN, desc = "other")
  @State
  public static class TryMarkRunnableVsComplete {
    final Node n = new Node().primeRunning();

    @Actor public void mark() { n.tryMarkAsRunnablePublic(); }
    @Actor public void complete() { n.completeNow(MC.ec()); }
    @Arbiter public void after(IIII_Result r) { stateResult(n, r); }
  }
}
