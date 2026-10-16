package optimus.graph;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Follow-up to jcstress H2Waiters.MarkRunnableVsComplete: what a node left in the state that race
 * produces (DONE_FLAG set, NOT_RUNNABLE clear) does next. Single-threaded and deterministic: it
 * reaches that state by replaying the losing interleaving by hand (complete, then the CAS that
 * markAsRunnable performs after its stale check), then calls tryRun and complete again, as a worker
 * that dequeued the node would.
 */
public final class H2eConsequence {
  public static void main(String[] args) throws Exception {
    MC.ec();
    VarHandle state = MethodHandles.privateLookupIn(NodeTask.class, MethodHandles.lookup())
        .findVarHandle(NodeTask.class, "_state", int.class);
    var inLogAndDie = NodeTask.class.getDeclaredField("inLogAndDie");
    inLogAndDie.setAccessible(true);

    MC.Node n = new MC.Node().primeRunning();
    boolean checkPassed = !n.isDoneOrRunnable(); // markAsRunnable's check, before complete
    n.completeNow(MC.ec());
    int s = (int) state.get(n);
    state.set(n, s & ~MC.NOT_RUNNABLE); // markAsRunnable's updateState(0, NOT_RUNNABLE), after complete
    System.out.printf("markAsRunnable check passed before complete: %b%n", checkPassed);
    System.out.printf("after race: state=0x%x isDone()=%b isDoneEx()=%b isRunnable()=%b%n",
        n.state(), n.isDone(), n.isDoneEx(), n.isRunnable());
    int prev = n.tryRunPublic();
    System.out.printf("tryRun: prev NOT_RUNNABLE clear = %b (scheduler would run the node again)%n", (prev & MC.NOT_RUNNABLE) == 0);
    System.out.printf("after tryRun: state=0x%x isDone()=%b (tryRun re-set NOT_RUNNABLE, so the re-run's completion is detected)%n", n.state(), n.isDone());
    n.completeNow(MC.ec()); // what the re-run's run() would do
    System.out.printf("second complete(): logAndDie entered = %b; state=0x%x isDone()=%b%n",
        inLogAndDie.getBoolean(null), n.state(), n.isDone());

  }
}
