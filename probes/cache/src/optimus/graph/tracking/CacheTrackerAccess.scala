package optimus.graph.tracking

import java.lang.reflect.Field
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import optimus.graph.CancellationScope
import optimus.graph.Node
import optimus.graph.NodeKey
import optimus.graph.TweakableKey
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.platform.ScenarioStack
import optimus.platform.Tweak

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Tracker driver for the cache campaign: everything private[tracking]/protected[tracking] the probes need, plus
 * reflection for private fields. Lives in optimus.graph.tracking so Scala access modifiers allow it.
 */
object CacheTrackerAccess {
  // ---- construction / teardown -------------------------------------------------------------------------------------
  /** DependencyTrackerRoot() under EvaluationContext.scenarioStack (call CacheFixtures.init() first). */
  def newRoot(): DependencyTrackerRoot = DependencyTrackerRoot()
  /** Root with a controllable TimedScheduler (cleanup timing). */
  def newRoot(scheduler: TimedScheduler): DependencyTrackerRoot = DependencyTrackerRoot(scheduler)
  /** Consistent child: shares the parent's queue. */
  def consistentChild(t: DependencyTracker, name: String): DependencyTracker = t.newChildScenario(name)
  /** Concurrent child: own queue (introduceConcurrentSubtree = true). */
  def concurrentChild(t: DependencyTracker, name: String): DependencyTracker =
    t.newChildScenario(name, introduceConcurrentSubtree = true, rejectTweaks = false)
  def disposeAllRoots(): Unit = DependencyTrackerRoot.TEST_ONLY_disposeAllRoots()
  def dispose(t: DependencyTracker): Unit = t.dispose()
  def setAutoCleanup(enabled: Boolean): Unit = CleanupSchedulerHelper.setAutoCleanupEnabled(enabled)

  def cause(name: String = "probe"): EventCause = TestEventCause(name)

  // ---- updates -----------------------------------------------------------------------------------------------------
  def addTweaks(t: DependencyTracker, tweaks: Tweak*): Unit = t.addTweaks(tweaks)
  def addTweaksAsync(t: DependencyTracker, tweaks: Seq[Tweak]): CompletableFuture[Unit] = {
    val f = new CompletableFuture[Unit]
    t.addTweaksAsync(tweaks, throwOnDuplicate = false, complete(f))
    f
  }
  def removeTweak(t: DependencyTracker, key: NodeKey[_]): Unit = t.removeTweak(key)
  def removeAllTweaks(t: DependencyTracker): Unit = t.removeAllTweaks()
  def batch[R](t: DependencyTracker, c: EventCause = cause())(f: DependencyTrackerBatchUpdater => R): R =
    t.executeBatchUpdate(c, f)
  def batchAsync[R](t: DependencyTracker, c: EventCause = cause())(
      f: DependencyTrackerBatchUpdater => R): CompletableFuture[R] = {
    val fut = new CompletableFuture[R]
    t.executeBatchUpdateAsync(c, f, complete(fut))
    fut
  }

  // ---- evaluations -------------------------------------------------------------------------------------------------
  def evaluate[T](t: DependencyTracker, key: NodeKey[T], scope: TrackingScope[_] = NullScope): T =
    t.evaluateNodeKey(key, cause(), scope)
  def evaluateKeys(t: DependencyTracker, keys: Seq[NodeKey[_]], scope: TrackingScope[_] = NullScope): Unit =
    t.evaluateNodeKeys(keys, cause(), scope)
  def evaluateKeysAsync(
      t: DependencyTracker,
      keys: Seq[NodeKey[_]],
      scope: TrackingScope[_] = NullScope): CompletableFuture[(Seq[Node[_]], Seq[NodeKey[_]])] = {
    val f = new CompletableFuture[(Seq[Node[_]], Seq[NodeKey[_]])]
    t.evaluateNodeKeysAsync(keys, complete(f), cause(), scope)
    f
  }
  def evaluateKeysLowPriorityAsync(
      t: DependencyTracker,
      keys: Seq[NodeKey[_]],
      scope: TrackingScope[_] = NullScope): CompletableFuture[(Seq[Node[_]], Seq[NodeKey[_]])] = {
    val f = new CompletableFuture[(Seq[Node[_]], Seq[NodeKey[_]])]
    t.evaluateNodeKeysLowPriorityAsync(keys, complete(f), cause(), scope)
    f
  }
  def executeEvaluate[T](t: DependencyTracker)(f: => T): T = t.executeEvaluate(() => f)
  def executeEvaluateAsync[T](t: DependencyTracker, lowPriority: Boolean = false)(f: => T): CompletableFuture[T] = {
    val fut = new CompletableFuture[T]
    t.executeEvaluateAsync(() => f, complete(fut), lowPriority)
    fut
  }

  /** The tracker's stack with the queue's current cancellation scope (what evaluate actions use). */
  def evaluationStack(t: DependencyTracker): ScenarioStack =
    t.scenarioStackWithCancelScope(t.queue.currentCancellationScope)
  /** The raw tracker stack (evaluating directly under it bypasses the queue: T4(a)). */
  def rawStack(t: DependencyTracker): ScenarioStack = t.scenarioStack

  // ---- callbacks / tracked user nodes ------------------------------------------------------------------------------
  final class ProbeScope(name: String) extends SimpleTrackingScope[TrackingMemo](name) {
    override def defaultMemo(node: NodeKey[_], ts: DependencyTracker): TrackingMemo = EmptyMemo(name)
  }
  def newScope(name: String): ProbeScope = new ProbeScope(name)
  def track[T](t: DependencyTracker, key: NodeKey[T], scope: TrackingScope[TrackingMemo]): TrackedNode[T] =
    t.track(key, scope)
  /** cb(invalidated user nodes, cause) after each update that invalidates tracked nodes of `scope`. */
  def registerNodesInvalidated(t: DependencyTracker, scope: TrackingScope[TrackingMemo])(
      cb: (List[(TrackedNode[_], TrackingMemo)], EventCause) => Unit): Unit =
    t.root.registerNodesInvalidated(scope, cb)

  // ---- internals ---------------------------------------------------------------------------------------------------
  def queue(t: DependencyTracker): DependencyTrackerQueue = t.queue
  def currentCancellationScope(t: DependencyTracker): CancellationScope = t.queue.currentCancellationScope
  /** _currentCancellationScope (private[this]; null outside evaluation/barrier windows). */
  def currentCancellationScopeRaw(t: DependencyTracker): CancellationScope =
    field(t.queue, "_currentCancellationScope").asInstanceOf[CancellationScope]
  def workQueue(t: DependencyTracker): AnyRef = field(t.queue, "workQueue")
  def lowPriorityQueue(t: DependencyTracker): AnyRef = field(t.queue, "lowPriorityQueue")
  def workQueueSize(t: DependencyTracker): Int = sizeOf(workQueue(t))
  def lowPriorityQueueSize(t: DependencyTracker): Int = sizeOf(lowPriorityQueue(t))
  def inFlightWork(t: DependencyTracker): Set[DependencyTrackerAction[_]] = t.queue.stateManager.inFlightWork
  def isUpdating(t: DependencyTracker): Boolean = t.queue.stateManager.isUpdating
  def queueState(t: DependencyTracker): String = t.queue.stateManager.toString
  def outstandingWork(t: DependencyTracker): Seq[DependencyTrackerAction[_]] = t.queue.currentQueue()
  /** DependencyTrackerRoot.traversalId (private[this] var; found by suffix among declared fields). */
  def traversalId(root: DependencyTrackerRoot): Int = field(root, "traversalId").asInstanceOf[Int]
  def setTraversalId(root: DependencyTrackerRoot, v: Int): Unit = setField(root, "traversalId", Int.box(v))
  def withFreshTraversalId(root: DependencyTrackerRoot)(f: Integer => Integer): Unit = root.withFreshTraversalId(f)
  /** TweakableTracker.ttracks: tweakable key -> TTrackRoot (package-private class, hence the wildcard). */
  def ttracks(t: DependencyTracker): java.util.Map[TweakableKey, _] =
    t.tweakableTracker.ttracks.asInstanceOf[java.util.Map[TweakableKey, _]]
  def tweakableTracker(t: DependencyTracker): TweakableTracker = t.tweakableTracker
  def forceCleanup(t: DependencyTracker): Unit = t.tweakableTracker.forceCleanup()

  // ---- reflection --------------------------------------------------------------------------------------------------
  /** Declared field on o's class or a superclass whose name is `name` or ends with "$$name" (Scala mangling). */
  def findField(cls0: Class[_], name: String): Field = {
    var cls = cls0
    while (cls ne null) {
      val f = cls.getDeclaredFields.find(f => f.getName == name || f.getName.endsWith("$$" + name))
      if (f.isDefined) { f.get.setAccessible(true); return f.get }
      cls = cls.getSuperclass
    }
    throw new NoSuchFieldException(s"$name in ${cls0.getName}")
  }
  def field(o: AnyRef, name: String): AnyRef = findField(o.getClass, name).get(o)
  def setField(o: AnyRef, name: String, v: AnyRef): Unit = findField(o.getClass, name).set(o, v)
  private def sizeOf(wq: AnyRef): Int = {
    val m = wq.getClass.getDeclaredMethods.find(m => m.getName == "size" && m.getParameterCount == 0).get
    m.setAccessible(true)
    m.invoke(wq).asInstanceOf[Int]
  }

  private def complete[T](f: CompletableFuture[T])(r: Try[T]): Unit = r match {
    case Success(v) => f.complete(v)
    case Failure(e) => f.completeExceptionally(e)
  }
  def await[T](f: CompletableFuture[T], ms: Long = 10000): T = f.get(ms, TimeUnit.MILLISECONDS)
}
