package optimus.graph

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import optimus.platform.EvaluationQueue
import optimus.platform.ScenarioStack
import optimus.platform.storable.Entity

/*
 * Package-optimus.graph helpers for the cache Tier 1 probes (Tier1Cache*.scala): async hand-written property nodes
 * (no sync stacks), externally completed "gate" nodes, a batcher stub (OutOfWorkListener calling the package-private
 * NodeTask.fixWaiterChainForXSFTFromBatcher, as NodeBatcherSchedulerPlugin does with prioritiseConcurrency) and a
 * two-node waitingOn cycle (as in probes/src/optimus/graph/Tier1b.java T4).
 */

/** An async property: deps(node) are fresh keys (looked up with getNode) or plain nodes (attached and enqueued). */
final class T1cProp(val pp: ProbeProperty) {
  def info: NodeTaskInfo = pp.info
  val runs = new AtomicInteger
  @volatile var deps: T1cNode => Seq[NodeTask] = _ => Nil
  @volatile var compute: (T1cNode, Array[AnyRef]) => Long = (n, _) => n.arg.toLong
  def key(arg: Int): T1cNode = new T1cNode(this, arg)
}

final class T1cNode(val p: T1cProp, val arg: Int) extends PropertyNode[java.lang.Long] {
  override val entity: Entity = ProbeModule
  override def propertyInfo: NodeTaskInfo = p.info
  override def args: Array[AnyRef] = Array(Integer.valueOf(arg))
  override def argsHash: Int = arg
  override def argsEquals(that: NodeKey[_]): Boolean = that match {
    case a: T1cNode => a.arg == arg && (a.p eq p)
    case _        => false
  }
  private var kids: Array[NodeTask] = _
  private var finished = false

  override def run(ec: OGSchedulerContext): Unit = {
    if (kids eq null) {
      p.runs.incrementAndGet()
      val ds = p.deps(this)
      kids = ds.map {
        case k: PropertyNode[_] if k.scenarioStack() eq null => scenarioStack().getNode(k.asInstanceOf[PropertyNode[AnyRef]], ec)
        case n => if (n.scenarioStack() eq null) n.attach(scenarioStack()); n
      }.toArray
      if (kids.isEmpty) finish(ec)
      else {
        var i = 0
        while (i < kids.length) { val k = kids(i); ec.enqueue(this, k); k.continueWith(this, ec); i += 1 }
      }
    } else if (!finished && kids.forall(_.isDone)) finish(ec)
  }

  private def finish(ec: OGSchedulerContext): Unit = {
    finished = true
    kids.foreach(k => combineInfo(k, ec))
    kids.find(_.isDoneWithException) match {
      case Some(k) => completeWithException(k.exception, ec)
      case None =>
        val v =
          try java.lang.Long.valueOf(p.compute(this, kids.map(_.resultObject)))
          catch { case t: Throwable => completeWithException(t, ec); null }
        if (v ne null) completeWithResult(v, ec)
    }
  }
  override def toString: String = s"${p.info.name}($arg)"
}

/** A node completed from outside (a "batch" member or a promise): run() hands itself to onRun and returns. */
final class T1cGate(val name: String, onRun: (T1cGate, OGSchedulerContext) => Unit) extends CompletableNode[java.lang.Long] {
  @volatile var ranOn: String = _
  override def run(ec: OGSchedulerContext): Unit = { ranOn = Thread.currentThread().getName; onRun(this, ec) }
  def complete(v: Long, eq: EvaluationQueue): Unit = completeWithResult(java.lang.Long.valueOf(v), eq)
  def fail(t: Throwable, eq: EvaluationQueue): Unit = completeWithException(t, eq)
  override def toString: String = s"T1cGate($name)"
}

/**
 * Batcher stub. checkForNewWork (called by OGScheduler.runOnStalledCallbacks under the scheduler's _lock) runs
 * fixWaiterChainForXSFTFromBatcher on each pending gate, once (`fixOnce`), after `armed` (waits up to armWaitMs);
 * onGraphStalled completes the pending gates with `value` ("runs the batch").
 */
final class T1cBatchStub(val name: String) extends OutOfWorkListener {
  val pending = new ConcurrentLinkedQueue[T1cGate]()
  @volatile var armed = true
  @volatile var armWaitMs = 8000L
  @volatile var value = 7L
  @volatile var fixOnce = true
  @volatile var completeOnStall = true
  val checks = new AtomicInteger
  val fixCalls = new AtomicInteger
  val fixReturnedTrue = new AtomicInteger
  val stalls = new AtomicInteger
  @volatile var fixThread: String = _
  @volatile var fixThreadHeldSchedulerLock: Boolean = false
  @volatile var fixError: Throwable = _
  private val fixed = new java.util.concurrent.atomic.AtomicBoolean(false)

  def register(g: T1cGate, s: Scheduler): Unit = { pending.add(g); s.addOutOfWorkListener(this) }

  override def checkForNewWork(scheduler: Scheduler): Int = {
    checks.incrementAndGet()
    if (pending.isEmpty) return 0
    if (!armed) {
      val t0 = System.nanoTime()
      while (!armed && System.nanoTime() - t0 < armWaitMs * 1000000L) Thread.sleep(1)
    }
    var added = false
    if (!fixOnce || fixed.compareAndSet(false, true)) {
      fixThread = Thread.currentThread().getName
      fixThreadHeldSchedulerLock = T1cBatchStub.holdsSchedulerLock(scheduler)
      pending.forEach { g =>
        fixCalls.incrementAndGet()
        try {
          val r = g.fixWaiterChainForXSFTFromBatcher(scheduler)
          if (r) fixReturnedTrue.incrementAndGet()
          added |= r
        } catch { case t: Throwable => fixError = t; t.printStackTrace(System.out) }
      }
    }
    if (added) Int.MaxValue else pending.size
  }

  override def onGraphStalled(scheduler: Scheduler, outstandingTasks: java.util.ArrayList[NodeTask]): Unit = {
    stalls.incrementAndGet()
    if (completeOnStall) {
      var g = pending.poll()
      while (g ne null) { g.complete(value, scheduler); g = pending.poll() }
    } else if (!pending.isEmpty) scheduler.addOutOfWorkListener(this)
  }
}
object T1cBatchStub {
  def holdsSchedulerLock(s: Scheduler): Boolean = s match {
    case o: OGScheduler =>
      try { val f = classOf[OGScheduler].getDeclaredField("_lock"); f.setAccessible(true); Thread.holdsLock(f.get(o)) }
      catch { case _: Throwable => false }
    case _ => false
  }
}

/** Two-node waitingOn cycle (Tier1b T4 shape); default tryToRecoverFromCycle (false) breaks it with an exception. */
final class T1cCycNode(val name: String) extends CompletableNode[Integer] {
  var partner: T1cCycNode = _
  var first = false
  private var started = false
  override def run(ec: OGSchedulerContext): Unit = {
    if (partner.isDoneEx) { completeWithResult(1, ec); return }
    if (!started) {
      started = true
      if (first) ec.enqueue(this, partner)
      partner.continueWith(this, ec)
    }
  }
  override def toString: String = name
}

/** A client node: getNode(key) under its stack, enqueue, wait; records what getNode returned. */
final class T1cClient(val key: PropertyNode[java.lang.Long], val tag: String) extends CompletableNode[java.lang.Long] {
  @volatile var looked: Node[java.lang.Long] = _
  @volatile var afterLookup: T1cClient => Unit = null
  @volatile var ranOn: String = _
  override def run(ec: OGSchedulerContext): Unit = {
    if (looked eq null) {
      ranOn = Thread.currentThread().getName
      val n = scenarioStack().getNode(key, ec)
      looked = n
      ec.enqueue(this, n)
      n.continueWith(this, ec)
      val h = afterLookup
      if (h ne null) h(this)
    } else {
      combineInfo(looked, ec)
      if (looked.isDoneWithException) completeWithException(looked.exception, ec)
      else completeWithResult(looked.result, ec)
    }
  }
  override def toString: String = s"Client($tag)"
}

object T1cGraphAccess {
  lazy val visitLock: ReentrantLock = {
    val f = classOf[NodeTask].getDeclaredField("_visitLock"); f.setAccessible(true); f.get(null).asInstanceOf[ReentrantLock]
  }
  def holdsVisitLock: Boolean = visitLock.isHeldByCurrentThread
  def newScheduler(name: String, threads: Int): OGScheduler = new OGScheduler(name, threads, Thread.NORM_PRIORITY)
  def waitingOn(n: NodeTask): NodeTask = n.getWaitingOn
  def sleep(ms: Long): Unit = TimeUnit.MILLISECONDS.sleep(ms)
}
