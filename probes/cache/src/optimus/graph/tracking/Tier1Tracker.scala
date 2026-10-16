package optimus.graph.tracking

import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import optimus.graph._
import optimus.graph.ProbeDag._
import optimus.graph.tracking.{CacheTrackerAccess => T}
import optimus.graph.tracking.ttracks.Tier1TrackerTT
import optimus.platform._
import optimus.ui.ScenarioReference
import probes.CacheDelay

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.util.Try

/* =====================================================================================================================
 * Tier 1 tracker-side probes of the cache campaign (CACHE_CAMPAIGN.md): T4 T5 T6 T7 T8 T9 T10 T11 T12 T14 T17 T18.
 *
 *   probes/cache/run-tier1-tracker.sh [ids...]
 *
 * The parent runs every probe (variant) in its own child JVM (settings are static finals, and a wedged queue or a
 * logAndDie must not take the other probes with it). Each child prints RESULT lines and halts; a watchdog in the child
 * dumps tracker queues, OGScheduler.toString and thread stacks on timeout, prints a classified RESULT and exits 3.
 * ===================================================================================================================*/

// ---- probe-specific nodes -------------------------------------------------------------------------------------------

/** Combinator that, when `hold` is set, signals `started` after reading its children and blocks on the latch. */
final class HeldCombinator(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key, hold: CountDownLatch, started: CountDownLatch)
    extends Combinator(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = {
    val v = super.compute(ec)
    if (hold ne null) { if (started ne null) started.countDown(); hold.await() }
    v
  }
}

/** argsHash throws (R1 / T5): any cache lookup of this key throws synchronously. */
final class ThrowingHashNode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends Combinator(s, p, k) {
  override def argsHash: Int = throw new IllegalStateException("probe: argsHash throws")
}

/** Generic node: reads its declared children, then runs `body(value, ec)` (which may block or call tracker APIs). */
final class BodyNode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key, body: (Long, OGSchedulerContext) => Long)
    extends Combinator(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = body(super.compute(ec), ec)
}

/** T4(b): reads K, then takes the tracker's child dependency (childrenWithDependency); value = K + 1000 * #children. */
final class ChildCountNode(
    s: ProbeSystem,
    p: ProbeProperty,
    k: ProbeDag.Key,
    tracker: DependencyTracker,
    hold: CountDownLatch,
    started: CountDownLatch)
    extends Combinator(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = {
    val v = super.compute(ec)
    val n = tracker.childrenWithDependency.size
    if (hold ne null) { started.countDown(); hold.await() }
    v + 1000L * n
  }
}

/** T4(c): reads K, then (first run, if suspend) suspends on an uncancellable NodePromise completed off-graph. */
final class PromiseNode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key, ctl: Tier1Tracker.PromiseCtl)
    extends ProbePropertyNode(s, p, k) {
  @volatile private var phase = 0
  @volatile private var kv = 0L
  @volatile private var promise: NodePromise[java.lang.Long] = _
  override protected def compute(ec: OGSchedulerContext): Long = throw new UnsupportedOperationException
  override def run(ec: OGSchedulerContext): Unit = {
    if (phase == 0) {
      pp.runs.incrementAndGet()
      pp.runsByKey.computeIfAbsent(dkey, _ => new AtomicInteger).incrementAndGet()
      pp.lastRun.put(dkey, this)
      kv = read(pp.prop.reads.get(0))
      if (ctl.suspend) {
        phase = 1
        val pr = NodePromise.uncancellable[java.lang.Long]()
        promise = pr
        ctl.node.set(this)
        ctl.promise.set(pr)
        pr.underlyingNode.continueWith(this, ec)
      } else completeWithResult(java.lang.Long.valueOf(kv), ec)
    } else {
      ctl.resumed.incrementAndGet()
      val child = promise.underlyingNode
      combineInfo(child, ec)
      completeWithResult(java.lang.Long.valueOf(kv + child.result.longValue), ec)
    }
  }
}

/** T4(c): the low-priority action's node; starts a PromiseNode under its own stack and completes (orphaning it). */
final class OrphanStarter(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key, mk: () => NodeTask, ctl: Tier1Tracker.PromiseCtl)
    extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = {
    val n = ec.scenarioStack.getNode(mk().asInstanceOf[PropertyNode[_]], ec)
    ec.enqueue(n)
    // do not wait for n (it is an orphan once we complete), but let it start and suspend first
    val end = System.currentTimeMillis + 5000
    while ((ctl.promise.get eq null) && System.currentTimeMillis < end) Thread.sleep(1)
    7L
  }
}

object Tier1Tracker {
  final class PromiseCtl {
    @volatile var suspend = true
    val node = new AtomicReference[NodeTask]()
    val promise = new AtomicReference[NodePromise[java.lang.Long]]()
    val resumed = new AtomicInteger
  }

  // ---- result reporting --------------------------------------------------------------------------------------------
  val DEMONSTRATED = "DEMONSTRATED"
  val NOT_REPRODUCED = "NOT_REPRODUCED"
  val QUANTIFIED = "QUANTIFIED"
  val ERROR = "ERROR"
  val SKIPPED = "SKIPPED"

  def result(id: String, outcome: String, detail: String): Unit = {
    println(s"RESULT $id $outcome ${detail.replace('\n', ' ')}")
    System.out.flush()
  }
  def info(s: String): Unit = { println(s"     $s"); System.out.flush() }

  // ---- watchdog ----------------------------------------------------------------------------------------------------
  @volatile private var scheduler: AnyRef = _
  @volatile private var watchedTrackers: Seq[(String, DependencyTracker)] = Nil
  @volatile private var onTimeout: () => (String, String, String) = () => ("?", ERROR, "watchdog")
  def watch(ts: (String, DependencyTracker)*): Unit = watchedTrackers = watchedTrackers ++ ts
  def classifyOnTimeout(f: => (String, String, String)): Unit = onTimeout = () => f

  def startWatchdog(id: String, seconds: Int): Unit = {
    val t = new Thread(
      () => {
        try Thread.sleep(seconds * 1000L)
        catch { case _: InterruptedException => return }
        println(s"WATCHDOG $id fired after ${seconds}s")
        dumpAll()
        val (rid, outcome, detail) =
          try onTimeout()
          catch { case t: Throwable => (id, ERROR, s"watchdog classifier threw $t") }
        result(if (rid == "?") id else rid, outcome, s"[watchdog ${seconds}s] $detail")
        System.out.flush()
        Runtime.getRuntime.halt(3)
      },
      "t1t-watchdog")
    t.setDaemon(true)
    t.start()
  }

  def qs(t: DependencyTracker): String =
    try {
      val inf = T.inFlightWork(t)
      s"wq=${T.workQueueSize(t)} lpq=${T.lowPriorityQueueSize(t)} inFlight=${inf.size}" +
        s"${inf.map(a => Option(a.getClass.getSimpleName).filter(_.nonEmpty).getOrElse(a.toString.take(32))).mkString("[", ",", "]")} updating=${T.isUpdating(t)} " +
        s"scope=${if (T.currentCancellationScopeRaw(t) eq null) "null" else "set"}"
    } catch { case e: Throwable => s"<qs threw $e>" }

  def dumpAll(): Unit = {
    println("---- tracker queues")
    watchedTrackers.foreach { case (n, t) => println(s"  $n: ${qs(t)} | ${Try(T.queueState(t)).getOrElse("?")}") }
    println("---- OGScheduler.toString()")
    try println(String.valueOf(scheduler))
    catch { case t: Throwable => println(s"scheduler toString threw $t") }
    println("---- deadlocks: " + deadlocks())
    println("---- threads")
    println(threads(_ => true, 22))
    System.out.flush()
  }

  private val mx = ManagementFactory.getThreadMXBean
  def deadlocks(): String = {
    val ids = mx.findDeadlockedThreads()
    if (ids eq null) "none"
    else mx.getThreadInfo(ids, true, true).map(ti => s"${ti.getThreadName}(${ti.getThreadState} on ${ti.getLockName} owned by ${ti.getLockOwnerName})").mkString("; ")
  }
  /** States of threads matching f (by name), with lock owners and top frames. */
  def threads(f: String => Boolean, frames: Int = 6): String = {
    val sb = new StringBuilder
    for (ti <- mx.dumpAllThreads(true, true) if f(ti.getThreadName)) {
      val st = ti.getStackTrace
      if (st.exists(e => e.getClassName.startsWith("optimus")) || frames > 10) {
        sb.append(s"  --- ${ti.getThreadName} ${ti.getThreadState}")
        if (ti.getLockName ne null) sb.append(s" on ${ti.getLockName}")
        if (ti.getLockOwnerName ne null) sb.append(s" owned by ${ti.getLockOwnerName}")
        sb.append('\n')
        st.take(frames).foreach(e => sb.append(s"      $e\n"))
      }
    }
    sb.toString
  }
  /** Compact: name:state[lock owner] of the given threads. */
  def stateOf(ts: Thread*): String =
    ts.map { t =>
        val ti = mx.getThreadInfo(t.getId, 40)
        if (ti eq null) s"${t.getName}:gone"
        else {
          val top = ti.getStackTrace.find(e => e.getClassName.startsWith("optimus") || e.getClassName.startsWith("probes"))
          s"${t.getName}:${ti.getThreadState}${if (ti.getLockOwnerName ne null) s"(lock owner ${ti.getLockOwnerName})" else ""}@${top.map(e => s"${e.getClassName.split('.').last}.${e.getMethodName}:${e.getLineNumber}").getOrElse("-")}"
        }
      }
      .mkString(" ")
  /** Threads whose stack contains a frame matching `frame` (class.method substring). */
  def threadsIn(frame: String): Seq[String] =
    mx.dumpAllThreads(false, false).toSeq.collect {
      case ti if ti.getStackTrace.exists(e => s"${e.getClassName}.${e.getMethodName}".contains(frame)) =>
        s"${ti.getThreadName}:${ti.getThreadState}"
    }

  def thread(name: String)(body: => Unit): Thread = {
    val t = new Thread(
      () => {
        CacheFixtures.init()
        try body
        catch { case _: InterruptedException => () ; case e: Throwable => info(s"thread $name threw $e") }
      },
      name)
    t.setDaemon(true)
    t.start()
    t
  }

  /** Some(value) / None on timeout; Left(exception) on failure. */
  def awaitOpt[A](f: CompletableFuture[A], ms: Long): Either[Throwable, Option[A]] =
    try Right(Some(f.get(ms, TimeUnit.MILLISECONDS)))
    catch {
      case _: TimeoutException                          => Right(None)
      case e: java.util.concurrent.ExecutionException => Left(e.getCause)
      case e: Throwable                                 => Left(e)
    }
  def show[A](r: Either[Throwable, Option[A]]): String = r match {
    case Right(Some(v)) => s"completed($v)"
    case Right(None)    => "TIMEOUT"
    case Left(e)        => s"failed(${e.getClass.getSimpleName}: ${e.getMessage})"
  }
  def waitUntil(ms: Long)(cond: => Boolean): Boolean = {
    val end = System.currentTimeMillis + ms
    while (!cond) { if (System.currentTimeMillis > end) return false; Thread.sleep(5) }
    true
  }
  def keyFilter(info: NodeTaskInfo): java.util.function.Predicate[Object] = {
    case n: TweakableKey => n.propertyInfo eq info
    case _               => false
  }
  def xinfo(n: NodeTask): AnyRef = T.findField(classOf[NodeTask], "_xinfo").get(n)
  def tw(sys: ProbeSystem, k: Key, v: Long): Tweak = sys.tweak(Tw.instance(k, v))

  // ==================================================================================================================
  // T4 (D6, D15): four routes into the barrier hole
  // ==================================================================================================================
  final class T4Dag {
    val dag = new ProbeDag
    val K: Prop = dag.prop("K").base(1)
    val L: Prop = dag.prop("L").base(100)
    val A: Prop = dag.prop("A").notTweakable().reads(K)
    val B: Prop = dag.prop("B").notTweakable().reads(A).reads(L)
    val C: Prop = dag.prop("C").notTweakable().reads(L)
    val X: Prop = dag.prop("X").notTweakable().reads(K) // the route's node
    val M: Prop = dag.prop("M").notTweakable() // orphan starter (route c)
    val sys = new ProbeSystem(dag)
    val base: Seq[Prop] = Seq(K, L, A, B, C)
    def preEval(): SequentialEvaluator = { val ev = new SequentialEvaluator(Stack.of()); (base :+ X).foreach(p => ev.eval(key(p))); ev }
    val post: Stack = Stack.of(Level.of(Tw.instance(key(K), 50)))
    def oracle(p: Prop): Long = new SequentialEvaluator(post).eval(key(p))
  }

  /** Invalidation + value oracle over the base DAG after K := 50 (nodes cached before the update). */
  def t4Oracle(d: T4Dag, root: DependencyTracker, cachedBefore: Map[Key, ProbePropertyNode]): String = {
    val dependents = InvalidationOracle.dependents(d.preEval(), key(d.K)).asScala.toSet
    val missedInvalid = cachedBefore.collect { case (k, n) if dependents(k) && !n.isInvalidCache => k.toString }
    val overInvalid = cachedBefore.collect { case (k, n) if !dependents(k) && n.isInvalidCache => k.toString }
    val wrong = d.base.flatMap { p =>
      val v = T.evaluate(root, d.sys.newKey(p)).longValue
      if (v != d.oracle(p)) Some(s"${p.name}=$v(oracle ${d.oracle(p)})") else None
    }
    s"baseDAG{missedInvalid=${missedInvalid.mkString(",")} overInvalid=${overInvalid.mkString(",")} valueMismatch=${wrong.mkString(",")}}"
  }

  def t4(variant: String): Unit = variant match {
    case "a" => t4a()
    case "b" => t4b()
    case "c" => t4c()
    case "d" =>
      result("T4d", SKIPPED,
        "route (d) TweakExpander impure translator: the fixtures cannot express a tweak translator / also-set marker tweak " +
          "(ProbeSystem.tweak builds only value, by-name and when tweaks; none sets hasUnresolvedOrMarkerTweaks, so " +
          "TweakExpander never runs); D5 is T3 on the cache side")
  }

  private def t4Setup(d: T4Dag): (DependencyTrackerRoot, Map[Key, ProbePropertyNode]) = {
    val root = T.newRoot()
    watch("root" -> root)
    T.evaluateKeys(root, d.base.map(p => d.sys.newKey(p)))
    val cached = d.base.map(p => key(p) -> d.sys.pp(p).lastRun.get(key(p))).toMap
    (root, cached)
  }
  private def ttrackReport(d: T4Dag, root: DependencyTracker, n: NodeTask): String =
    s"fromK:${Tier1TrackerTT.locate(T.ttracks(root), n, keyFilter(d.sys.info(d.K)))} anyRoot:${Tier1TrackerTT.reachable(T.ttracks(root), n, null)} xinfo=${Tier1TrackerTT.describe(xinfo(n))}"

  /** (a) evaluation under the raw tracker stack (no queue, no cancel scope) spanning an update that tweaks K. */
  def t4a(): Unit = {
    val d = new T4Dag; import d._
    val (root, cached) = t4Setup(d)
    val hold = new CountDownLatch(1); val started = new CountDownLatch(1)
    val got = new AtomicReference[java.lang.Long]()
    val h = thread("t4a-raw") { got.set(CacheFixtures.eval(T.rawStack(root), new HeldCombinator(sys, sys.pp(X), key(X), hold, started))) }
    started.await(5, TimeUnit.SECONDS)
    val xNode = sys.pp(X).lastRun.get(key(X))
    val upd = T.addTweaksAsync(root, Seq(tw(sys, key(K), 50)))
    val updR = awaitOpt(upd, 5000)
    val trackDuring = Tier1TrackerTT.describe(xinfo(xNode))
    hold.countDown(); h.join(5000)
    val afterComplete = ttrackReport(d, root, xNode)
    val runs0 = sys.pp(X).runsOf(key(X))
    val v = T.evaluate(root, new HeldCombinator(sys, sys.pp(X), key(X), null, null)).longValue
    val reran = sys.pp(X).runsOf(key(X)) - runs0
    val stale = v != oracle(X)
    val oracleStr = t4Oracle(d, root, cached)
    // D15: does a later K update reach X's node?
    val x2 = sys.pp(X).lastRun.get(key(X))
    T.addTweaks(root, tw(sys, key(K), 60))
    val laterInvalid = x2.isInvalidCache
    val v2 = T.evaluate(root, new HeldCombinator(sys, sys.pp(X), key(X), null, null)).longValue
    result("T4a", if (stale) DEMONSTRATED else NOT_REPRODUCED,
      s"raw-stack X read K=1, update K:=50 ${show(updR)} while X held (X track during update: $trackDuring); X completed=${got.get}; " +
        s"queue eval of X after update=$v oracle=${oracle(X)} reran=$reran xNode.isInvalidCache=${xNode.isInvalidCache} " +
        s"ttrack after completion {$afterComplete}; next update K:=60 invalidated X=$laterInvalid, X then=$v2 (oracle 60); " +
        oracleStr)
  }

  /** (b) newChildScenario from a client thread while an evaluation holding the child dependency is in flight. */
  def t4b(): Unit = {
    val d = new T4Dag; import d._
    val (root, cached) = t4Setup(d)
    val hold = new CountDownLatch(1); val started = new CountDownLatch(1)
    val f = T.evaluateKeysAsync(root, Seq(new ChildCountNode(sys, sys.pp(X), key(X), root, hold, started)))
    started.await(5, TimeUnit.SECONDS)
    val xNode = sys.pp(X).lastRun.get(key(X))
    val newChild = new CompletableFuture[String]
    thread("t4b-client") { newChild.complete(T.consistentChild(root, "kid").name) }
    val ncR = awaitOpt(newChild, 5000)
    val trackAfterFire = Tier1TrackerTT.describe(xinfo(xNode))
    hold.countDown()
    val fR = awaitOpt(f, 5000)
    val expect1 = 1L + 1000L * root.children.size
    val runs0 = sys.pp(X).runsOf(key(X))
    val v = T.evaluate(root, new ChildCountNode(sys, sys.pp(X), key(X), root, null, null)).longValue
    val reran = sys.pp(X).runsOf(key(X)) - runs0
    val staleChildren = v != expect1
    val xInvalidThen = xNode.isInvalidCache
    val afterComplete = ttrackReport(d, root, xNode)
    // then a K update: does it reach the node completed onto the invalidated track?
    val x2 = sys.pp(X).lastRun.get(key(X))
    val before2 = Tier1TrackerTT.describe(xinfo(x2))
    T.addTweaks(root, tw(sys, key(K), 50))
    val oracleStr = t4Oracle(d, root, cached)
    val v2 = T.evaluate(root, new ChildCountNode(sys, sys.pp(X), key(X), root, null, null)).longValue
    val expect2 = 50L + 1000L * root.children.size
    val staleK = v2 != expect2
    result("T4b", if (staleChildren || staleK) DEMONSTRATED else NOT_REPRODUCED,
      s"X read K and took the child dependency, held in flight; client newChildScenario ${show(ncR)} (X track after fire: $trackAfterFire); " +
        s"X eval ${show(fR)}; queue eval of X=$v expected=$expect1 (children=${root.children.size}) reran=$reran " +
        s"xNode.isInvalidCache=$xInvalidThen; ttrack {$afterComplete}; then K:=50 (base DAG $oracleStr): X=$v2 expected=$expect2 " +
        s"x2.isInvalidCache=${x2.isInvalidCache} (x2 track before: $before2)")
  }

  /** (c) orphan suspended on an uncancellable promise; LP phase ends; promise completed inside the update's barrier. */
  def t4c(): Unit = {
    val d = new T4Dag; import d._
    val (root, cached) = t4Setup(d)
    val ctl = new PromiseCtl
    val lp = T.evaluateKeysLowPriorityAsync(root, Seq(new OrphanStarter(sys, sys.pp(M), key(M), () => new PromiseNode(sys, sys.pp(X), key(X), ctl), ctl)))
    val lpR = awaitOpt(lp, 5000)
    waitUntil(5000)(ctl.promise.get ne null)
    val phaseEnded = waitUntil(5000)(T.currentCancellationScopeRaw(root) eq null)
    val xNode = ctl.node.get
    val atPhaseEnd = s"isDone=${xNode.isDone} cancelled=${xNode.isDoneWithCancellation} promiseDone=${ctl.promise.get.underlyingNode.isDone}"
    Thread.sleep(200)
    val beforeUpdate = s"isDone=${xNode.isDone} cancelled=${xNode.isDoneWithCancellation} promiseDone=${ctl.promise.get.underlyingNode.isDone}"
    val fired = new AtomicInteger
    val resumedInHook = new AtomicInteger(-1)
    CacheDelay.onHit("dtq.barrier.beforeWait", _ => {
      if (fired.getAndIncrement() == 0) {
        val c = thread("t4c-completer") { ctl.promise.get.complete(scala.util.Success(java.lang.Long.valueOf(7))) }
        c.join(2000)
        Thread.sleep(300)
        resumedInHook.set(ctl.resumed.get)
      }
    })
    val upd = T.batchAsync(root)(_.addTweaksImmediate(Seq(tw(sys, key(K), 50)), throwOnDuplicate = false))
    val updR = awaitOpt(upd, 5000)
    CacheDelay.clearHandler("dtq.barrier.beforeWait")
    if (fired.get == 0) ctl.promise.get.complete(scala.util.Success(java.lang.Long.valueOf(7)))
    waitUntil(2000)(xNode.isDone)
    val state = s"isDone=${xNode.isDone} cancelled=${xNode.isDoneWithCancellation} usable=${xNode.isDoneWithUsableResult} " +
      s"invalid=${xNode.isInvalidCache} resumedRuns=${ctl.resumed.get} result=${Try(xNode.resultObject).getOrElse("-")}"
    ctl.suspend = false
    val runs0 = sys.pp(X).runsOf(key(X))
    val v = T.evaluate(root, new PromiseNode(sys, sys.pp(X), key(X), ctl)).longValue
    val reran = sys.pp(X).runsOf(key(X)) - runs0
    val stale = v != oracle(X) && v != oracle(X) + 7
    result("T4c", if (stale) DEMONSTRATED else NOT_REPRODUCED,
      s"LP M ${show(lpR)} orphaned X (read K, suspended on uncancellable promise); LP phase ended=$phaseEnded (X then: $atPhaseEnd; 200 ms later: $beforeUpdate); update K:=50 ${show(updR)}; " +
        s"barrier hook fired=${fired.get} (promise completed inside it; X resumed runs by end of hold=${resumedInHook.get}); " +
        s"orphan X: $state ttrack{${ttrackReport(d, root, xNode)}}; queue eval of X after=$v oracle=${oracle(X)} reran=$reran; " +
        t4Oracle(d, root, cached))
  }

  // ==================================================================================================================
  // T5 (R1): synchronous throw from executeAsync wedges the queue
  // ==================================================================================================================
  def t5(variant: String): Unit = {
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val bad = dag.prop("bad").notTweakable().reads(a)
    val ok = dag.prop("ok").notTweakable().reads(a)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    watch("root" -> root)
    T.evaluateKeys(root, Seq(sys.newKey(ok)))
    variant match {
      case "argsHash" =>
        val thrown = Try(T.evaluateKeysAsync(root, Seq(new ThrowingHashNode(sys, sys.pp(bad), key(bad)))))
        val f1 = thrown.toOption
        val after = qs(root)
        val f1R = f1.map(awaitOpt(_, 1000)).map(show).getOrElse("-")
        val upd = T.addTweaksAsync(root, Seq(tw(sys, key(a), 2)))
        val updR = awaitOpt(upd, 5000)
        val wedged = updR == Right(None)
        result("T5", if (wedged && T.inFlightWork(root).nonEmpty) DEMONSTRATED else NOT_REPRODUCED,
          s"evaluateNodeKeysAsync(argsHash throws) -> ${thrown.failed.map(e => s"threw ${e.getClass.getSimpleName}: ${e.getMessage}").getOrElse("returned")} " +
            s"callback=$f1R; queue after: $after; addTweaksAsync ${show(updR)}; queue now: ${qs(root)}")
      case "npe" =>
        val thrown = new AtomicReference[String]("returned")
        val t = new Thread(() => {
          try T.evaluateKeysAsync(root, Seq(sys.newKey(ok)))
          catch { case e: Throwable => thrown.set(s"threw ${e.getClass.getSimpleName}: ${e.getMessage} at ${e.getStackTrace.headOption.getOrElse("")}") }
        }, "t5-no-ec")
        t.start(); t.join(5000)
        val after = qs(root)
        val upd = T.addTweaksAsync(root, Seq(tw(sys, key(a), 2)))
        val updR = awaitOpt(upd, 5000)
        val wedged = updR == Right(None)
        result("T5-npe", if (wedged && T.inFlightWork(root).nonEmpty) DEMONSTRATED else NOT_REPRODUCED,
          s"evaluateNodeKeysAsync from a plain thread (no EvaluationContext) ${thrown.get}; queue after: $after; " +
            s"addTweaksAsync from main ${show(updR)}; queue now: ${qs(root)}")
    }
  }

  // ==================================================================================================================
  // T6 (R2): many-node async evaluation from inside another tracker's node; caller's scope cancelled first
  // ==================================================================================================================
  def t6(variant: String): Unit = {
    val many = variant == "many"
    val id = if (many) "T6" else "T6-control"
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val held = dag.prop("held").notTweakable().reads(a)
    val other = dag.prop("other").notTweakable().reads(a)
    val n = dag.prop("n").notTweakable().reads(a)
    val sys = new ProbeSystem(dag)
    val r1 = T.newRoot()
    val t1 = T.consistentChild(r1, "t1")
    val t2 = T.newRoot()
    watch("T1(r1/t1)" -> t1, "T2" -> t2)
    val hold = new CountDownLatch(1); val heldStarted = new CountDownLatch(1)
    val t2f = new CompletableFuture[String]
    val issued = new AtomicReference[String]("not issued")
    val nKey = new BodyNode(sys, sys.pp(n), key(n), (v, ec) => {
      val hk = new HeldCombinator(sys, sys.pp(held), key(held), hold, heldStarted)
      try {
        if (many)
          t2.evaluateNodeKeysAsync(Seq(hk, sys.newKey(other)), r => t2f.complete(r.map(_ => "ok").getOrElse("fail")), T.cause("t6"), NullScope)
        else
          t2.evaluateNodeKeyAsync(hk, (r: Try[java.lang.Long]) => t2f.complete(r.map(_.toString).getOrElse("fail")), T.cause("t6"), NullScope)
        issued.set(s"issued from ${Thread.currentThread.getName}")
      } catch { case e: Throwable => issued.set(s"threw $e") }
      v
    })
    val f1 = T.evaluateKeysAsync(t1, Seq(nKey))
    val f1R = awaitOpt(f1, 5000)
    val t1Ended = waitUntil(5000)(T.inFlightWork(t1).isEmpty && (T.currentCancellationScopeRaw(t1) eq null))
    heldStarted.await(5, TimeUnit.SECONDS)
    val t2Before = qs(t2)
    hold.countDown()
    val t2R = awaitOpt(t2f, 3000)
    val upd = T.addTweaksAsync(t2, Seq(tw(sys, key(a), 2)))
    val updR = awaitOpt(upd, 5000)
    val wedged = updR == Right(None)
    result(id, if (wedged) DEMONSTRATED else NOT_REPRODUCED,
      s"${if (many) "evaluateNodeKeysAsync(2 keys)" else "evaluateNodeKeyAsync(1 key)"} on T2 from a node of T1: ${issued.get}; T1 eval ${show(f1R)}, " +
        s"T1 phase ended (scope cancelled+reset)=$t1Ended; T2 before release: $t2Before; held released; T2 eval callback ${show(t2R)}; " +
        s"addTweaksAsync(T2) ${show(updR)}; T2 now: ${qs(t2)}")
  }

  // ==================================================================================================================
  // T7 (R3): watermark cleanup from complete() vs low-priority cancel under queueLock
  // ==================================================================================================================
  def t7(variant: String): Unit = {
    val manual = variant == "manual"
    val id = if (manual) "T7-manual" else "T7"
    val dag = new ProbeDag
    val leaf = dag.prop("leaf").base(1)
    val other = dag.prop("other").base(2)
    val comb = dag.prop("comb").notTweakable()
    for (i <- 0 until 200) comb.reads(leaf, MODULE, i)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    watch("root" -> root)
    info(s"watermark=${Settings.ttrackClearedNodeRefWatermark} cacheSize=${optimus.graph.cache.UNodeCache.global.getMaxSize}")
    val combKeyArg = new AtomicInteger(1000)
    // warm-up: generate cleared TTrackRefs (re-evaluate after invalidating, drop lastRun pins, GC)
    for (r <- 0 until 8) {
      T.evaluateKeys(root, (0 until 5).map(j => sys.newKey(comb, MODULE, r * 10 + j)))
      T.addTweaks(root, sys.tweak(Tw.property(leaf, 10 + r)))
      sys.props.foreach(_.lastRun.clear())
    }
    // garbage generation outside actions (queue idle): raw-stack evaluations leave no action/cause holding the nodes
    (0 until 40).foreach(j => CacheFixtures.eval(T.rawStack(root), sys.newKey(comb, MODULE, 500 + j)))
    val probeRefs = sys.props.toSeq.flatMap(_.lastRun.values.asScala).map(n => new java.lang.ref.WeakReference[AnyRef](n))
    sys.props.foreach(_.lastRun.clear())
    System.gc(); Thread.sleep(100)
    info(s"global cache size=${optimus.graph.cache.UNodeCache.global.getSizeIndicative}; alive before clear: ${probeRefs.count(_.get ne null)}/${probeRefs.size}")
    if (variant == "gc") { // the warm-up nodes are still held by the cache: drop them (the TTrackRefs stay)
      val n = optimus.graph.cache.Caches.clearAllCaches(optimus.graph.cache.CauseGCNative)
      info(s"clearAllCaches removed $n")
    }
    val hold = new CountDownLatch(1); val started = new CountDownLatch(1)
    val zArg = combKeyArg.incrementAndGet()
    val lpThread = new AtomicReference[Thread]()
    val refQ = T.tweakableTracker(root).refQ
    val clearedAfterWarmup = refQ.snapshot
    val z = new BodyNode(sys, sys.pp(comb), ProbeDag.key(comb, MODULE, zArg), (v, _) => {
      lpThread.set(Thread.currentThread); started.countDown(); hold.await()
      // manual: the public test hook, same path as a TTrackRef creation that finds cleared refs (RefCounter.maybeBreachWatermark)
      if (manual) refQ.incrementCountManuallyForTestingAndTestWatermark(1)
      v
    })
    val lp = T.evaluateKeysLowPriorityAsync(root, Seq(z))
    started.await(5, TimeUnit.SECONDS)
    // populate the reference queue now (Z holds a graph thread; the warm-up nodes are unreachable)
    System.gc(); Thread.sleep(200); System.gc(); Thread.sleep(200)
    info(s"after GC: ${probeRefs.count(_.get ne null)}/${probeRefs.size} of the last warm-up nodes still strongly reachable")
    val alive = probeRefs.find(_.get ne null).map(_.get)
    alive.foreach(a => info(s"sample alive: $a ${a.getClass.getSimpleName}"))
    val loops = new AtomicInteger
    CacheDelay.onHit("sched.waitCancelled.loop", _ => {
      if (loops.getAndIncrement() == 0) hold.countDown()
      Thread.sleep(20)
    })
    val updThread = new AtomicReference[Thread]()
    val upd = new CompletableFuture[String]
    val u = thread("t7-updater") {
      updThread.set(Thread.currentThread)
      val f = T.addTweaksAsync(root, Seq(sys.tweak(Tw.property(other, 3))))
      upd.complete(show(awaitOpt(f, 30000)))
    }
    def classify(): (String, String, String) = {
      val lpT = lpThread.get
      val dl = deadlocks()
      val zState = if (lpT ne null) stateOf(lpT) else "?"
      val uState = stateOf(u)
      val blockedOnQueueLock = (lpT ne null) && {
        val ti = mx.getThreadInfo(lpT.getId, 40)
        ti.getThreadState == Thread.State.BLOCKED && ti.getLockOwnerName == u.getName &&
        ti.getStackTrace.exists(_.getMethodName.contains("maybeRunCleanupNow"))
      }
      val uWaiting = threadsIn("waitForCancelledNodeToStop").exists(_.startsWith(u.getName))
      (id, if (blockedOnQueueLock && uWaiting) DEMONSTRATED else if (!upd.isDone) DEMONSTRATED else NOT_REPRODUCED,
        s"${if (manual) "breach via refQ.incrementCountManuallyForTestingAndTestWatermark from the LP node" else "breach via real cleared TTrackRefs (GC)"}; " +
        s"cleared refs warmup=$clearedAfterWarmup now=${refQ.snapshot}; update not complete; findDeadlockedThreads=$dl; Z(LP graph thread)=$zState blockedOnQueueLockInMaybeRunCleanupNow=$blockedOnQueueLock; " +
          s"updater=$uState inWaitForCancelledNodeToStop=$uWaiting; waitCancelled loops=${loops.get}; root ${qs(root)}")
    }
    classifyOnTimeout(classify())
    val r = awaitOpt(upd, 10000)
    if (r == Right(None)) {
      val (_, o, d) = classify()
      println("---- stacks (Z and updater)")
      println(threads(n => n == u.getName || ((lpThread.get ne null) && n == lpThread.get.getName), 30))
      result(id, o, d)
    } else {
      CacheDelay.clearHandler("sched.waitCancelled.loop")
      result(id, NOT_REPRODUCED,
        s"${if (manual) "manual breach" else "GC breach"}; cleared refs warmup=$clearedAfterWarmup now=${refQ.snapshot}; update ${r.toOption.flatten.getOrElse(r)} with waitCancelled loops=${loops.get}; LP ${show(awaitOpt(lp, 5000))}; deadlocks=${deadlocks()}; root ${qs(root)}")
    }
  }

  // ==================================================================================================================
  // T8 (R5, R6)
  // ==================================================================================================================
  def t8(variant: String): Unit = {
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val b = dag.prop("b").base(5)
    val c = dag.prop("c").notTweakable().reads(a)
    val p = dag.prop("p").notTweakable().reads(b)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    watch("root" -> root)
    variant match {
      case "a" =>
        val scope = T.newScope("t8a")
        val once = new AtomicBoolean
        val cbThread = new AtomicReference[Thread]()
        val cbDone = new AtomicReference[String]("not run")
        T.registerNodesInvalidated(root, scope)((nodes, _) =>
          if (once.compareAndSet(false, true)) {
            cbThread.set(Thread.currentThread)
            cbDone.set("entered")
            T.addTweaks(root, tw(sys, key(b), 6)) // sync, same queue, from inside the update
            cbDone.set("returned")
          })
        T.track(root, sys.newKey(c), scope)
        T.evaluate(root, sys.newKey(c), scope)
        val upd = T.addTweaksAsync(root, Seq(tw(sys, key(a), 2)))
        classifyOnTimeout(("T8a", DEMONSTRATED, s"callback ${cbDone.get}; ${Option(cbThread.get).map(stateOf(_)).getOrElse("-")}; root ${qs(root)}"))
        val r = awaitOpt(upd, 5000)
        val cbT = cbThread.get
        val inAwait = threadsIn("DependencyTrackerAction.await")
        val hang = r == Right(None)
        result("T8a", if (hang) DEMONSTRATED else NOT_REPRODUCED,
          s"registerNodesInvalidated callback calling addTweaks (sync) on its own queue: update ${show(r)}; callback ${cbDone.get} " +
            s"on ${Option(cbT).map(stateOf(_)).getOrElse("-")}; threads in DependencyTrackerAction.await=${inAwait.mkString(",")}; " +
            s"findDeadlockedThreads=${deadlocks()}; root ${qs(root)}")
      case "b" =>
        val gate = new CountDownLatch(1); val started = new CountDownLatch(1)
        val pThread = new AtomicReference[Thread]()
        val inner = new AtomicReference[String]("not called")
        val pk = new BodyNode(sys, sys.pp(p), key(p), (v, _) => {
          pThread.set(Thread.currentThread); started.countDown(); gate.await()
          inner.set("calling evaluateKeys")
          T.evaluateKeys(root, Seq(sys.newKey(c)))
          inner.set("returned")
          v
        })
        val lp = T.evaluateKeysLowPriorityAsync(root, Seq(pk))
        started.await(5, TimeUnit.SECONDS)
        val loops = new AtomicInteger
        CacheDelay.onHit("sched.waitCancelled.loop", _ => { if (loops.getAndIncrement() == 0) { gate.countDown(); Thread.sleep(100) } })
        val updDone = new CompletableFuture[String]
        val u = thread("t8b-updater") { updDone.complete(show(awaitOpt(T.addTweaksAsync(root, Seq(tw(sys, key(a), 2))), 30000))) }
        def detail = s"LP node inner sync evaluateKeys: ${inner.get}; LP node ${Option(pThread.get).map(stateOf(_)).getOrElse("-")}; " +
          s"updater ${stateOf(u)} (inWaitForCancelledNodeToStop=${threadsIn("waitForCancelledNodeToStop").exists(_.startsWith(u.getName))}); " +
          s"waitCancelled loops=${loops.get}; findDeadlockedThreads=${deadlocks()}; root ${qs(root)}"
        classifyOnTimeout(("T8b", DEMONSTRATED, detail))
        val r = awaitOpt(updDone, 5000)
        if (r == Right(None)) {
          println(threads(n => n == u.getName || ((pThread.get ne null) && n == pThread.get.getName), 25))
          result("T8b", DEMONSTRATED, s"update not complete in 5 s; $detail")
        } else result("T8b", NOT_REPRODUCED, s"update ${r}; LP ${show(awaitOpt(lp, 3000))}; $detail")
    }
  }

  // ==================================================================================================================
  // T9 (R7): batch update over parent and child with a by-name tweak; vary map size and order
  // ==================================================================================================================
  def t9(): Unit = {
    val rows = new StringBuilder
    var bad = 0
    var runs = 0
    for (instance <- Seq(true, false); n <- Seq(2, 5, 17, 33); order <- Seq("hash", "P-first", "C-first")) {
      val dag = new ProbeDag
      val pp = dag.prop("p").base(1)
      val cc = dag.prop("c").base(0)
      val ff = dag.prop("f").base(0)
      val sys = new ProbeSystem(dag)
      val pTr = T.newRoot()
      val cTr = T.consistentChild(pTr, "C")
      val fillers = (0 until n - 2).map(i => T.consistentChild(pTr, s"F$i"))
      val rhs = new Rhs(java.util.List.of(new Ref(pp, MODULE, 0)), (_, xs) => xs.sum + 1)
      def pTw(v: Long) = if (instance) Tw.instance(key(pp), v) else Tw.property(pp, v)
      val cTw = if (instance) Tw.instance(key(cc), rhs, Mode.BYNAME) else Tw.property(cc, rhs, Mode.BYNAME)
      T.addTweaks(pTr, sys.tweak(pTw(5)))
      T.addTweaks(cTr, sys.tweak(cTw))
      val pre = T.evaluate(cTr, sys.newKey(cc)).longValue
      val entries: Seq[(ScenarioReference, Seq[Tweak])] =
        Seq(cTr.scenarioReference -> Seq(sys.tweak(cTw)), pTr.scenarioReference -> Seq(sys.tweak(pTw(10)))) ++
          fillers.zipWithIndex.map { case (f, i) => f.scenarioReference -> Seq(sys.tweak(Tw.instance(ProbeDag.key(ff, MODULE, i), i))) }
      val m: Map[ScenarioReference, Seq[Tweak]] = order match {
        case "hash"    => entries.toMap
        case "P-first" => ListMap((entries(1) +: entries(0) +: entries.drop(2)): _*)
        case "C-first" => ListMap(entries: _*)
      }
      val iterOrder = m.keys.toSeq.map(r => if (r == pTr.scenarioReference) "P" else if (r == cTr.scenarioReference) "C" else "f").filter(_ != "f").mkString
      T.batch(pTr)(_.multiUpdater.addTweakMapImmediate(m, Map.empty, throwOnDuplicate = false))
      val post = T.evaluate(cTr, sys.newKey(cc)).longValue
      val postP = T.evaluate(cTr, sys.newKey(pp)).longValue
      val oracle = new SequentialEvaluator(Stack.of(Level.of(pTw(10)), Level.of(cTw))).eval(key(cc))
      runs += 1
      if (post != oracle) { bad += 1; rows.append(s" MISMATCH[${if (instance) "inst" else "prop"} n=$n $order iter=$iterOrder c=$post oracle=$oracle]") }
      else if (n == 33 && order == "hash") rows.append(s" [${if (instance) "inst" else "prop"} n=33 hash iter=$iterOrder pre=$pre c=$post p=$postP oracle=$oracle]")
      T.disposeAllRoots()
    }
    result("T9", if (bad > 0) DEMONSTRATED else NOT_REPRODUCED,
      s"$runs batches (instance/property tweaks x n in {2,5,17,33} x order {hash,P-first,C-first}); mismatches=$bad;$rows " +
        "(plain by-name tweaks do not set hasUnresolvedOrMarkerTweaks, so doAddTweaks never expands them against the partial batch)")
  }

  // ==================================================================================================================
  // T10 (R8): setPluginTag vs concurrent freeze
  // ==================================================================================================================
  /** Unwrap NodeInputMapValue layers (see T10-wrap) and report the payload. */
  def unwrapTag(o: Option[Any]): (Option[Any], Int) = {
    var depth = 0
    var v: Any = o.orNull
    while (v.isInstanceOf[optimus.platform.inputs.NodeInputMapValue[_]]) {
      v = v.asInstanceOf[optimus.platform.inputs.NodeInputMapValue[_]].value(); depth += 1
    }
    (Option(v), depth)
  }

  def t10(variant: String): Unit = if (variant == "wrap") t10wrap() else {
    val clearFirst = variant == "clear"
    val id = if (clearFirst) "T10-clear" else "T10"
    CacheFixtures.init()
    val root = T.newRoot()
    val kid = T.consistentChild(root, "kid")
    watch("root" -> root)
    def set(v: Long): Unit = {
      if (clearFirst) root.clearPluginTag(ProbeTagKey) // avoids the double wrap of T10-wrap: always addInput's None branch
      root.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(v))
    }
    set(0)
    val stop = new AtomicBoolean
    val reads = new AtomicInteger
    val readers = (0 until 8).map(i => thread(s"t10-reader-$i") {
      val ss = T.rawStack(root)
      val ssK = T.rawStack(kid)
      while (!stop.get) {
        if ((reads.incrementAndGet() & 1) == 0) ss.findPluginTag(ProbeTagKey)
        else CacheFixtures.evalIn(ssK)(EvaluationContext.findPluginTag(ProbeTagKey))
      }
    })
    var staleRaw, staleQueue, staleKid, persistent, absent = 0
    var maxWrap = 0
    val examples = new StringBuilder
    def check(i: Int, what: String, o: Option[Any]): Boolean = {
      val (v, depth) = unwrapTag(o)
      maxWrap = math.max(maxWrap, depth)
      if (v.isEmpty) absent += 1
      val ok = v.contains(java.lang.Long.valueOf(i))
      if (!ok && examples.length < 300) examples.append(s" [i=$i $what=$v]")
      ok
    }
    for (i <- 1 to 1000) {
      set(i)
      if (!check(i, "raw", T.rawStack(root).findPluginTag(ProbeTagKey))) {
        staleRaw += 1
        Thread.sleep(1)
        if (!unwrapTag(T.rawStack(root).findPluginTag(ProbeTagKey))._1.contains(java.lang.Long.valueOf(i))) persistent += 1
      }
      if (!check(i, "queue", T.executeEvaluate(root)(EvaluationContext.findPluginTag(ProbeTagKey)))) staleQueue += 1
      if (!check(i, "kid", T.rawStack(kid).findPluginTag(ProbeTagKey))) staleKid += 1
    }
    stop.set(true); readers.foreach(_.join(2000))
    val kidSS = T.rawStack(kid)
    val has = kidSS.hasPluginTag(ProbeTagKey)
    val find = unwrapTag(kidSS.findPluginTag(ProbeTagKey))._1
    val rootHas = T.rawStack(root).hasPluginTag(ProbeTagKey)
    val disagree = has != find.isDefined
    val stale = staleRaw + staleQueue + staleKid
    result(id, if (stale > 0 || disagree) DEMONSTRATED else NOT_REPRODUCED,
      s"1000 ${if (clearFirst) "clearPluginTag+setPluginTag" else "setPluginTag"} on root with 8 concurrent readers (${reads.get} reads); " +
        s"stale/absent after the write returned (payload compared after unwrapping NodeInputMapValue, max wrap depth $maxWrap): rootRawStack=$staleRaw " +
        s"(still stale 1 ms later: $persistent) viaQueueEvaluate=$staleQueue kidRawStack=$staleKid absent=$absent;$examples; inherited tag on " +
        s"consistent child: hasPluginTag=$has findPluginTag=$find (root hasPluginTag=$rootHas) disagree=$disagree")
  }

  /** New: a second setPluginTag for the same key double-wraps the value (MutableNodeInputMap.addInput Some branch). */
  def t10wrap(): Unit = {
    val dag = new ProbeDag
    val t = dag.prop("t").notTweakable().dontCache().kind(Kind.TAG_READER)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    root.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(5))
    val v1: Option[Any] = T.rawStack(root).findPluginTag(ProbeTagKey)
    val r1 = Try(T.evaluate(root, sys.newKey(t)): Any)
    root.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(6))
    val v2: Option[Any] = T.rawStack(root).findPluginTag(ProbeTagKey)
    val r2 = Try(T.evaluate(root, sys.newKey(t)): Any)
    val cls2 = v2.map(_.getClass.getName).orNull
    val broken = v2.exists(_.isInstanceOf[optimus.platform.inputs.NodeInputMapValue[_]]) || r2.isFailure
    result("T10-wrap", if (broken) DEMONSTRATED else NOT_REPRODUCED,
      s"NEW: setPluginTag(k, 5) then setPluginTag(k, 6) on the same tracker: findPluginTag after first=$v1 (TagReader eval ${r1}); " +
        s"after second=${v2} class=$cls2 unwrapped=${unwrapTag(v2)} (TagReader eval ${r2.failed.map(e => s"failed ${e.getClass.getSimpleName}: ${e.getMessage}").getOrElse(r2.toString)}); " +
        "MutableNodeInputMap.addInput wraps PluginTagKey.combine's NodeInputMapValue in another NodeInputMapValue")
  }

  // ==================================================================================================================
  // T11 (R9): concurrent removePendingInvalidate during an invalidation batch
  // ==================================================================================================================
  def t11(): Unit = {
    val dag = new ProbeDag
    val k = dag.prop("k").base(1)
    val c = dag.prop("c").notTweakable().reads(k, MODULE, 0)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    watch("root" -> root)
    val scope = T.newScope("t11")
    val N = 2000
    val keys = (0 until N).map(i => ProbeDag.key(c, MODULE, i))
    val delivered = ConcurrentHashMap.newKeySet[Integer]()
    val cbCalls = new AtomicInteger
    T.registerNodesInvalidated(root, scope)((nodes, _) => {
      cbCalls.incrementAndGet()
      nodes.foreach { case (tn, _) => tn.key match { case p: ProbePropertyNode => delivered.add(p.arg); case _ => } }
    })
    keys.foreach(kk => T.track(root, sys.newKey(kk), scope))
    var lost, dup, exceptions, rounds = 0
    val exSamples = new StringBuilder
    val ref = root.scenarioReference
    for (r <- 0 until 100) {
      T.evaluateKeys(root, keys.map(sys.newKey), scope)
      delivered.clear()
      val removed = (0 until N).filter(_ % 2 == 0)
      val slices = removed.grouped(removed.size / 8).toSeq.take(8)
      val exc = new AtomicInteger
      T.batch(root) { u =>
        u.addTweaksImmediate(Seq(tw(sys, key(k), 10 + r)), throwOnDuplicate = false)
        val ev = u.multiUpdater
        val barrier = new CyclicBarrier(8)
        val done = new CountDownLatch(8)
        slices.zipWithIndex.foreach { case (slice, i) =>
          thread(s"t11-g$i") {
            val ks: Set[NodeKey[java.lang.Long]] = slice.map(j => sys.newKey(keys(j)): NodeKey[java.lang.Long]).toSet
            try {
              barrier.await(5, TimeUnit.SECONDS)
              ev.evaluateNodeKeysAsync(Map(ref -> ks), (_: EventCause) => (), removePendingInvalidate = true, T.cause("t11"), scope)
            } catch {
              case e: Throwable =>
                exc.incrementAndGet()
                if (exSamples.length < 300) exSamples.synchronized(exSamples.append(s" [$e]"))
            } finally done.countDown()
          }
        }
        done.await(10, TimeUnit.SECONDS)
      }
      rounds += 1
      exceptions += exc.get
      val removedSet = removed.toSet
      val d = delivered.asScala.map(_.intValue).toSet
      lost += (0 until N).count(i => !removedSet(i) && !d(i))
      dup += removed.count(d)
    }
    result("T11", if (exceptions > 0 || lost > 0) DEMONSTRATED else if (dup > 0) QUANTIFIED else NOT_REPRODUCED,
      s"$rounds batches x $N tracked nodes invalidated, 8 threads each removePendingInvalidate on ${N / 16} keys concurrently; " +
        s"callbacks=${cbCalls.get}; lost callbacks (not removed, not delivered)=$lost; failed removals (removed but delivered)=$dup; " +
        s"exceptions=$exceptions$exSamples")
  }

  // ==================================================================================================================
  // T12 (R10): interrupted sync waiter strands its action
  // ==================================================================================================================
  def t12(): Unit = {
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val c = dag.prop("c").notTweakable().reads(a)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    watch("root" -> root)
    val hold = new CountDownLatch(1); val started = new CountDownLatch(1)
    val ev = T.executeEvaluateAsync(root) { started.countDown(); hold.await(); 1 }
    started.await(5, TimeUnit.SECONDS)
    val upd = T.addTweaksAsync(root, Seq(tw(sys, key(a), 2)))
    val xOut = new AtomicReference[String]("running")
    val x = thread("t12-sync") {
      try xOut.set(s"returned ${T.evaluate(root, sys.newKey(c))}")
      catch { case e: Throwable => xOut.set(s"threw ${e.getClass.getSimpleName}") }
    }
    val blocked = waitUntil(5000)(T.workQueueSize(root) == 2 && Set(Thread.State.WAITING, Thread.State.TIMED_WAITING)(x.getState))
    val before = qs(root)
    x.interrupt(); x.join(2000)
    hold.countDown()
    val evR = awaitOpt(ev, 5000)
    val updR = awaitOpt(upd, 5000)
    val mid = qs(root)
    val next = T.addTweaksAsync(root, Seq(tw(sys, key(a), 3)))
    val nextR = awaitOpt(next, 5000)
    val wedged = nextR == Right(None)
    result("T12", if (wedged) DEMONSTRATED else NOT_REPRODUCED,
      s"sync evaluate queued behind an update (blocked=$blocked, queue $before); interrupted -> ${xOut.get}; held eval ${show(evR)}; " +
        s"queued update ${show(updR)}; queue then: $mid; next addTweaksAsync ${show(nextR)}; queue now: ${qs(root)}")
  }

  // ==================================================================================================================
  // T14 (F5, F6)
  // ==================================================================================================================
  def t14(variant: String): Unit = variant match {
    case "a" =>
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val c = dag.prop("c").notTweakable().reads(a)
      val sys = new ProbeSystem(dag)
      val root = T.newRoot()
      val kid = T.consistentChild(root, "kid")
      T.addTweaks(kid, tw(sys, key(a), 5))
      T.evaluate(kid, sys.newKey(c))
      val retained = T.rawStack(kid)
      val retainedEval = T.executeEvaluate(kid)(EvaluationContext.scenarioStack)
      T.dispose(kid)
      classifyOnTimeout(("T14a-inner", DEMONSTRATED, "evaluation under the disposed tracker's retained stack never returned (node not completed after the logAndDie in complete())"))
      info(s"kid disposed=${kid.isDisposed}; evaluating a fresh tweakable key (arg 7) under the retained raw stack")
      System.out.flush()
      val r = Try(sys.eval(retained, ProbeDag.key(a, MODULE, 7)))
      info(s"raw-stack eval returned $r")
      val r2 = Try(sys.eval(retainedEval, ProbeDag.key(c, MODULE, 8)))
      info(s"evaluation-stack eval returned $r2")
      result("T14a-inner", if (r.isFailure || r2.isFailure) DEMONSTRATED else NOT_REPRODUCED, s"process survived; raw=$r eval=$r2")
    case "b" =>
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      a.dontTrack = true
      val c = dag.prop("c").notTweakable().reads(a)
      val sys = new ProbeSystem(dag)
      val root = T.newRoot()
      watch("root" -> root)
      val info0 = sys.info(a)
      val v0 = T.evaluate(root, sys.newKey(c)).longValue
      val cNode = sys.pp(c).lastRun.get(key(c))
      val tracked0 = info0.trackForInvalidation()
      val roots0 = Tier1TrackerTT.countRoots(T.ttracks(root), keyFilter(info0))
      info0.markTrackedForInvalidation() // what setExternalConfig(trackForInvalidation = true) does
      val tracked1 = info0.trackForInvalidation()
      val runs0 = sys.pp(c).runsOf(key(c))
      T.addTweaks(root, tw(sys, key(a), 50))
      val v1 = T.evaluate(root, sys.newKey(c)).longValue
      val reran = sys.pp(c).runsOf(key(c)) - runs0
      val oracle = new SequentialEvaluator(Stack.of(Level.of(Tw.instance(key(a), 50)))).eval(key(c))
      // control: a node cached after the flip is invalidated by the next tweak
      T.addTweaks(root, tw(sys, key(a), 60))
      val v2 = T.evaluate(root, sys.newKey(c)).longValue
      result("T14b", if (v1 != oracle) DEMONSTRATED else NOT_REPRODUCED,
        s"c cached with a DONT_TRACK_FOR_INVALIDATION (c=$v0, ttrack roots for a=$roots0, trackForInvalidation=$tracked0); " +
          s"markTrackedForInvalidation -> $tracked1; tweak a:=50: c=$v1 oracle=$oracle reran=$reran cNode.isInvalidCache=${cNode.isInvalidCache}; " +
          s"control after next tweak a:=60: c=$v2 (oracle 60)")
  }

  // ==================================================================================================================
  // T17 (P2): root update latency with a busy concurrent child
  // ==================================================================================================================
  def t17(variant: String): Unit = {
    val chained = variant == "chained"
    val id = if (chained) "T17-chained" else "T17"
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val slow = dag.prop("slow").notTweakable().dontCache()
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    val kid = T.concurrentChild(root, "kid")
    watch("root" -> root, "kid" -> kid)
    val argGen = new AtomicInteger
    def slowKey() = new BodyNode(sys, sys.pp(slow), ProbeDag.key(slow, MODULE, argGen.incrementAndGet()), (v, _) => { Thread.sleep(3); v })
    def measure(n: Int, capMs: Long): Seq[Long] =
      (0 until n).map { i =>
        val t0 = System.nanoTime
        val f = T.addTweaksAsync(root, Seq(tw(sys, key(a), 100 + i + (capMs % 7))))
        awaitOpt(f, capMs) match {
          case Right(Some(_)) => (System.nanoTime - t0) / 1000
          case _              => -1L
        }
      }
    measure(5, 5000) // warm-up
    val idle = measure(20, 5000)
    val idleMed = idle.sorted.apply(10)
    @volatile var stop = false
    val issued = new AtomicInteger
    // poll: a feeder thread keeps <= 2 LP actions outstanding (there are gaps when it is descheduled);
    // chained: each LP action's completion callback issues the next one (2 chains), i.e. one is always queued
    def startFeeder(): Unit =
      if (chained) {
        def chain(): Unit = if (!stop) {
          issued.incrementAndGet()
          T.evaluateKeysLowPriorityAsync(kid, Seq(slowKey())).whenComplete((_, _) => chain())
        }
        chain(); chain()
      } else {
        val outstanding = new AtomicInteger
        thread("t17-feeder") {
          while (!stop) {
            if (outstanding.get < 2) {
              outstanding.incrementAndGet(); issued.incrementAndGet()
              T.evaluateKeysLowPriorityAsync(kid, Seq(slowKey())).whenComplete((_, _) => outstanding.decrementAndGet())
            } else Thread.sleep(0, 200000)
          }
        }
      }
    startFeeder()
    Thread.sleep(200)
    val cap = math.max(2000L, 20 * idleMed / 1000)
    val busy = scala.collection.mutable.ArrayBuffer[Long]()
    var timeouts = 0
    var recovered = ""
    var i = 0
    while (i < 20 && timeouts < 2) {
      val t0 = System.nanoTime
      val f = T.addTweaksAsync(root, Seq(tw(sys, key(a), 1000 + i)))
      awaitOpt(f, cap) match {
        case Right(Some(_)) => busy += (System.nanoTime - t0) / 1000
        case _ =>
          timeouts += 1
          val q = s"root ${qs(root)} kid ${qs(kid)}"
          stop = true
          val t1 = System.nanoTime
          val r = awaitOpt(f, 10000)
          recovered += s" [update $i: not done after $cap ms ($q); feeder stopped -> ${show(r)} ${(System.nanoTime - t1) / 1000000} ms later]"
          Thread.sleep(100)
          stop = false
          if (timeouts < 2) { startFeeder(); Thread.sleep(200) }
      }
      i += 1
    }
    stop = true
    def stats(xs: Seq[Long]) = if (xs.isEmpty) "n=0" else { val s = xs.sorted; f"n=${s.size} median=${s(s.size / 2) / 1000.0}%.2fms max=${s.last / 1000.0}%.2fms" }
    val ratioMax = if (busy.isEmpty) Double.PositiveInfinity else busy.max.toDouble / idleMed
    val falsified = timeouts > 0 || ratioMax > 10
    result(id, if (falsified) DEMONSTRATED else QUANTIFIED,
      s"root addTweaks latency, concurrent child idle: ${stats(idle)}; child busy (${if (chained) "chained from completion callbacks" else "feeder thread"}, LP stream of ~3 ms nodes, " +
        s"${issued.get} LP actions issued): ${stats(busy.toSeq)} " + f"max/idleMedian=$ratioMax%.1f; updates not complete within $cap ms: $timeouts$recovered")
  }

  // ==================================================================================================================
  // T18 (F9, F10)
  // ==================================================================================================================
  def t18(variant: String): Unit = variant match {
    case "a" =>
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val sys = new ProbeSystem(dag)
      val root = T.newRoot()
      watch("root" -> root)
      val evaluator = new AtomicReference[DependencyMultiTrackerEvaluator]()
      val batchThread = new AtomicReference[Thread]()
      val b = T.batchAsync(root) { u =>
        batchThread.set(Thread.currentThread)
        evaluator.set(root.getCurrentEvaluator.orNull) // safeToEvaluate as a side effect, no finishEvaluating
        u.addTweaksImmediate(Seq(tw(sys, key(a), 2)), throwOnDuplicate = false)
      }
      val bR = awaitOpt(b, 5000)
      val next = T.addTweaksAsync(root, Seq(tw(sys, key(a), 3)))
      val nR = awaitOpt(next, 5000)
      val spinning = Option(batchThread.get).map(t => stateOf(t)).getOrElse("-")
      val inWait = threadsIn("waitForEvaluations")
      val q = qs(root)
      // recovery: the missing finishEvaluating
      Option(evaluator.get).foreach(_.finishEvaluating())
      val bR2 = awaitOpt(b, 5000)
      val nR2 = awaitOpt(next, 5000)
      result("T18a", if (bR == Right(None) && nR == Right(None)) DEMONSTRATED else NOT_REPRODUCED,
        s"getCurrentEvaluator (safeToEvaluate side effect) inside a batch, no finishEvaluating: evaluator=${Option(evaluator.get).map(_.getClass.getSimpleName).orNull}; " +
          s"batch addTweaksImmediate ${show(bR)}; next addTweaksAsync ${show(nR)}; batch thread $spinning; threads in waitForEvaluations=${inWait.mkString(",")}; " +
          s"queue $q; after one finishEvaluating(): batch ${show(bR2)} next ${show(nR2)}")
    case "b" =>
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val n = dag.prop("n").notTweakable().reads(a)
      val sys = new ProbeSystem(dag)
      val t1 = T.newRoot()
      val t2 = T.newRoot()
      watch("T1" -> t1, "T2" -> t2)
      val id = s"T18b-ideal${Settings.threadsIdeal}"
      def og = mx.dumpAllThreads(false, false).toSeq.filter(_.getThreadName.startsWith("og")).map(ti => s"${ti.getThreadName}:${ti.getThreadState}")
      // 1. N starts first on a graph thread and parks on gateN (so it holds the only ideal slot when ideal = 1)
      val gateN = new CountDownLatch(1)
      val nThread = new AtomicReference[Thread]()
      val nIn = new CountDownLatch(1)
      val inner = new AtomicReference[String]("not called")
      val f = T.evaluateKeysAsync(t1, Seq(new BodyNode(sys, sys.pp(n), key(n), (v, _) => {
        nThread.set(Thread.currentThread); nIn.countDown(); gateN.await()
        inner.set("blocked in T2.executeEvaluate")
        val r = T.executeEvaluate(t2)(42)
        inner.set(s"returned $r")
        v
      })))
      if (!nIn.await(5, TimeUnit.SECONDS)) {
        result(id, ERROR, s"N never started; graph threads ${og.mkString(" ")}; T1 ${qs(t1)}"); return
      }
      // 2. a plain thread holds T2 in a sync evaluation; 3. an update is queued behind it
      val gateX = new CountDownLatch(1); val xIn = new CountDownLatch(1)
      val x = thread("t18b-X") { T.executeEvaluate(t2) { xIn.countDown(); gateX.await(); 1 } }
      xIn.await(5, TimeUnit.SECONDS)
      val upd = T.addTweaksAsync(t2, Seq(tw(sys, key(a), 2)))
      // 4. N calls T2.executeEvaluate: queued behind the update, N's graph thread parks on the action latch
      gateN.countDown()
      waitUntil(3000)(threadsIn("DependencyTrackerAction.await").nonEmpty)
      val nParked = stateOf(nThread.get)
      // 5. release the plain thread: its completion dispatches the update, which needs a graph thread
      gateX.countDown(); x.join(5000)
      val fR = awaitOpt(f, 5000)
      val uR = awaitOpt(upd, 1000)
      val hang = fR == Right(None)
      val detail = s"threadsIdeal=${Settings.threadsIdeal} (scheduler ideal ${EvaluationContext.scheduler.getIdealThreadCount}): graph node N on T1 calls T2.executeEvaluate (sync) while T2 has an " +
        s"update queued behind a sync evaluation held by a plain thread (N then $nParked); plain thread released; N ${inner.get}; T1 eval ${show(fR)}; " +
        s"T2 update ${show(uR)}; N thread now ${stateOf(nThread.get)}; graph threads ${og.mkString(" ")}; T2 ${qs(t2)}"
      classifyOnTimeout((id, DEMONSTRATED, detail))
      if (hang) println(threads(_ => true, 14))
      result(id, if (hang) DEMONSTRATED else NOT_REPRODUCED, detail)
  }

  // ==================================================================================================================
  // driver
  // ==================================================================================================================
  final case class Run(id: String, probe: String, variant: String, flags: Seq[String], budgetSec: Int)
  val runs: Seq[Run] = Seq(
    Run("T4a", "T4", "a", Nil, 20),
    Run("T4b", "T4", "b", Nil, 20),
    Run("T4c", "T4", "c", Nil, 20),
    Run("T4d", "T4", "d", Nil, 10),
    Run("T5", "T5", "argsHash", Nil, 15),
    Run("T5-npe", "T5", "npe", Nil, 15),
    Run("T6", "T6", "many", Nil, 20),
    Run("T6-control", "T6", "single", Nil, 20),
    Run("T7", "T7", "gc", Seq(s"-D${CacheFixtures.watermarkProperty}=1", "-Doptimus.scheduler.cacheSize=16"), 40),
    Run("T7-manual", "T7", "manual", Seq(s"-D${CacheFixtures.watermarkProperty}=1", "-Doptimus.scheduler.cacheSize=16"), 40),
    Run("T8a", "T8", "a", Seq("-Doptimus.graph.tracking.stallPrintIntervalSecs=2"), 15),
    Run("T8b", "T8", "b", Nil, 15),
    Run("T9", "T9", "-", Nil, 60),
    Run("T10-wrap", "T10", "wrap", Nil, 15),
    Run("T10", "T10", "set", Nil, 60),
    Run("T10-clear", "T10", "clear", Nil, 60),
    Run("T11", "T11", "-", Nil, 90),
    Run("T12", "T12", "-", Nil, 30),
    Run("T14a", "T14", "a", Nil, 20),
    Run("T14b", "T14", "b", Nil, 15),
    Run("T17", "T17", "poll", Nil, 120),
    Run("T17-chained", "T17", "chained", Nil, 120),
    Run("T18a", "T18", "a", Nil, 25),
    Run("T18b-ideal1", "T18", "b", Seq("-Doptimus.gthread.ideal=1"), 20),
    Run("T18b-ideal4", "T18", "b", Seq("-Doptimus.gthread.ideal=4"), 20)
  )

  def child(probe: String, variant: String): Unit = {
    CacheFixtures.init()
    scheduler = EvaluationContext.scheduler
    T.setAutoCleanup(probe == "T7") // timed cleanups off except where cleanup is the subject
    val budget = runs.find(r => r.probe == probe && (r.variant == variant)).map(_.budgetSec).getOrElse(30)
    startWatchdog(probe + variant, budget)
    probe match {
      case "T4"  => t4(variant)
      case "T5"  => t5(variant)
      case "T6"  => t6(variant)
      case "T7"  => t7(variant)
      case "T8"  => t8(variant)
      case "T9"  => t9()
      case "T10" => t10(variant)
      case "T11" => t11()
      case "T12" => t12()
      case "T14" => t14(variant)
      case "T17" => t17(variant)
      case "T18" => t18(variant)
    }
  }

  def parent(only: Set[String]): Unit = {
    val logDir = new java.io.File(Option(System.getenv("T1T_LOGS")).getOrElse("probes/cache/logs-tier1-tracker"))
    logDir.mkdirs()
    val resultsFile = Option(System.getenv("T1T_RESULTS")).getOrElse("probes/cache/RESULTS-tier1-tracker.txt")
    val out = new java.io.PrintWriter(new java.io.FileWriter(resultsFile, true))
    out.println(s"# Tier1Tracker ${new java.util.Date} (JDK ${System.getProperty("java.version")})")
    out.flush()
    for (r <- runs if only.isEmpty || only(r.id) || only(r.probe)) {
      val t0 = System.currentTimeMillis
      val (code, output) = CacheFixtures.runChild(r.flags, "optimus.graph.tracking.Tier1Tracker", Seq("--child", r.probe, r.variant), (r.budgetSec + 30) * 1000L)
      val ms = System.currentTimeMillis - t0
      java.nio.file.Files.writeString(new java.io.File(logDir, s"${r.id}.log").toPath, output)
      val lines = output.linesIterator.filter(_.startsWith("RESULT ")).toSeq
      val extra = s" {child exit=$code ${ms}ms flags=${r.flags.mkString(" ")}}"
      val res =
        if (r.id == "T14a") {
          val logAndDie = output.linesIterator.find(_.contains("logAndDie")).map(_.trim.take(160))
          val recursive = output.contains("logAndDie called recursively")
          val npe = output.linesIterator.find(l => l.contains("NullPointerException")).map(_.trim.take(160))
          val inner = lines.headOption.getOrElse("(no inner result: process died)")
          Seq(s"RESULT T14a ${if (code == 121 || recursive || logAndDie.isDefined) DEMONSTRATED else NOT_REPRODUCED} exit=$code " +
            s"logAndDie=${logAndDie.getOrElse("-")} recursive(F1b swallow)=$recursive npe=${npe.getOrElse("-")}; inner: ${inner.stripPrefix("RESULT ")}$extra")
        } else if (lines.isEmpty) Seq(s"RESULT ${r.id} $ERROR no RESULT line; tail: ${output.linesIterator.toSeq.takeRight(5).mkString(" | ").take(400)}$extra")
        else lines.map(_ + extra)
      res.foreach { l => println(l); out.println(l) }
      out.flush()
    }
    out.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length >= 3 && args(0) == "--child") {
      try child(args(1), args(2))
      catch { case t: Throwable => result(s"${args(1)}-${args(2)}", ERROR, s"threw $t"); t.printStackTrace(System.out) }
      System.out.flush()
      Runtime.getRuntime.halt(0)
    } else {
      parent(args.toSet)
      System.exit(0)
    }
  }
}
