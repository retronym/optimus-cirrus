package optimus.graph {

import java.util.BitSet
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import optimus.graph.ProbeDag._
import optimus.graph.cache.Caches
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.UNodeCache
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerBatchUpdater
import optimus.graph.tracking.{CacheTrackerAccess => T}
import optimus.graph.tracking.ttracks.CacheHunterTT
import optimus.graph.tracking.ttracks.CacheSoakTT
import optimus.platform._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/* =====================================================================================================================
 * H4 retention soak (CACHE_CAMPAIGN.md Tier 2). One long-lived JVM per mode; the live key set is constant so any
 * quantity that keeps growing is retention.
 *
 *   CacheSoak a <durationSec> <seedBase> [sampleSec]   H2-style reuse traffic over a FIXED set of hunter.soakDags (20)
 *                                                      H2 specs (seeds seedBase..), scenario trees built once, cycled
 *                                                      forever by hunter.clients (4) threads, H2Child.check as oracle.
 *   CacheSoak b <durationSec> <seedBase> [sampleSec]   H3-style tracker traffic: 3 roots, each with its own fixed spec
 *                                                      (seed seedBase+r) and tracker plan root / consistent child /
 *                                                      concurrent child / late consistent child under the concurrent
 *                                                      one (created by the newChild action, disposed and recreated at
 *                                                      checkpoints). Driver actions use a fresh RNG seed per phase;
 *                                                      checkpoints run H3's value, invalidation and TTrack oracles.
 *
 * Every sampleSec (300) a SUMMARY line: heap after GC, global/siGlobal size, verifyInvariants warnings/errors,
 * approxNumCacheEntries, CacheIDs.global size, per-root TTrack stats, cleared-ref counters, og.worker threads,
 * _onStalledCallbacks, tracker queue sizes, cumulative oracle counters and finding families.
 * A hang halts with exit 3 (HUtil.hangAndHalt); run-cache-soak.sh restarts a new segment for the remaining time.
 * ===================================================================================================================*/

object SoakCommon {
  import HUtil._
  val t0: Long = System.nanoTime()
  def upSec: Long = (System.nanoTime() - t0) / 1000000000L

  // ---- verifyInvariants logs WARN for "in LRU but absent from table" and throws for the rest; count via logback
  val invWarn = new AtomicLong
  val invErr = new AtomicLong
  val invMsgs = new ConcurrentHashMap[String, AtomicInteger]()
  def installLogCounter(): Unit = {
    import ch.qos.logback.classic.{Logger => LLogger, Level}
    import ch.qos.logback.classic.spi.ILoggingEvent
    import ch.qos.logback.core.AppenderBase
    val lg = org.slf4j.LoggerFactory.getLogger("optimus.graph.cache.NodeCacheWithLRU").asInstanceOf[LLogger]
    val app = new AppenderBase[ILoggingEvent] {
      override def append(e: ILoggingEvent): Unit = {
        if (e.getLevel.isGreaterOrEqual(Level.ERROR)) invErr.incrementAndGet()
        else if (e.getLevel.isGreaterOrEqual(Level.WARN)) invWarn.incrementAndGet()
        if (e.getLevel.isGreaterOrEqual(Level.WARN)) {
          val m = String.valueOf(e.getFormattedMessage).replaceAll("[0-9-]+", "N").take(80)
          invMsgs.computeIfAbsent(m, _ => new AtomicInteger).incrementAndGet()
        }
      }
    }
    app.setContext(lg.getLoggerContext)
    app.start()
    lg.addAppender(app)
    if (lg.getLevel == null || !lg.isWarnEnabled) lg.setLevel(Level.INFO)
    // "onChildCompleted threw exception!" for every unsupported XS+bind (counted as *.xsBindUnsupported) floods the log
    org.slf4j.LoggerFactory.getLogger("optimus.graph.cache.NCSupport").asInstanceOf[LLogger].setLevel(Level.OFF)
  }

  def verify(c: AnyRef, name: String): String = c match {
    case n: NodeCCache =>
      val w0 = invWarn.get; val e0 = invErr.get
      val thrown = try { n.verifyInvariants(); "" } catch { case t: Throwable => ":threw(" + brief(t).take(80) + ")" }
      s"$name+${invWarn.get - w0}w+${invErr.get - e0}e$thrown"
    case null => s"$name=null"
    case o    => s"$name=${o.getClass.getSimpleName}"
  }
  private lazy val sizeM = { val m = Class.forName("optimus.graph.cache.NodeCacheWithLRU").getDeclaredMethod("getSizeIndicative", java.lang.Boolean.TYPE); m.setAccessible(true); m }
  def size(c: AnyRef): Int = if (c == null) -1 else sizeM.invoke(c, java.lang.Boolean.TRUE).asInstanceOf[Int]

  private lazy val cacheIdsGlobal: java.util.Map[_, _] = {
    val mod = Class.forName("optimus.graph.CacheIDs$").getField("MODULE$").get(null)
    val f = mod.getClass.getDeclaredFields.find(f => f.getName == "global" || f.getName.endsWith("$$global")).get
    f.setAccessible(true)
    f.get(mod).asInstanceOf[java.util.Map[_, _]]
  }
  def onStalled(): Int = {
    val s = EvaluationContext.scheduler.asInstanceOf[OGScheduler]
    val m = classOf[OGScheduler].getDeclaredMethod("dbgOnStallCallBacksCount"); m.setAccessible(true)
    m.invoke(s).asInstanceOf[Int]
  }
  def heapAfterGC(): (Long, Long) = {
    System.gc(); System.gc()
    val rt = Runtime.getRuntime
    ((rt.totalMemory - rt.freeMemory) >> 20, rt.maxMemory >> 20)
  }
  def ogWorkers: Int = Thread.getAllStackTraces.keySet.asScala.count(t => t.isAlive && t.getName.startsWith("og.worker"))
  def threads: Int = Thread.activeCount()

  /** The global part of a SUMMARY line. */
  def globalPart(quiescent: String): String = {
    val (heap, max) = heapAfterGC()
    val g = UNodeCache.global; val si = UNodeCache.siGlobal
    val gs = size(g); val sis = size(si)
    val inv = verify(g, "global") + "," + verify(si, "siGlobal")
    val adj = try { val m = Class.forName("optimus.graph.cache.NodeCacheWithLRU").getDeclaredMethod("adjustedMaxSize", Integer.TYPE); m.setAccessible(true); m.invoke(g, Integer.valueOf(Settings.cacheSize)) } catch { case t: Throwable => "?" }
    s"quiescent=$quiescent heapMB=$heap/$max global=$gs globalMaxAdjusted=$adj scopedTotal=${optimus.graph.cache.NodeScopedCache.getScopedCachesTotalSize} ${census(g)} siGlobal=$sis approxEntries=${Caches.approxNumCacheEntries} cacheIDs=${cacheIdsGlobal.size}" +
      s" inv=[$inv] invWarnTot=${invWarn.get} invErrTot=${invErr.get} ogWorkers=$ogWorkers threads=$threads onStalled=$onStalled"
  }
  /** Census of the global cache table: entries, invalid (isInvalidCache), done-with-exception, TweakNodes, proxies. */
  def census(c: AnyRef): String = if (c == null) "-" else try {
    val tf = Class.forName("optimus.graph.cache.NodeCacheBase").getDeclaredField("table"); tf.setAccessible(true)
    val ef = Class.forName("optimus.graph.cache.NCEntry")
    val vf = ef.getDeclaredField("value"); vf.setAccessible(true)
    val nf = ef.getDeclaredField("next"); nf.setAccessible(true)
    val t = tf.get(c).asInstanceOf[Array[AnyRef]]
    var n, inv, exc, twk, proxy, other = 0
    val ids = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val sss = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val byType = new java.util.HashMap[String, Integer]()
    for (h <- t) { var e = h; while (e != null) {
      val v = vf.get(e).asInstanceOf[NodeTask]
      if (v != null) {
        n += 1
        if (v.isInvalidCache) inv += 1
        if (v.isDoneWithException) exc += 1
        val ss = v.scenarioStack
        if (ss != null) { ids.put(ss._cacheID, java.lang.Boolean.TRUE); sss.put(ss, java.lang.Boolean.TRUE) }
        val cn = v.getClass.getSimpleName
        if (cn.contains("Tweak")) twk += 1 else if (cn.contains("Proxy")) proxy += 1
      }
      e = nf.get(e)
    } }
    s"census{entries=$n invalid=$inv exc=$exc tweakNodes=$twk proxies=$proxy distinctCacheIDs=${ids.size} distinctSS=${sss.size}}"
  } catch { case t: Throwable => s"census-failed(${t})" }

  def familiesStr: String = families.asScala.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${v.get}" }.mkString(",")
  def cv(k: String): Long = { val x = cov.get(k); if (x == null) 0L else x.get }
}

// ------------------------------------------------------------------------------------------------ A: H2 reuse soak
object SoakA {
  import HUtil._
  import SoakCommon._

  final class Dag(val seed: Long, val spec: GSpec, val m: Mat, val rt: HRt, val ss1: Array[ScenarioStack],
      val ss2: Array[ScenarioStack], val oracles: Array[HOracle], val anyTag: Boolean, val anyIsdProne: Boolean)

  def build(seed: Long, k: Knobs): Dag = {
    val spec = GSpec.generate(seed, h3 = false)
    val m = new Mat(spec, k)
    val rt = new HRt(m, seed)
    val kept = spec.nodes.filter(g => m.keptNode(g.id))
    rt.configure(kept.flatMap(_.level.tws))
    val anyTag = kept.exists(g => g.level.tag != null) && !k.has("tags")
    val anyIsdProne = m.gp.exists(g => g.si && g.isdProne) && !k.has("si")
    val root = CacheFixtures.newRoot()
    val ss1 = new Array[ScenarioStack](spec.nodes.size)
    val ss2 = new Array[ScenarioStack](spec.nodes.size)
    ss1(0) = root; ss2(0) = root
    for (g <- spec.nodes if g.id > 0 && m.keptNode(g.id) && ss1(g.parent) != null) {
      def scen(n: GNode) = rt.sys.scenario(m.level(n.level), n.unordered && !k.has("unordered"))
      def mk(): ScenarioStack =
        if (g.nested && g.parent != 0 && !k.has("nest") && ss1(spec.nodes(g.parent).parent) != null) {
          val par = spec.nodes(g.parent)
          CacheFixtures.child(ss1(par.parent), scen(par).nest(scen(g)))
        } else CacheFixtures.child(ss1(g.parent), scen(g), m.level(g.level).tag)
      try {
        ss1(g.id) = mk()
        ss2(g.id) = if (g.twice && !k.has("twice")) mk() else ss1(g.id)
      } catch { case t: Throwable => inc("buildFail") }
    }
    val oracles = Array.tabulate(spec.nodes.size)(nid => if (m.keptNode(nid)) new HOracle(m, m.nodeLevels(nid), 0, withReads = false) else null)
    for (g <- kept if g.id > 0; t <- g.level.tws if t.prop < m.n) inc("tw.total")
    inc("nodes", kept.size - 1)
    new Dag(seed, spec, m, rt, ss1, ss2, oracles, anyTag, anyIsdProne)
  }

  def run(durationSec: Long, seedBase: Long, sampleSec: Long): Unit = {
    val k = Knobs.fromProps()
    CacheFixtures.init()
    installLogCounter()
    // H2Child.leakDiag resolves keys through the single H2Child.curSys, which races across DAGs here; disable it
    printed.computeIfAbsent("leakdiag", _ => new AtomicInteger).set(1000)
    val nDags = Integer.getInteger("hunter.soakDags", 20)
    val dags = (0 until nDags).map(i => build(seedBase + i, k))
    val nClients = if (k.clients > 0) k.clients else 4
    val wdMs = java.lang.Long.getLong("hunter.wdMs", 60000L)
    println(s"KV soak mode=a seedBase=$seedBase dags=$nDags clients=$nClients ideal=${Settings.idealThreadCountToCount()} cacheSize=${Settings.cacheSize}" +
      s" delayMaxNanos=${java.lang.Long.getLong("probes.delay.maxNanos", 0L)} xsftDefault=${CacheFixtures.xsftEnabledByDefault} wdMs=$wdMs ${k.describe}")
    println(s"KV dags " + dags.map(d => s"s${d.seed}:p${d.m.n}/E${d.m.E}/A${d.m.A}/n${d.spec.nodes.count(g => d.m.keptNode(g.id))}").mkString(" "))
    System.out.flush()
    val ext = new ScheduledThreadPoolExecutor(2, (r: Runnable) => thread("hunter-cancel")(r.run()))
    @volatile var stop = false
    @volatile var pause = false
    val paused = new AtomicInteger
    val clientTick = Array.fill(nClients)(new AtomicLong(System.nanoTime()))
    val clients = (0 until nClients).map { c =>
      thread(s"hunter-client-$c") {
        CacheFixtures.init()
        val r = new Random(seedBase * 31 + c * 7919L + System.nanoTime())
        var i = 0L
        while (!stop) {
          if (pause) {
            paused.incrementAndGet()
            while (pause && !stop) { Thread.sleep(5); clientTick(c).set(System.nanoTime()) }
            paused.decrementAndGet()
          }
          val d = dags(r.nextInt(dags.size))
          val spec = d.spec; val m = d.m
          val nid = r.nextInt(spec.nodes.size)
          val np = spec.props.size
          val pr = if (r.nextDouble() < 0.6) np / 2 + r.nextInt(np - np / 2) else r.nextInt(np)
          val e = r.nextInt(spec.E + 1) - 1
          val a = r.nextInt(spec.A)
          val cancel = r.nextInt(10) == 0 && !k.has("cancel")
          val delayUs = r.nextInt(3000)
          val v2 = r.nextBoolean()
          if (pr < m.n && m.keptNode(nid) && d.ss1(nid) != null) {
            val ss = if (v2) d.ss2(nid) else d.ss1(nid)
            val key = new Key(m.props(pr), e, a)
            val ssr = if (cancel) ss.withChildCancellationScope() else ss
            if (cancel) { inc("req.cancelScope"); ext.schedule((() => ssr.cancelScope.cancel("hunter")): Runnable, delayUs, TimeUnit.MICROSECONDS) }
            H2Child.curSys = d.rt.sys
            val res: Either[Throwable, java.lang.Long] = try Right(java.lang.Long.valueOf(d.rt.sys.eval(ssr, key))) catch { case t: Throwable => Left(t) }
            inc("req")
            try H2Child.check(m, spec, d.oracles(nid), nid, key, res, cancel, cancel && ssr.cancelScope.isCancelled, (i / 1000).toInt, c, (i % 1000).toInt,
              d.anyTag, d.anyIsdProne, ssr)
            catch { case t: Throwable => finding("HARNESS", t.getClass.getSimpleName, brief(t)) }
          }
          i += 1
          clientTick(c).set(System.nanoTime())
        }
      }
    }
    clients.foreach(_.start())
    val wd = thread("soak-watchdog") {
      while (!stop) {
        Thread.sleep(500)
        val now = System.nanoTime()
        for (c <- 0 until nClients) {
          val idle = (now - clientTick(c).get) / 1000000
          if (idle > wdMs) hangAndHalt(s"client $c no progress for $idle ms")
        }
      }
    }
    wd.start()
    val oracleKeys = Seq("req", "req.value", "req.isd", "req.cancelScope", "req.cancelled", "req.xsBindUnsupported")
    def sample(n: Int): Unit = {
      pause = true
      val tp = System.nanoTime()
      while (paused.get < nClients && !stop && System.nanoTime() - tp < 60000000000L) Thread.sleep(5)
      val h5 = settle(10000)
      val q = if (paused.get >= nClients || stop) (if (h5.isEmpty) "yes" else "clientsPaused/sched:" + h5.mkString(",").take(200)) else "NO(clients not paused)"
      val g = try globalPart(q) finally pause = false
      println(s"SUMMARY mode=a n=$n upSec=$upSec $g ttrack=- queues=- oracle=[${oracleKeys.map(x => s"$x=${cv(x)}").mkString(" ")} findings=${findings.get}] families=[$familiesStr]")
      System.out.flush()
    }
    sample(0)
    val end = System.nanoTime() + durationSec * 1000000000L
    var n = 1
    var next = System.nanoTime() + sampleSec * 1000000000L
    while (System.nanoTime() < end) {
      Thread.sleep(200)
      if (System.nanoTime() >= next) { sample(n); n += 1; next += sampleSec * 1000000000L }
    }
    stop = true
    clients.foreach(_.join(wdMs))
    sample(n)
    println(s"KV cov $covLine")
    familyLines()
    println("SOAK-END")
    System.out.flush()
  }
}

// ------------------------------------------------------------------------------------------------ B: H3 tracker soak
/** One root's H3 instance, adapted from H3Child.run: fixed spec and tracker plan, unbounded phases. */
final class SoakBInst(val rootIdx: Int, val seed: Long, k: Knobs, clientsCap: Int) {
  import HUtil._
  import GSpec._
  import H3Child.{TI, TState, onDone}

  @volatile var lastTick: Long = System.nanoTime()
  @volatile var tickWhat: String = "start"
  def tick(what: String): Unit = { lastTick = System.nanoTime(); tickWhat = what }

  val spec0: GSpec = GSpec.generate(seed, h3 = true)
  // root / consistent child / concurrent child / late consistent child under the concurrent child (the D18 shape)
  val plan = Vector(new GTracker(0, -1, false, false, 0), new GTracker(1, 0, false, false, 1), new GTracker(2, 0, true, false, 1),
    new GTracker(3, 2, false, true, 2))
  val spec = new GSpec(spec0.seed, spec0.props, spec0.E, spec0.A, spec0.nodes, plan, math.min(spec0.clients, clientsCap), tagsOn = rootIdx == 0)
  val m = new Mat(spec, k)
  val rt = new HRt(m, seed)
  rt.configure(Nil)
  val tagsOn: Boolean = spec.tagsOn && !k.has("tags")
  val anyIsdProne: Boolean = m.gp.exists(g => g.si && g.isdProne) && !k.has("si")
  val tweakables: Vector[Int] = m.gp.filter(_.tweakable).map(_.id)
  val wm: Int = Settings.ttrackClearedNodeRefWatermark
  val tis: Array[TI] = {
    val a = new Array[TI](plan.size)
    for (g <- plan) a(g.id) = new TI(g, if (g.parent < 0) null else a(g.parent))
    a
  }
  val listenerToTi = new ConcurrentHashMap[AnyRef, TI]()
  val mainScheduler: OGScheduler = EvaluationContext.scheduler.asInstanceOf[OGScheduler]
  @volatile var lateCreatedPhase = -1
  @volatile var curPhase = 0
  def create(ti: TI): Unit = {
    ti.created = true
    ti.t =
      if (ti.parent == null) T.newRoot()
      else if (ti.g.concurrent) T.concurrentChild(ti.parent.t, s"r${rootIdx}t${ti.id}")
      else T.consistentChild(ti.parent.t, s"r${rootIdx}t${ti.id}")
    ti.qlock = if (ti.parent == null || ti.g.concurrent) new Object else ti.parent.qlock
    listenerToTi.put(T.tweakableTracker(ti.t), ti)
    listenerToTi.put(ti.t.scenarioStack.tweakableListener, ti)
    ti.alive = true
    if (ti.g.late) lateCreatedPhase = curPhase
    inc(if (ti.parent == null) "trk.root" else if (ti.g.concurrent) "trk.concurrent" else if (ti.g.late) "trk.late" else "trk.consistent")
  }
  for (ti <- tis if !ti.g.late) create(ti)

  val recorded = new ConcurrentHashMap[java.lang.Long, NodeTask]()
  rt.sys.runHook = (n: ProbePropertyNode) => {
    val ss = n.scenarioStack()
    if (ss != null) {
      val l = ss.tweakableListener
      var ti = listenerToTi.get(l)
      if (ti == null && l != null) ti = listenerToTi.get(l.underlyingListener)
      if (ti != null && ti.alive) recorded.put(java.lang.Long.valueOf((ti.id.toLong << 32) | m.keyIdx(n.dkey)), n)
    }
  }
  val ext = new ScheduledThreadPoolExecutor(2, (r: Runnable) => thread("hunter-cancel")(r.run()))
  val phaseLog = new java.util.concurrent.ConcurrentLinkedQueue[String]()
  val phaseQueues = ConcurrentHashMap.newKeySet[AnyRef]()
  val phaseFeatures = ConcurrentHashMap.newKeySet[String]()
  def log(s: String): Unit = phaseLog.add(s)

  def dump(): String = {
    val sb = new StringBuilder(s"TRACKERS root$rootIdx seed=$seed phase=$curPhase")
    for (ti <- tis if ti.alive) {
      val wq = T.workQueueSize(ti.t); val lp = T.lowPriorityQueueSize(ti.t); val inf = T.inFlightWork(ti.t).size; val up = T.isUpdating(ti.t)
      sb.append(s"\n  t${ti.id} wq=$wq lp=$lp inFlight=$inf updating=$up submitted=${ti.submitted.get} completed=${ti.completed.get} state=${T.queueState(ti.t)}")
      if ((wq > 0 || lp > 0) && inf == 0 && !up) sb.append(" WEDGE")
      for (a <- T.inFlightWork(ti.t)) {
        sb.append(s"\n    inFlight: ${a.getClass.getName} ${String.valueOf(a).take(300)}")
        try {
          val mth = a.getClass.getMethods.find(m => m.getName == "nodes" && m.getParameterCount == 0)
          mth.foreach { mm =>
            val ns = mm.invoke(a).asInstanceOf[scala.collection.Seq[NodeTask]]
            val allDone = ns.forall(n => n.isDone || n.isInvalidCache)
            sb.append(s"\n      nodes: ${ns.take(8).map(n => s"$n:${n.stateAsString}:cancelled=${n.scenarioStack.cancelScope.isCancelled}").mkString(" ")} allDoneOrInvalid=$allDone")
            if (allDone) sb.append(" COUNTER_LOST")
          }
        } catch { case t: Throwable => sb.append(s" (nodes: $t)") }
      }
    }
    try {
      val sch = mainScheduler
      def sf(n: String) = { val x = classOf[OGScheduler].getDeclaredField(n); x.setAccessible(true); x.get(sch) }
      val qq = sf("_qqueue")
      def qf(n: String) = { val x = qq.getClass.getDeclaredField(n); x.setAccessible(true); x.get(qq) }
      val arr = qf("_q").asInstanceOf[Array[NodeTask]]; val top = qf("_top").asInstanceOf[Int]; val base = qf("_base").asInstanceOf[Int]
      val g = (base until top).take(arr.length).map(i => arr(i & (arr.length - 1))).filter(t => t != null && (t ne NodeTask.nullTask))
      val pq = sf("_queue").asInstanceOf[java.util.PriorityQueue[NodeTask]].asScala.toSeq
      def cnt(n: String) = sf(n).asInstanceOf[optimus.core.IndexedArrayList[_]].count
      sb.append(s"\n  scheduler: qqueue=${g.size} queue=${pq.size} waiters=${cnt("_waiters")} idle=${cnt("_idlePool")} workers=${cnt("_workers")} externallyBlocked=${cnt("_externallyBlocked")} badState=${sch.schedulerInBadState()}")
      if (g.exists(!_.isDoneOrNotRunnable) || pq.exists(!_.isDoneOrNotRunnable)) sb.append(s" STRANDED_IN_GLOBAL_QUEUE: ${(g ++ pq).take(6).map(t => s"$t:${t.stateAsString}").mkString(" ")}")
    } catch { case t: Throwable => sb.append(s"\n  scheduler dump failed $t") }
    sb.append("\nPHASE-LOG ").append(phaseLog.asScala.mkString(" | ").take(3000))
    sb.toString
  }

  val poisoned = ConcurrentHashMap.newKeySet[Integer]()
  def isPoisoned(ti: TI): Boolean = ti.path.exists(a => poisoned.contains(a.id))
  def updateFailed(ti: TI, what: String, e: Throwable): Unit = {
    poisoned.add(ti.id)
    val fam = if (String.valueOf(e).contains("EventCauseInInvalidState") && HUtil.chain(e).exists(_.getStackTrace.exists(_.getMethodName.contains("cancelAndRescheduleLowPriorityActions")))) "R14(EventCause)"
      else e.getClass.getSimpleName
    finding("UPDATE_FAILED", fam, s"root$rootIdx t${ti.id} $what ${brief(e)}")
  }
  val oracleCache = new ConcurrentHashMap[String, HOracle]()
  def oracleFor(ti: TI, combo: Seq[Int]): HOracle = {
    val key = ti.path.map(_.id).mkString(",") + "@" + combo.mkString(",")
    var o = oracleCache.get(key)
    if (o == null) {
      if (oracleCache.size > 400) oracleCache.clear()
      o = new HOracle(m, ti.path.zip(combo).map { case (a, v) => a.level(m, v) }.toArray, 0, withReads = false)
      oracleCache.put(key, o)
    }
    o
  }
  def suspects(ti: TI, key: Key, sysIsd: Boolean, isdExpected: Boolean, cancel: Boolean, nCombo: Long): List[String] = {
    val cl = m.closure(key.prop.id, ti.path.flatMap(_.states.last.tws.map(_._2)))
    val kinds = m.kindsIn(cl)
    val s = mutable.ListBuffer[String]()
    if (kinds("TAG") && tagsOn) s += "F8"
    if (sysIsd && !isdExpected && anyIsdProne) s += "D4"
    if (kinds("XS")) s += "XS"
    if (cancel) s += "CANCEL"
    if (nCombo > 1) s += "RACING_UPDATE"
    if (ti.path.exists(_.g.concurrent)) s += "CONC_CHILD"
    if (ti.g.late) s += "LATE_CHILD"
    s.toList
  }
  val known = Set("F8", "D4", "XS")
  def checkEval(ti: TI, key: Key, lo: Seq[Int], hi: Seq[Int], res: Either[Throwable, java.lang.Long], how: String,
      cancel: Boolean, wasCancelled: Boolean): Unit = {
    if (isPoisoned(ti)) { inc("eval.skippedPoisoned"); return }
    if (lo.size != ti.path.size || hi.size != ti.path.size) { inc("eval.skippedRebased"); return }
    inc("eval.checked")
    val ranges = lo.zip(hi).map { case (a, b) => a to math.max(a, b) }
    val nCombo = ranges.map(_.size.toLong).product
    if (nCombo > 48) { inc("eval.ambiguous"); return }
    val combos = ranges.foldLeft(Seq(Seq.empty[Int]))((acc, rg) => for (c <- acc; v <- rg) yield c :+ v)
    val ex = try combos.map(c => oracleFor(ti, c).eval(key)) catch { case _: IndexOutOfBoundsException => inc("eval.skippedRebased"); return }
    def report(what: String, sysV: String, sysIsd: Boolean): Unit = {
      val sus = suspects(ti, key, sysIsd, ex.exists(_.isd), cancel, nCombo)
      val fam = sus.find(known).getOrElse("UNEXPLAINED")
      finding("ORACLE", fam, s"$what via=$how root$rootIdx tracker=t${ti.id} path=${ti.path.map(_.id).mkString("/")} key=$key system=$sysV " +
        s"oracle=${ex.map(r => if (r.isd) "ISD" else r.v.toString).distinct.mkString("|")} versions=${lo.mkString(",")}..${hi.mkString(",")} suspects=${sus.mkString(",")}")
    }
    res match {
      case Right(v) =>
        inc("eval.value")
        if (!ex.exists(r => !r.isd && r.v == v.longValue)) report(if (ex.forall(_.isd)) "value-where-ISD" else "wrong-value", v.toString, false)
      case Left(t) =>
        if (t eq InvalidatedBeforeRead) inc("eval.invalidatedBeforeRead")
        else if (wasCancelled || isCancel(t)) inc("eval.cancelled")
        else if (isISD(t)) { inc("eval.isd"); if (!ex.exists(_.isd)) report("ISD-where-value", "ISD", true) }
        else if (chain(t).exists(x => x.isInstanceOf[ClassCastException] && String.valueOf(x.getMessage).contains("NodeInputMapValue"))) finding("EXC", "TAG_DOUBLE_WRAP(F18)", s"via=$how root$rootIdx tracker=t${ti.id} key=$key ${brief(t)}", 3)
        else if (t.isInstanceOf[NotDoneMarker]) finding("NOT_DONE_IN_CALLBACK", how, s"root$rootIdx tracker=t${ti.id} key=$key node=${t.getMessage}")
        else if (String.valueOf(t.getMessage).contains("in cross-scenario node") && String.valueOf(t.getMessage).contains("is not supported")) inc("eval.xsBindUnsupported")
        else finding("EXC", t.getClass.getSimpleName, s"via=$how root$rootIdx tracker=t${ti.id} key=$key ${brief(t)}")
    }
  }
  def nodeRes(n: AnyRef): Either[Throwable, java.lang.Long] = {
    val nt = n.asInstanceOf[NodeTask]
    if (!nt.isDone && nt.isInvalidCache) Left(InvalidatedBeforeRead)
    else if (!nt.isDone) {
      val st0 = nt.stateAsString
      val t0 = System.nanoTime()
      while (!nt.isDone && System.nanoTime() - t0 < 2000000000L) Thread.sleep(1)
      Left(new NotDoneMarker(s"${nt.getClass.getSimpleName} state=$st0 invalid=${nt.isInvalidCache} -> ${if (nt.isDone) "done later" else "still running after 2s"}"))
    } else if (nt.isDoneWithException) Left(nt.exception) else Right(nt.resultObject.asInstanceOf[java.lang.Long])
  }
  def randomKeys(r: Random): Seq[Key] = (0 until 1 + r.nextInt(4)).map { _ =>
    val pr = if (r.nextDouble() < 0.6) m.n / 2 + r.nextInt(m.n - m.n / 2) else r.nextInt(m.n)
    new Key(m.props(pr), r.nextInt(m.E + 1) - 1, r.nextInt(m.A))
  }
  def window(ti: TI): Seq[Int] = ti.path.map(_.completed.get)
  def windowHi(ti: TI): Seq[Int] = ti.path.map(_.submitted.get)
  def bump(ti: TI, f: TState => TState, tws: Seq[GTw], tag: Boolean): Int = {
    ti.states = ti.states :+ f(ti.states.last)
    val j = ti.states.size - 1
    ti.submitted.set(j)
    ti.synchronized { tws.foreach(t => if (t.inst) ti.touchedKeys.set(m.keyIdx(t.prop, t.ent, t.arg)) else ti.touchedProps.set(t.prop)) }
    phaseQueues.add(ti.qlock)
    inc("tw.total", tws.size)
    if (tag) inc("upd.tag")
    j
  }
  def done(ti: TI, j: Int): Unit = ti.completed.accumulateAndGet(j, (a: Int, b: Int) => math.max(a, b))
  def genTweaks(r: Random): Seq[GTw] = {
    val used = mutable.Set[Long]()
    (0 until 1 + r.nextInt(3)).flatMap(_ => Option(GSpec.genTw(r, m.gp, tweakables, m.E, m.A, used)))
  }
  def tweaksOf(tws: Seq[GTw]): Seq[Tweak] = tws.map(g => rt.sys.tweak(m.tw(g)))

  val futures = new java.util.concurrent.ConcurrentLinkedQueue[CompletableFuture[_]]()
  def driverPhase(ti: TI, phase: Int): Unit = {
    val r = new Random(seed * 1000003L + ti.id * 7919L + phase * 104729L)
    for (_ <- 0 until 1 + r.nextInt(3) if ti.alive) {
      val x = r.nextInt(100)
      tick(s"phase $phase t${ti.id} action $x")
      try {
        if (x < 32) {
          val sync = x < 18
          val tws = genTweaks(r)
          if (tws.nonEmpty) ti.qlock.synchronized {
            val tweaks = tweaksOf(tws)
            val j = bump(ti, _.withTws(m, tws), tws, tag = false)
            log(s"t${ti.id} add${if (sync) "" else "Async"} v$j [${tws.mkString(", ")}]")
            inc(if (sync) "upd.addSync" else "upd.addAsync")
            if (sync) { try T.addTweaks(ti.t, tweaks: _*) catch { case e: Throwable => updateFailed(ti, "addTweaks", e) }; done(ti, j) }
            else
              try futures.add(onDone(T.addTweaksAsync(ti.t, tweaks)) { (_, e) => done(ti, j); if (e != null) updateFailed(ti, "addTweaksAsync", e) })
              catch { case e: Throwable => updateFailed(ti, "addTweaksAsync(submit)", e); done(ti, j) }
          }
        } else if (x < 42) {
          val sync = r.nextBoolean()
          ti.qlock.synchronized {
            val kids = tis.filter(c => c.alive && (c.parent eq ti) && !c.g.concurrent).filter(_ => r.nextBoolean())
            val parts = (ti +: kids.toSeq).map(c => (c, genTweaks(r)))
            val tag = if (tagsOn && r.nextInt(3) == 0) Some(java.lang.Long.valueOf(1 + r.nextInt(5))) else None
            val js = parts.map { case (c, tws) =>
              (c, bump(c, s => { val s2 = s.withTws(m, tws); if ((c eq ti) && tag.isDefined) s2.withTag(tag.get) else s2 }, tws, (c eq ti) && tag.isDefined))
            }
            val prepared = parts.map { case (c, tws) => (c, tweaksOf(tws)) }
            log(s"t${ti.id} batch${if (sync) "" else "Async"} ${parts.map { case (c, tws) => s"t${c.id}:[${tws.mkString(", ")}]" }.mkString(" ")}${tag.map(" tag=" + _).getOrElse("")}")
            inc(if (sync) "upd.batchSync" else "upd.batchAsync")
            phaseFeatures.add("batch")
            val f: DependencyTrackerBatchUpdater => Unit = u => {
              prepared.foreach { case (c, tw) => if (tw.nonEmpty) u.updaterFor(c.t).addTweaksImmediate(tw, throwOnDuplicate = false) }
              tag.foreach(v => u.setPluginTag(ProbeTagKey, v))
            }
            if (sync) { try T.batch(ti.t)(f) catch { case e: Throwable => js.foreach { case (c, _) => poisoned.add(c.id) }; updateFailed(ti, "batch", e) }; js.foreach { case (c, j) => done(c, j) } }
            else
              try futures.add(onDone(T.batchAsync(ti.t)(f)) { (_, e) => js.foreach { case (c, j) => done(c, j) }; if (e != null) updateFailed(ti, "batchAsync", e) })
              catch { case e: Throwable => js.foreach { case (c, j) => poisoned.add(c.id); done(c, j) }; updateFailed(ti, "batchAsync(submit)", e) }
          }
        } else if (x < 52) {
          inc("eval.driverSync")
          T.evaluateKeys(ti.t, randomKeys(r).map(rt.sys.newKey(_)))
        } else if (x < 72) {
          val lp = x >= 62 && !k.has("lp")
          val keys = randomKeys(r)
          val lo = window(ti)
          inc(if (lp) "eval.driverLowPri" else "eval.driverAsync")
          if (lp) phaseFeatures.add("lowPriority")
          val f = if (lp) T.evaluateKeysLowPriorityAsync(ti.t, keys.map(rt.sys.newKey(_))) else T.evaluateKeysAsync(ti.t, keys.map(rt.sys.newKey(_)))
          futures.add(onDone(f) { (res, e) =>
            val hi = windowHi(ti)
            if (e != null) { if (!isCancel(e)) finding("ACTION_FAILED", if (lp) "evalLowPri" else "evalAsync", brief(e)) else inc("eval.cancelled") }
            else keys.zip(res._1).foreach { case (kk, n) => checkEval(ti, kk, lo, hi, nodeRes(n), if (lp) "driverLowPri" else "driverAsync", false, false) }
          })
        } else if (x < 80) {
          tis.find(c => !c.created && (c.parent eq ti)).foreach { c =>
            ti.qlock.synchronized { create(c) }
            log(s"t${ti.id} newChildScenario t${c.id}")
            phaseFeatures.add("newChild")
            inc("upd.newChildScenario")
          }
        } else if (x < 88) {
          if (tagsOn) ti.qlock.synchronized {
            val v = java.lang.Long.valueOf(1 + r.nextInt(5))
            val j = bump(ti, _.withTag(v), Nil, tag = true)
            log(s"t${ti.id} setPluginTag v$j $v")
            phaseFeatures.add("tag")
            try ti.t.setPluginTag(ProbeTagKey, v) catch { case e: Throwable => updateFailed(ti, "setPluginTag", e) }
            done(ti, j)
          }
        } else {
          val keys = randomKeys(r)
          val delay = r.nextInt(2000)
          val lo = window(ti)
          inc("eval.cancellable")
          phaseFeatures.add("cancelEval")
          val f = T.executeEvaluateAsync(ti.t) {
            val ss = T.evaluationStack(ti.t).withChildCancellationScope()
            val cs = ss.cancelScope
            ext.schedule((() => cs.cancel("hunter")): Runnable, delay, TimeUnit.MICROSECONDS)
            val out = keys.map(kk => try Right(java.lang.Long.valueOf(rt.sys.eval(ss, kk))) catch { case t: Throwable => Left(t) })
            (out, cs.isCancelled)
          }
          futures.add(onDone(f) { (res, e) =>
            val hi = windowHi(ti)
            if (e != null) { if (!isCancel(e)) finding("ACTION_FAILED", "cancellableEval", brief(e)) }
            else {
              if (res._2) inc("eval.scopeCancelled")
              keys.zip(res._1).foreach { case (kk, v) => checkEval(ti, kk, lo, hi, v, "cancellable", true, res._2) }
            }
          })
        }
      } catch {
        case t: Throwable =>
          val fam = if (String.valueOf(t).contains("EventCauseInInvalidState") && chain(t).exists(_.getStackTrace.exists(_.getMethodName.contains("cancelAndRescheduleLowPriorityActions")))) "R14(EventCause)" else t.getClass.getSimpleName
          finding("ACTION_FAILED", fam, s"root$rootIdx t${ti.id} action=$x ${brief(t)}")
      }
    }
  }

  // ---- clients
  val nClients: Int = spec.clients
  @volatile var pause = false
  @volatile var stop = false
  val paused = new AtomicInteger
  val clients: Seq[Thread] = (0 until nClients).map { c =>
    thread(s"hunter-client-r$rootIdx-$c") {
      CacheFixtures.init()
      val r = new Random(seed * 17 + c + System.nanoTime())
      var i = 0
      while (!stop) {
        if (pause) {
          paused.incrementAndGet()
          while (pause && !stop) Thread.sleep(1)
          paused.decrementAndGet()
        } else {
          val alive = tis.filter(_.alive)
          val ti = alive(r.nextInt(alive.length))
          val keys = randomKeys(r)
          val how = { val h = r.nextInt(3); if (h == 2 && k.has("lp")) 1 else h }
          val lo = window(ti)
          val res: Seq[Either[Throwable, java.lang.Long]] =
            try how match {
                case 0 => keys.map(kk => try Right(T.evaluate(ti.t, rt.sys.newKey(kk))) catch { case t: Throwable => Left(t) })
                case 1 => T.await(T.evaluateKeysAsync(ti.t, keys.map(rt.sys.newKey(_))).thenApply(_._1.map(nodeRes)), 3600000)
                case _ => T.await(T.evaluateKeysLowPriorityAsync(ti.t, keys.map(rt.sys.newKey(_))).thenApply(_._1.map(nodeRes)), 3600000)
              } catch { case t: Throwable => keys.map(_ => Left(t)) }
          val hi = windowHi(ti)
          inc(Seq("eval.clientSync", "eval.clientAsync", "eval.clientLowPri")(how))
          keys.zip(res).foreach { case (kk, v) => checkEval(ti, kk, lo, hi, v, Seq("clientSync", "clientAsync", "clientLowPri")(how), false, false) }
          i += 1
          tick(s"client $c req $i")
        }
      }
    }
  }

  // ---- checkpoint (H3's, plus rebase / recovery / late-child disposal so the model stays bounded)
  final class Snap(val ti: TI, val keys: BitSet, val props: BitSet)
  var prevSnap = new java.util.IdentityHashMap[NodeTask, Snap]()
  val infoToProp = new java.util.IdentityHashMap[NodeTaskInfo, Integer]()
  for (i <- 0 until m.n) infoToProp.put(rt.info(i), i)
  val entByHash: Map[Int, IndexedSeq[Int]] = (-1 until m.E).groupBy(e => rt.sys.entity(e).hashCode)
  @volatile var maxIdleWaitMs = 0L
  @volatile var maxWqAtPause = 0
  def idle(ti: TI): Boolean = T.workQueueSize(ti.t) == 0 && T.lowPriorityQueueSize(ti.t) == 0 && T.inFlightWork(ti.t).isEmpty && !T.isUpdating(ti.t)
  def checkpoint(phase: Int, wdMs: Long): Unit = {
    tick(s"checkpoint $phase idle")
    val alive = tis.filter(_.alive)
    val wq0 = alive.map(ti => T.workQueueSize(ti.t) + T.lowPriorityQueueSize(ti.t)).sum
    if (wq0 > maxWqAtPause) maxWqAtPause = wq0
    val ti0 = System.nanoTime()
    while (!alive.forall(idle)) {
      if ((System.nanoTime() - ti0) / 1000000 > wdMs) { extraDump = () => dump(); hangAndHalt(s"root$rootIdx queues not idle at checkpoint $phase (stopped draining)") }
      Thread.sleep(2)
    }
    val iw = (System.nanoTime() - ti0) / 1000000
    if (iw > maxIdleWaitMs) maxIdleWaitMs = iw
    for (ti <- alive if ti.completed.get != ti.submitted.get && !isPoisoned(ti))
      finding("VERSION_LAG", "-", s"root$rootIdx t${ti.id} submitted=${ti.submitted.get} completed=${ti.completed.get}")
    tick(s"checkpoint $phase oracles")
    val cur = alive.map(ti => ti -> new HOracle(m, ti.path.map(a => a.level(m, a.states.size - 1)).toArray, 0, withReads = true)).toMap
    val curTT = alive.map(ti => ti -> new HOracle(m, cur(ti).levels, 0, withReads = true, ttMode = true)).toMap
    val feats = phaseFeatures.asScala.toSeq.sorted.mkString(",")
    prevSnap.forEach { (n, s) =>
      if (s.ti.alive) {
        val inv = n.isInvalidCache
        val touched = s.ti.path.exists(a => a.synchronized(a.touchedKeys.intersects(s.keys) || a.touchedProps.intersects(s.props)))
        if (inv && !touched) {
          inc("inv.over")
          finding("OVERINV", "-", s"phase=$phase root$rootIdx node=$n tracker=t${s.ti.id} features=[$feats] log=${phaseLog.asScala.mkString(" | ").take(600)}", 6)
        } else if (!inv && touched) inc("inv.under") else if (inv) inc("inv.expected") else inc("inv.untouchedValid")
      }
    }
    for (ti <- alive) ti.synchronized { ti.touchedKeys.clear(); ti.touchedProps.clear() }
    tick(s"checkpoint $phase nodes")
    val snap = new java.util.IdentityHashMap[NodeTask, Snap]()
    val ttrackTodo = mutable.ArrayBuffer[(NodeTask, TI, Key, BitSet)]()
    recorded.forEach { (kk, n) =>
      val ti = tis(((kk.longValue >>> 32) & 0xffff).toInt)
      val key = m.keyOf((kk.longValue & 0xffffffffL).toInt)
      val g = m.gp(key.prop.id)
      if (ti.alive && !g.si && g.pol != POL_DONTCACHE && !isPoisoned(ti)) {
        if (n.isInvalidCache) inc("chk.invalid")
        else if (!n.isDone) inc("chk.notDone")
        else if (!n.isDoneWithException) {
          inc("chk.valid")
          val ex = cur(ti).eval(key)
          val v = n.resultObject.asInstanceOf[java.lang.Long]
          if (ex.isd || v.longValue != ex.v) {
            val served = try { val sv = T.evaluate(ti.t, rt.sys.newKey(key)); if (ex.isd) "value " + sv else sv.toString } catch { case t: Throwable => if (isISD(t)) "ISD" else brief(t) }
            val servedStale = served != (if (ex.isd) "ISD" else ex.v.toString)
            val sus = suspects(ti, key, false, ex.isd, false, 1) ++ (if (wm <= 1) List("CLEANUP_WM1") else Nil) ++ phaseFeatures.asScala.map("phase:" + _)
            val fam = sus.find(known).getOrElse(if (servedStale) "UNEXPLAINED" else "LATENT")
            if (!servedStale) recorded.remove(kk) // report each latent stale node once (it is no longer what the cache serves)
            finding(if (servedStale) "STALE_SERVED" else "STALE_LATENT", fam,
              s"phase=$phase root$rootIdx tracker=t${ti.id} path=${ti.path.map(_.id).mkString("/")} key=$key cached=$v oracle=${if (ex.isd) "ISD" else ex.v} servedNow=$served " +
                s"suspects=${sus.mkString(",")} log=${phaseLog.asScala.mkString(" | ").take(800)}")
          } else {
            inc("chk.validOk")
            val pb = new BitSet
            var i = ex.reads.nextSetBit(0)
            while (i >= 0) { pb.set(m.keyOf(i).prop.id); i = ex.reads.nextSetBit(i + 1) }
            snap.put(n, new Snap(ti, ex.reads, pb))
            if (!m.kindsIn(m.closure(key.prop.id, Nil)).contains("XS")) ttrackTodo += ((n, ti, key, curTT(ti).eval(key).reads))
          }
        }
      }
    }
    prevSnap = snap
    tick(s"checkpoint $phase ttracks")
    def ttrackPass(): Seq[String] = {
      val byKey = new java.util.HashMap[Integer, List[AnyRef]]()
      val byProp = new java.util.HashMap[Integer, List[AnyRef]]()
      def add(mp: java.util.HashMap[Integer, List[AnyRef]], i: Int, r: AnyRef): Unit = mp.put(i, r :: Option(mp.get(i)).getOrElse(Nil))
      for (ti <- alive) T.ttracks(ti.t).forEach { (tk, root) =>
        val p = infoToProp.get(tk.propertyInfo)
        if (p != null) tk match {
          case n: ProbePropertyNode => add(byKey, m.keyIdx(n.dkey), root.asInstanceOf[AnyRef])
          case h: NodeHash =>
            for (e <- entByHash.getOrElse(h.entityHash, Nil) if h.argsHash >= 0 && h.argsHash < m.A) add(byKey, m.keyIdx(p, e, h.argsHash), root.asInstanceOf[AnyRef])
          case _ => add(byProp, p, root.asInstanceOf[AnyRef])
        }
      }
      val reach = new java.util.IdentityHashMap[AnyRef, java.util.Set[NodeTask]]()
      def reached(root: AnyRef): java.util.Set[NodeTask] = {
        var s = reach.get(root)
        if (s == null) { s = CacheHunterTT.reach(root); reach.put(root, s) }
        s
      }
      val bad = mutable.ArrayBuffer[String]()
      for ((n, ti, key, reads) <- ttrackTodo if !n.isInvalidCache) {
        var i = reads.nextSetBit(0)
        while (i >= 0) {
          inc("ttrack.edgesChecked")
          val rk = m.keyOf(i)
          val roots = Option(byKey.get(i)).getOrElse(Nil) ++ Option(byProp.get(rk.prop.id)).getOrElse(Nil)
          if (roots.isEmpty) bad += s"NOROOT node=$key@t${ti.id} reads=$rk"
          else if (!roots.exists(r => reached(r).contains(n))) bad += s"UNREACHABLE node=$key@t${ti.id} reads=$rk roots=${roots.size}"
          i = reads.nextSetBit(i + 1)
        }
      }
      bad.toSeq
    }
    var bad = ttrackPass()
    if (bad.nonEmpty) { Thread.sleep(300); bad = ttrackPass() }
    inc("ttrack.nodesChecked", ttrackTodo.size)
    for (b <- bad) finding("TTRACK", b.takeWhile(_ != ' ') + (if (wm <= 1) "-wm1" else ""), s"phase=$phase root$rootIdx $b features=[$feats]")
    inc("checkpoints")
    maintain(phase)
  }

  private val maintRng = new Random(seed * 7777777L + 13)
  private val rebaseOn = !java.lang.Boolean.getBoolean("hunter.soakNoRebase")
  private val disposeOn = !java.lang.Boolean.getBoolean("hunter.soakNoDispose")
  /** Keep the model bounded and the trees usable over an hour: rebase versions, recover poisoned subtrees, cycle the late child. */
  def maintain(phase: Int): Unit = {
    tick(s"checkpoint $phase maintain")
    // recover poisoned subtrees: remove all tweaks on the subtree and restart its model from "no tweaks"
    val poisonedNow = poisoned.asScala.toSeq.map(_.intValue).sorted
    if (poisonedNow.nonEmpty) {
      val sub = tis.filter(ti => ti.alive && ti.path.exists(a => poisonedNow.contains(a.id)))
      try {
        for (ti <- sub) {
          T.removeAllTweaks(ti.t)
          val tag = ti.states.last.tag
          if (tag != null) ti.t.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(1L))
          ti.states = Vector(new TState(Vector.empty, if (tag != null) java.lang.Long.valueOf(1L) else null))
          ti.submitted.set(0); ti.completed.set(0)
        }
        poisoned.clear(); inc("recovered")
        val pr = sub.toSet[AnyRef]
        val it = prevSnap.values.iterator; while (it.hasNext) if (pr.contains(it.next().ti)) it.remove()
      } catch { case t: Throwable => finding("RECOVERY_FAILED", t.getClass.getSimpleName, s"root$rootIdx ${brief(t)}") }
      val t0 = System.nanoTime()
      while (!tis.filter(_.alive).forall(idle) && System.nanoTime() - t0 < 10000000000L) Thread.sleep(2)
    }
    // rebase every tracker's version history onto its last state (everything is idle, clients paused, futures drained)
    if (rebaseOn) for (ti <- tis if ti.alive && ti.submitted.get == ti.completed.get) {
      ti.states = Vector(ti.states.last); ti.submitted.set(0); ti.completed.set(0)
    }
    if (rebaseOn) { oracleCache.clear(); resetMemos() }
    // cycle the late child: dispose after >= 5 phases with p = 1/4, the newChild action recreates it
    val late = tis(3)
    if (disposeOn && late.alive && phase - lateCreatedPhase >= 5 && maintRng.nextInt(4) == 0) {
      try {
        T.dispose(late.t)
        inc("trk.lateDisposed")
      } catch { case t: Throwable => finding("DISPOSE_FAILED", t.getClass.getSimpleName, s"root$rootIdx ${brief(t)}") }
      late.alive = false; late.created = false
      listenerToTi.values.removeIf(_ eq late)
      recorded.keySet.removeIf(kk => ((kk.longValue >>> 32) & 0xffff) == 3)
      val it = prevSnap.values.iterator; while (it.hasNext) if (it.next().ti eq late) it.remove()
      late.states = Vector(new TState(Vector.empty, null)); late.submitted.set(0); late.completed.set(0)
      late.t = null
    }
  }

  /** Mat memoises Tw/Rhs/Level by identity of the generator objects; every driver tweak and TState is new, so clear them. */
  private val memoFields = Seq("tws", "rhss", "levels").map(n => T.findField(classOf[Mat], n))
  def resetMemos(): Unit = memoFields.foreach { f => val mp = f.get(m).asInstanceOf[java.util.IdentityHashMap[_, _]]; mp.synchronized(mp.clear()) }

  /** Per-root retention metrics, taken at a checkpoint (idle). */
  @volatile var sampleReq: CompletableFuture[String] = null
  @volatile var release: java.util.concurrent.CountDownLatch = null
  @volatile var atBarrier = false
  def trackerStats(): String = {
    val parts = for (ti <- tis if ti.alive) yield {
      val tt = T.tweakableTracker(ti.t)
      val w = CacheSoakTT.walk(T.ttracks(ti.t).values)
      s"t${ti.id}:roots=${T.ttracks(ti.t).size}/tracks=${w(0)}/edges=${w(1)}/refs=${w(2)}/clearedRefs=${w(3)}/cleared=${tt.refQ.snapshot}"
    }
    s"r$rootIdx{${parts.mkString(" ")} recorded=${recorded.size} prevSnap=${prevSnap.size} oracleCache=${oracleCache.size} maxIdleWaitMs=$maxIdleWaitMs maxWqAtPause=$maxWqAtPause}"
  }
  def queueStats(): String =
    s"r$rootIdx{" + tis.filter(_.alive).map(ti => s"t${ti.id}:wq=${T.workQueueSize(ti.t)}/lp=${T.lowPriorityQueueSize(ti.t)}/inFlight=${T.inFlightWork(ti.t).size}").mkString(" ") + "}"

  def runLoop(wdMs: Long): Unit = {
    clients.foreach(_.start())
    val drivers = Executors.newFixedThreadPool(tis.length, (r: Runnable) => thread(s"hunter-driver-r$rootIdx") { CacheFixtures.init(); r.run() })
    var phase = 0
    while (!stop) {
      curPhase = phase
      phaseLog.clear(); phaseQueues.clear(); phaseFeatures.clear()
      val alive = tis.filter(_.alive)
      val tasks = alive.map(ti => drivers.submit((() => driverPhase(ti, phase)): Runnable))
      tasks.foreach(_.get())
      tick(s"phase $phase futures")
      // bounded: a driver future that never completes is a queue that stopped draining (clients may still tick)
      // After wdMs/3 the clients are paused: if the futures then complete, the queue was starved by foreground
      // (client) work, not wedged (STARVED finding); if they still do not complete by wdMs, it is a hang.
      val tf = System.nanoTime()
      var starvedPause = false
      var f = futures.poll()
      while (f != null) {
        val el = (System.nanoTime() - tf) / 1000000
        val lim = if (starvedPause) wdMs - el else wdMs / 3 - el
        try { f.get(math.max(1L, lim), TimeUnit.MILLISECONDS); f = futures.poll() } catch {
          case _: java.util.concurrent.TimeoutException =>
            if (!starvedPause) {
              starvedPause = true; pause = true
              finding("STARVED_PROBE", "-", s"root$rootIdx phase $phase driver futures not complete after ${wdMs / 3} ms with clients running; pausing clients\n${dump()}", 4)
            } else { extraDump = () => dump(); hangAndHalt(s"root$rootIdx phase $phase driver futures not complete after $wdMs ms, clients paused for ${wdMs * 2 / 3} ms (queue stopped draining)") }
          case _: Throwable => f = futures.poll()
        }
      }
      if (starvedPause) {
        finding("STARVED", "-", s"root$rootIdx phase $phase driver futures completed ${(System.nanoTime() - tf) / 1000000} ms after start once clients were paused (foreground starvation, not a wedge)")
        pause = false
      }
      tick(s"phase $phase pause")
      pause = true
      while (paused.get < nClients) Thread.sleep(1)
      checkpoint(phase, wdMs)
      val req = sampleReq
      if (req != null) {
        sampleReq = null
        val rel = release
        atBarrier = true
        req.complete(try trackerStats() catch { case t: Throwable => s"r$rootIdx{stats failed ${brief(t)}}" })
        if (rel != null) rel.await(180, TimeUnit.SECONDS) // all roots idle at once while the sampler checks the global cache
        atBarrier = false
        tick(s"phase $phase sampled")
      }
      pause = false
      phase += 1
      inc("phases")
      tick(s"phase $phase start")
    }
    clients.foreach(_.join(wdMs))
    drivers.shutdownNow()
  }
}

object SoakB {
  import HUtil._
  import SoakCommon._

  def run(durationSec: Long, seedBase: Long, sampleSec: Long): Unit = {
    val k = Knobs.fromProps()
    CacheFixtures.init()
    installLogCounter()
    val nRoots = Integer.getInteger("hunter.soakRoots", 3)
    val cap = Integer.getInteger("hunter.soakClients", 2)
    val wdMs = java.lang.Long.getLong("hunter.wdMs", 60000L)
    val only = Integer.getInteger("hunter.soakRoot", -1)
    val insts = (0 until nRoots).filter(r => only < 0 || r == only).map(r => new SoakBInst(r, seedBase + r, k, cap))
    println(s"KV soak mode=b seedBase=$seedBase roots=$nRoots ideal=${Settings.idealThreadCountToCount()} cacheSize=${Settings.cacheSize}" +
      s" delayMaxNanos=${java.lang.Long.getLong("probes.delay.maxNanos", 0L)} watermark=${Settings.ttrackClearedNodeRefWatermark} wdMs=$wdMs ${k.describe}")
    println("KV roots " + insts.map(i => s"r${i.rootIdx}:s${i.seed}:p${i.m.n}/E${i.m.E}/A${i.m.A}/clients${i.nClients}").mkString(" "))
    System.out.flush()
    val loops = insts.map(i => thread(s"soak-root-${i.rootIdx}") { CacheFixtures.init(); try i.runLoop(wdMs) catch { case t: Throwable => println(s"ROOT-CRASH r${i.rootIdx} $t"); t.printStackTrace(System.out); Runtime.getRuntime.halt(5) } })
    loops.foreach(_.start())
    @volatile var stopWd = false
    val wd = thread("soak-watchdog") {
      while (!stopWd) {
        Thread.sleep(500)
        for (i <- insts) {
          val idle = (System.nanoTime() - i.lastTick) / 1000000
          if (idle > wdMs && !stopWd && !i.atBarrier) { extraDump = () => i.dump(); hangAndHalt(s"root${i.rootIdx} no progress for $idle ms (last: ${i.tickWhat})") }
        }
      }
    }
    wd.start()
    val oracleKeys = Seq("phases", "checkpoints", "eval.checked", "eval.value", "eval.ambiguous", "eval.skippedPoisoned", "eval.skippedRebased", "chk.valid", "chk.validOk",
      "inv.expected", "inv.over", "inv.under", "inv.untouchedValid", "ttrack.nodesChecked", "ttrack.edgesChecked", "recovered", "trk.late", "trk.lateDisposed")
    def sample(n: Int): Unit = {
      val q = insts.map(_.queueStats()).mkString(" ")
      val rel = new java.util.concurrent.CountDownLatch(1)
      val reqs = insts.map { i => val f = new CompletableFuture[String]; i.release = rel; i.sampleReq = f; f }
      var allIn = true
      val tt = insts.zip(reqs).map { case (i, f) => try f.get(120, TimeUnit.SECONDS) catch { case _: Throwable => allIn = false; s"r${i.rootIdx}{noCheckpointIn120s}" } }.mkString(" ")
      val h5 = settle(10000)
      val g = try globalPart(if (!allIn) "NO(root missing)" else if (h5.isEmpty) "yes" else "rootsIdle/sched:" + h5.mkString(",").take(200))
        finally { rel.countDown(); insts.foreach(i => i.tick("released")) }
      println(s"SUMMARY mode=b n=$n upSec=$upSec $g queues=[$q] ttrack=[$tt] oracle=[${oracleKeys.map(x => s"$x=${cv(x)}").mkString(" ")} findings=${findings.get}] families=[$familiesStr]")
      System.out.flush()
    }
    sample(0)
    val end = System.nanoTime() + durationSec * 1000000000L
    var n = 1
    var next = System.nanoTime() + sampleSec * 1000000000L
    while (System.nanoTime() < end) {
      Thread.sleep(200)
      if (System.nanoTime() >= next) { sample(n); n += 1; next += sampleSec * 1000000000L }
    }
    sample(n)
    stopWd = true
    insts.foreach(_.stop = true)
    println(s"KV cov $covLine")
    familyLines()
    println("SOAK-END")
    System.out.flush()
  }
}

object CacheSoak {
  def main(args: Array[String]): Unit = {
    var code = 0
    val t = HUtil.thread("soak-main") {
      try args.toSeq match {
          case Seq("a", d, s, rest @ _*) => SoakA.run(d.toLong, s.toLong, rest.headOption.map(_.toLong).getOrElse(300L))
          case Seq("b", d, s, rest @ _*) => SoakB.run(d.toLong, s.toLong, rest.headOption.map(_.toLong).getOrElse(300L))
          case _ => System.err.println("usage: CacheSoak a|b <durationSec> <seedBase> [sampleSec]"); code = 2
        } catch { case e: Throwable => println(s"SOAK-CRASH $e"); e.printStackTrace(System.out); code = 5 }
    }
    t.start(); t.join()
    System.out.flush()
    Runtime.getRuntime.halt(code)
  }
}
}

package optimus.graph.tracking.ttracks {
  object CacheSoakTT {
    /** Over the TTrack graph from the given roots: Array(tracks, ptrack edges, TTrackRefs, cleared TTrackRefs). */
    def walk(roots: java.util.Collection[_]): Array[Long] = {
      val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[TTrack, java.lang.Boolean]())
      val st = new java.util.ArrayDeque[TTrack]()
      roots.forEach { r => val t = r.asInstanceOf[TTrack]; if (seen.add(t)) st.push(t) }
      var edges = 0L; var refs = 0L; var cleared = 0L
      while (!st.isEmpty) {
        val t = st.pop()
        var ref = t.nodes
        while (ref != null && (ref ne TTrackRef.Nil) && (ref ne TTrackRef.Invalid)) {
          refs += 1
          if (ref.get == null) cleared += 1
          ref = ref.nref
        }
        val p = t.ptracks
        if (p != null) {
          val sz = p.currentSize
          var i = 0
          while (i < sz) { val c = p.at(i); if (c != null) { edges += 1; if (seen.add(c)) st.push(c) }; i += 1 }
        }
      }
      Array(seen.size.toLong, edges, refs, cleared)
    }
  }
}
