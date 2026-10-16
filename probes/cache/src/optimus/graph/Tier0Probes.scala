package optimus.graph {

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import optimus.config.RuntimeComponents
import optimus.core.TPDMask
import optimus.graph.ProbeDag._
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.UNodeCache
import optimus.graph.tracking.{CacheTrackerAccess => T}
import optimus.graph.tracking.Tier0TrackingAccess
import optimus.platform._
import optimus.platform.dal.EntityResolver
import optimus.platform.storable.EntityImpl
import probes.CacheDelay

import scala.jdk.CollectionConverters._

/** A combinator whose args() is NodeTask.argsEmpty: what NCPolicy.localMatch requires of a LOCAL_CACHE node. */
final class NoArgCombinator(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends Combinator(s, p, k) {
  override def args: Array[AnyRef] = NodeTask.argsEmpty
}

/** W8: RuntimeComponents whose newTopScenario "queries the initial time" (a fresh Instant per call). */
final class W8Components(info: TwkPropertyInfo0[_, _], lookupNanos: Int) extends RuntimeComponents(null) {
  val calls = new AtomicInteger
  val seq = new AtomicLong
  val instantOf = new ConcurrentHashMap[Scenario, Instant]()
  override def createEntityResolver(): EntityResolver = null
  override def createRuntime(): RuntimeEnvironment = null
  override private[optimus] def newTopScenario(resolver: EntityResolver): Scenario = {
    calls.incrementAndGet()
    val t = Instant.ofEpochSecond(1700000000L, seq.incrementAndGet())
    if (lookupNanos > 0) Thread.sleep(0, lookupNanos) // a server time lookup is not instantaneous
    val sc = Scenario(Seq(CacheFixtures.propertyTweak(info, t)))
    instantOf.put(sc, t)
    sc
  }
  override private[optimus] def initialRuntimeScenarioStack(ss: ScenarioStack, initialTime: Instant): ScenarioStack = ss
}

/**
 * Fixture-dependent Tier 0 probes of the cache campaign (CACHE_CAMPAIGN.md): W2 W4 W8 W9 W10 W13.
 * probes/cache/run.sh optimus.graph.Tier0Probes [ids...]   (default: all). One RESULT line per probe.
 */
object Tier0Probes {
  val results = new java.util.ArrayList[String]()
  def result(id: String, outcome: String, detail: String): Unit = {
    val l = s"RESULT $id $outcome $detail"
    results.add(l)
    println(l)
    System.out.flush()
  }
  def info(s: String): Unit = { println("     " + s); System.out.flush() }

  def oracle(st: Stack, k: Key): Long = new SequentialEvaluator(st).eval(k)

  // ---- helpers --------------------------------------------------------------------------------------------------
  def field(o: AnyRef, name: String): AnyRef = T.field(o, name)
  def fieldOf(cls: Class[_], o: AnyRef, name: String): AnyRef = { val f = T.findField(cls, name); f.get(o) }
  def staticField(cls: Class[_], name: String): AnyRef = { val f = cls.getDeclaredField(name); f.setAccessible(true); f.get(null) }

  /** Run f on a fresh thread; None if it did not finish within ms (thread left running, daemon). */
  def withTimeout[A](ms: Long, name: String)(f: => A): Either[String, A] = {
    val r = new AtomicReference[Either[String, A]]()
    val t = new Thread(() => {
      try { CacheFixtures.init(); r.set(Right(f)) }
      catch { case e: Throwable => r.set(Left(s"threw ${e.getClass.getSimpleName}: ${e.getMessage}")) }
    }, name)
    t.setDaemon(true)
    t.start()
    t.join(ms)
    if (t.isAlive) Left(s"TIMEOUT after $ms ms (state ${t.getState} at ${t.getStackTrace.take(4).mkString(" | ")})")
    else r.get
  }

  /** getNode + runAndWait + combineInfo in the current node's context; returns the node getNode handed back. */
  def lookupHere[A](key: PropertyNode[A]): Node[A] = {
    val ec = OGSchedulerContext.current()
    val node = ec.scenarioStack.getNode(key, ec)
    ec.runAndWait(node)
    ec.getCurrentNodeTask.combineInfo(node, ec)
    node
  }
  def getNodeOnly[A](ss: ScenarioStack, key: PropertyNode[A]): Node[A] = CacheFixtures.evalIn(ss) {
    val ec = OGSchedulerContext.current()
    ec.scenarioStack.getNode(key, ec)
  }
  def ident(n: AnyRef): String = if (n eq null) "null" else s"${n.getClass.getSimpleName}@${Integer.toHexString(System.identityHashCode(n))}"
  def nodeStr(n: NodeTask): String =
    if (n eq null) "null"
    else s"${ident(n)}(${n.toString} done=${n.isDone} invalid=${n.isInvalidCache}${if (n.isDoneWithResult) " v=" + n.resultObject else ""})"

  // ================================================================================================================
  // W2 (D9): LOCAL_CACHE slot
  // ================================================================================================================
  def w2(): Unit = {
    val parts = new StringBuilder
    var demonstratedB = false
    var demonstratedInv = false
    for (policyName <- Seq("Basic", "XSFT(default)")) {
      val dag = new ProbeDag
      val a = dag.prop("w2A_" + policyName).notTweakable().base(1); a.localCache = true
      val b = dag.prop("w2B_" + policyName).notTweakable().base(2); b.localCache = true
      val sys = new ProbeSystem(dag)
      if (policyName == "Basic") { CacheFixtures.setPolicy(sys.info(a), NCPolicy.Basic); CacheFixtures.setPolicy(sys.info(b), NCPolicy.Basic) }
      val e = 7
      val ka = ProbeDag.key(a, e, 0); val kb = ProbeDag.key(b, e, 0)
      val root = CacheFixtures.newRoot()
      val (na, nb) = CacheFixtures.evalIn(root) {
        val na = lookupHere(new NoArgCombinator(sys, sys.pp(a), ka))
        val nb = lookupHere(new NoArgCombinator(sys, sys.pp(b), kb))
        (na, nb)
      }
      val oa = oracle(Stack.of(), ka); val ob = oracle(Stack.of(), kb)
      val bv = nb.resultObject
      val wrong = (na eq nb) || bv != java.lang.Long.valueOf(ob)
      if (wrong) demonstratedB = true
      parts ++= s"[$policyName policy=${sys.info(a).cachePolicy}] A->${nodeStr(na)} B->${nodeStr(nb)} B.sameNodeAsA=${na eq nb} B.value=$bv oracle(B)=$ob oracle(A)=$oa B.runs=${sys.pp(b).runs.get} slot=${ident(EntityImpl.getLocalCache(sys.entity(e)))}; "
    }

    // part 2: invalidation under a tracker
    def invPart(viaQueue: Boolean): String = {
      val dag = new ProbeDag
      val l = dag.prop("w2L" + viaQueue).base(100)
      val a = dag.prop("w2InvA" + viaQueue).notTweakable().reads(l, 7, 0).base(1); a.localCache = true
      val sys = new ProbeSystem(dag)
      CacheFixtures.setPolicy(sys.info(a), NCPolicy.Basic)
      val ka = ProbeDag.key(a, 7, 0)
      val tr = T.newRoot()
      def look(): Either[String, Node[java.lang.Long]] =
        if (viaQueue)
          withTimeout(5000, "w2-look") { T.executeEvaluate(tr) { CacheFixtures.lookupNode(T.evaluationStack(tr), new NoArgCombinator(sys, sys.pp(a), ka)) } }
        else
          withTimeout(5000, "w2-look-raw") { CacheFixtures.lookupNode(T.rawStack(tr), new NoArgCombinator(sys, sys.pp(a), ka)) }
      val first = look().map(nodeStr).merge
      val n1 = sys.pp(a).lastRun.get(ka)
      val slot1 = EntityImpl.getLocalCache(sys.entity(7))
      T.addTweaks(tr, sys.tweak(Tw.instance(ProbeDag.key(l, 7, 0), 500)))
      val inv = n1 != null && n1.isInvalidCache
      val slot2 = EntityImpl.getLocalCache(sys.entity(7))
      val runs1 = sys.pp(a).runs.get
      val second = look()
      val ora = oracle(Stack.of(Level.of(Tw.instance(ProbeDag.key(l, 7, 0), 500))), ka)
      val secondStr = second match {
        case Right(n) =>
          val stale = (n eq n1) || (n.isDoneWithResult && n.resultObject != java.lang.Long.valueOf(ora))
          if (stale) demonstratedInv = true
          s"returned ${nodeStr(n)} sameAsInvalidated=${n eq n1} value=${if (n.isDoneWithResult) n.resultObject else "-"}"
        case Left(err) => demonstratedInv = true; s"FAILED: $err"
      }
      val getv = withTimeout(5000, "w2-get") {
        if (viaQueue) T.executeEvaluate(tr) { CacheFixtures.eval(T.evaluationStack(tr), new NoArgCombinator(sys, sys.pp(a), ka)) }
        else CacheFixtures.eval(T.rawStack(tr), new NoArgCombinator(sys, sys.pp(a), ka))
      }
      if (getv.isLeft || getv.exists(_ != java.lang.Long.valueOf(ora))) demonstratedInv = true
      val r = s"[tracker ${if (viaQueue) "executeEvaluate+evaluationStack" else "rawStack (fixed cancel scope)"}] first=${first} slotAfterFirst=${ident(slot1)} slotIsA=${slot1 eq n1} afterTweak: A.isInvalidCache=$inv slot=${ident(slot2)} slotIsA=${slot2 eq n1}; second lookup: $secondStr lookupAndGet=${getv.merge} A.runs ${runs1}->${sys.pp(a).runs.get} oracle=$ora; "
      T.disposeAllRoots()
      r
    }
    parts ++= invPart(viaQueue = true)
    parts ++= invPart(viaQueue = false)
    val outcome = if (demonstratedB || demonstratedInv) "DEMONSTRATED" else "NOT_REPRODUCED"
    result("W2", outcome, s"wrongPropertyHit=$demonstratedB staleOrBrokenAfterInvalidation=$demonstratedInv " + parts.toString)
  }

  // ================================================================================================================
  // W4 (S4 / OPTIMUS-57931): resize desync
  // ================================================================================================================
  final case class W4Count(lruLive: Int, lruRemoved: Int, tableLive: Int, tableRemoved: Int, lruNotInTable: Int,
                           tableNotInLru: Int, tableNotInLruInvalid: Int, sizeField: Int, getSize: Int, verify: String,
                           cumLruNotInTable: Int = 0, inserted: Long = 0) {
    override def toString: String =
      s"inserted=$inserted distinctLruNotInTableSampledDuringRun=$cumLruNotInTable atQuiescence: lruNotInTable=$lruNotInTable tableNotInLru=$tableNotInLru(of which invalid/removed-value=$tableNotInLruInvalid) " +
        s"lruLive=$lruLive lruRemoved=$lruRemoved tableLive=$tableLive tableRemoved=$tableRemoved size=$sizeField getSize()=$getSize verifyInvariants=$verify"
  }

  def w4Count(c: UNodeCache, seen: java.util.Set[AnyRef] = null): W4Count = {
    val gs = c.getSize() // catchupCore under updateLock
    val verify = if (seen != null) "-" else try { c.verifyInvariants(); "ok(LRU-not-in-table only warns)" } catch { case t: Throwable => s"threw ${t.getClass.getSimpleName}: ${t.getMessage.replace('\n', ' ')}" }
    val lock = field(c, "updateLock").asInstanceOf[java.util.concurrent.locks.ReentrantLock]
    val ncEntry = Class.forName("optimus.graph.cache.NCEntry")
    val ncEntryLRU = Class.forName("optimus.graph.cache.NCEntryLRU")
    val fAfter = T.findField(ncEntryLRU, "after")
    val fInLedger = T.findField(ncEntryLRU, "inLedger")
    val fNext = T.findField(ncEntry, "next")
    val fValue = T.findField(ncEntry, "value")
    lock.lock()
    try {
      val header = field(c, "header")
      val table = field(c, "table").asInstanceOf[Array[AnyRef]]
      val size = field(c, "size").asInstanceOf[Int]
      val lru = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]())
      var lruLive = 0; var lruRemoved = 0
      var e = fAfter.get(header)
      while (e ne header) {
        lru.add(e)
        if (fValue.get(e) == null) lruRemoved += 1 else lruLive += 1
        e = fAfter.get(e)
      }
      val tab = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]())
      var tableLive = 0; var tableRemoved = 0; var tNotL = 0; var tNotLInv = 0
      for (head <- table) {
        var x = head
        while (x != null) {
          tab.add(x)
          val v = fValue.get(x).asInstanceOf[NodeTask]
          if (v == null) tableRemoved += 1 else tableLive += 1
          if (!lru.contains(x) && !fInLedger.getBoolean(x)) {
            tNotL += 1
            if (v == null || v.isInvalidCache) tNotLInv += 1
          }
          x = fNext.get(x)
        }
      }
      var lNotT = 0
      lru.forEach(x => if (fValue.get(x) != null && !tab.contains(x)) { lNotT += 1; if (seen != null) seen.add(x) })
      W4Count(lruLive, lruRemoved, tableLive, tableRemoved, lNotT, tNotL, tNotLInv, size, gs, verify)
    } finally lock.unlock()
  }

  def w4Run(run: Int, delayed: Boolean): W4Count = {
    val dag = new ProbeDag
    val x = dag.prop(s"w4X_$run$delayed").notTweakable().base(1)
    val sys = new ProbeSystem(dag)
    CacheFixtures.setPolicy(sys.info(x), NCPolicy.Basic)
    val c = CacheFixtures.privateCache(sys.info(x), 16)
    val root = CacheFixtures.newRoot()
    val stop = new AtomicBoolean(false)
    val inserted = new AtomicLong
    val inserters = new java.util.HashSet[Thread]()
    if (delayed) {
      // Widen the "slow inserter" window: some inserters pause between reading the bucket head and the CAS
      val rnd = new java.util.SplittableRandom(run)
      CacheDelay.onHit("nc.put.head", _ => {
        if (Thread.currentThread().getName.startsWith("w4-ins") || Thread.currentThread().getName.contains("optimus")) {
          val r = rnd.synchronized(rnd.nextInt(100))
          if (r < 5) java.util.concurrent.locks.LockSupport.parkNanos(200000L + r * 100000L)
        }
      })
    }
    for (t <- 0 until 4) {
      val th = new Thread(() => {
        CacheFixtures.init()
        var i = 0
        while (!stop.get) {
          sys.eval(root, ProbeDag.key(x, ProbeDag.MODULE, t * 100000000 + i)); i += 1
        }
        inserted.addAndGet(i)
      }, s"w4-ins-$t")
      inserters.add(th); th.start()
    }
    Thread.sleep(50)
    // LRU-not-in-table entries are evicted again quickly in a 16-entry cache, so sample after every resize (under
    // updateLock, inserters running) and count distinct ones; the leak direction is checked at quiescence below.
    val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]())
    for (_ <- 0 until 200) {
      c.setMaxSize(64); w4Count(c, seen); Thread.sleep(1)
      c.setMaxSize(16); w4Count(c, seen); Thread.sleep(1)
    }
    stop.set(true)
    inserters.forEach(_.join(10000))
    if (delayed) CacheDelay.clearHandler("nc.put.head")
    Thread.sleep(100)
    val r = w4Count(c).copy(cumLruNotInTable = seen.size, inserted = inserted.get)
    info(s"W4 run $run delayed=$delayed $r")
    r
  }

  def w4(): Unit = {
    val runs = (1 to 3).map(i => ("free", w4Run(i, delayed = false))) ++ (1 to 3).map(i => ("nc.put.head-delay", w4Run(i, delayed = true)))
    val anyLeak = runs.exists(_._2.tableNotInLru > 0)
    val anyLnT = runs.exists(r => r._2.lruNotInTable > 0 || r._2.cumLruNotInTable > 0)
    val detail = runs.zipWithIndex.map { case ((m, r), i) => s"run${i + 1}[$m]: $r" }.mkString("; ")
    result("W4", if (anyLeak) "DEMONSTRATED" else "QUANTIFIED",
      s"tableNotInLru(any run)=$anyLeak lruNotInTable(any run)=$anyLnT (16-entry private UNodeCache, 4 inserters, 200x setMaxSize(64)/setMaxSize(16) 1 ms apart) $detail")
  }

  // ================================================================================================================
  // W8 (R13): scenarioStackWithInitialTime
  // ================================================================================================================
  def w8(): Unit = {
    val a = w8Run(0)
    val b = w8Run(200000)
    result("W8", if (a._1 > 1 || b._1 > 1) "DEMONSTRATED" else "NOT_REPRODUCED", s"[newTopScenario instantaneous] ${a._2} | [newTopScenario takes 0.2 ms, like a time lookup] ${b._2}")
  }

  def w8Run(lookupNanos: Int): (Int, String) = {
    val dag = new ProbeDag
    val it = dag.prop("w8InitialTime").base(0)
    val sys = new ProbeSystem(dag)
    val rounds = 50
    val threads = 16
    var worst = 0
    var roundsWithDup = 0
    val perRound = new StringBuilder
    var totalCalls = 0
    for (r <- 0 until rounds) {
      val comps = new W8Components(sys.pp(it).info, lookupNanos)
      val env = new RuntimeEnvironment(comps, null)
      val root = CacheFixtures.newRoot()
      val shared = root.ssShared
      T.setField(shared, "_environment", env)
      require(shared.environmentWithoutTrackingAccess eq env)
      val barrier = new CyclicBarrier(threads)
      val got = new java.util.concurrent.ConcurrentLinkedQueue[ScenarioStack]()
      val ths = (0 until threads).map { i =>
        val t = new Thread(() => {
          barrier.await()
          got.add(shared.scenarioStackWithInitialTime)
        }, s"w8-$i")
        t.start(); t
      }
      ths.foreach(_.join(10000))
      val stacks = got.asScala.toSeq
      val instants = stacks.map(s => comps.instantOf.get(s.topScenario)).toSet
      val distinctStacks = stacks.map(System.identityHashCode).toSet.size
      val finalInst = comps.instantOf.get(shared.scenarioStackWithInitialTime.topScenario)
      if (instants.size > 1) roundsWithDup += 1
      worst = math.max(worst, instants.size)
      totalCalls += comps.calls.get
      if (r < 3)
        perRound ++= s"r$r:{instants=${instants.size} stacks=$distinctStacks newTopScenarioCalls=${comps.calls.get} laterCallersSee=${finalInst.getNano}} "
    }
    (worst, s"rounds=$rounds threads=$threads roundsWithMoreThanOneInitialTime=$roundsWithDup maxDistinctInitialTimes=$worst totalNewTopScenarioCalls=$totalCalls (1 per round if correct); $perRound")
  }

  // ================================================================================================================
  // W9 (D7): XSFT stale promise via tryToRecoverFromCycle
  // ================================================================================================================
  lazy val xsftCls: Class[_] = Class.forName("optimus.graph.cache.DelayedXSFTProxyNode")
  lazy val ipsCls: Class[_] = Class.forName("optimus.graph.cache.DelayedXSFTProxyNode$InProgressState")
  def ips(p: AnyRef): AnyRef = fieldOf(xsftCls, p, "ips")
  def ipsStr(p: AnyRef, names: List[(AnyRef, String)]): String = {
    if (p == null) return "<no proxy>"
    val s = ips(p)
    def nm(o: AnyRef) = if (o == null) "null" else names.collectFirst { case (k, v) if k eq o => v }.getOrElse(ident(o))
    if (s == null) "null"
    else if (s eq staticField(xsftCls, "COMPLETED")) "COMPLETED"
    else if (s eq staticField(xsftCls, "EMPTY")) "EMPTY"
    else {
      val down = fieldOf(ipsCls, s, "promisedToDown").asInstanceOf[java.util.ArrayList[AnyRef]]
      val mask = fieldOf(ipsCls, s, "dependsOnMask")
      s"IPS{by=${nm(fieldOf(ipsCls, s, "promisedBy"))} up=${nm(fieldOf(ipsCls, s, "promisedToUp"))} " +
        s"down=${if (down == null) "[]" else down.asScala.map(nm).mkString("[", ",", "]")} must=${fieldOf(ipsCls, s, "mustHaveValue")} " +
        s"self=${fieldOf(ipsCls, s, "doSelfEvaluate")} mask=${if (mask == null) "null" else mask.asInstanceOf[TPDMask].stringEncoded}}"
    }
  }
  def ipsField(p: AnyRef, name: String): AnyRef = { val s = ips(p); if (s == null || !ipsCls.isInstance(s)) null else fieldOf(ipsCls, s, name) }

  def w9(): Unit = {
    val dag = new ProbeDag
    val k = dag.prop("w9K").base(1) // tweakable leaf: 1
    val z = dag.prop("w9Z").base(1000) // unrelated tweakable
    val armed = new AtomicBoolean(false)
    val latchRoot = new CountDownLatch(1); val latchLow = new CountDownLatch(1)
    val startedRoot = new CountDownLatch(1); val startedLow = new CountDownLatch(1)
    val f = dag.prop("w9F").notTweakable().reads(k).combine { (_, xs) =>
      val kv = xs(0)
      if (armed.get) {
        if (kv == 1) { startedRoot.countDown(); latchRoot.await(30, TimeUnit.SECONDS) }
        else { startedLow.countDown(); latchLow.await(30, TimeUnit.SECONDS) }
      }
      kv * 10
    }
    val sys = new ProbeSystem(dag)
    CacheFixtures.setTweakableID(sys.info(k), 300)
    CacheFixtures.setTweakableID(sys.info(z), 301)
    CacheFixtures.setAssumeDependsOn(sys.info(f)) // empty mask, XSFT
    val kf = ProbeDag.key(f)
    val l1 = Level.of(Tw.property(k, 5)); val l2 = Level.of(Tw.property(z, 7))
    val ora_root = oracle(Stack.of(), kf); val ora_s1 = oracle(Stack.of(l1), kf); val ora_s2 = oracle(Stack.of(l1, l2), kf)
    armed.set(true)
    val root = CacheFixtures.newRoot()
    val s1 = CacheFixtures.child(root, sys.scenario(l1))
    val s2 = CacheFixtures.child(s1, sys.scenario(l2))
    val steps = new StringBuilder
    def step(s: String): Unit = { steps ++= s + "; "; info("W9 " + s) }
    step(s"policy=${sys.info(f).cachePolicy} mask=${sys.info(f).dependsOnTweakMask.stringEncoded} K.mask=${sys.info(k).tweakMask.stringEncoded} s1.mayContainK=${s1._cacheID.mayContainTweak(sys.info(k).tweakMask)} s2.mayContainK=${s2._cacheID.mayContainTweak(sys.info(k).tweakMask)}")

    def async[A](name: String)(body: => A): (Thread, AtomicReference[Either[Throwable, A]]) = {
      val r = new AtomicReference[Either[Throwable, A]]()
      val t = new Thread(() => { CacheFixtures.init(); try r.set(Right(body)) catch { case e: Throwable => r.set(Left(e)) } }, name)
      t.setDaemon(true); t.start(); (t, r)
    }
    // 1. F@root: R = U_old, self-evaluates; its source blocks on latchRoot
    val (ta, ra) = async("w9-root") { sys.lookupNode(root, kf) }
    val okRoot = startedRoot.await(5, TimeUnit.SECONDS)
    val R = getNodeOnly(root, sys.newKey(kf))
    var names: List[(AnyRef, String)] = List(R -> "R")
    step(s"1 root requested: srcStarted=$okRoot R=${ident(R)} R.ips=${ipsStr(R, names)}")
    // 2. F@s2: P waits on R
    val (tb, rb) = async("w9-s2") { sys.lookupNode(s2, kf) }
    var P: Node[java.lang.Long] = null
    val deadline = System.nanoTime() + 5000000000L
    var waiting = false
    while (!waiting && System.nanoTime() < deadline) {
      Thread.sleep(5)
      P = getNodeOnly(s2, sys.newKey(kf))
      if (!names.exists(_._1 eq P)) names = (P, "P") :: names
      val down = ipsField(R, "promisedToDown").asInstanceOf[java.util.ArrayList[AnyRef]]
      waiting = ipsField(P, "promisedBy") != null && down != null && down.contains(P)
    }
    step(s"2 s2 requested: P=${ident(P)} P.isXSFTProxy=${xsftCls.isInstance(P)} P waits on R=$waiting P.waitingOn=${ident(P.getWaitingOn)} P.ips=${ipsStr(P, names)} R.ips=${ipsStr(R, names)}")
    if (!waiting) {
      latchRoot.countDown(); latchLow.countDown()
      result("W9", "ERROR", s"could not put P in WAIT on R: ${steps}")
      return
    }
    // 3. P.tryToRecoverFromCycle(maskWithK)
    val maskK = sys.info(k).tweakMask
    val recovered = new AtomicReference[java.lang.Boolean]()
    val (tc, _) = async("w9-recover") {
      CacheFixtures.evalIn(s2) { recovered.set(P.tryToRecoverFromCycle(maskK, OGSchedulerContext.current())); true }
    }
    val d3 = System.nanoTime() + 5000000000L
    while (recovered.get == null && System.nanoTime() < d3) Thread.sleep(5)
    val okLow = startedLow.await(5, TimeUnit.SECONDS)
    val U1 = ipsField(P, "promisedToUp")
    if (U1 != null) names = (U1, "U1") :: names
    step(s"3 P.tryToRecoverFromCycle(K) returned ${recovered.get}: F.mask=${sys.info(f).dependsOnTweakMask.stringEncoded} P.ips=${ipsStr(P, names)} R.ips=${ipsStr(R, names)} " +
      s"U1=${if (U1 == null) "null" else ident(U1) + " U1.ss==s1:" + (U1.asInstanceOf[NodeTask].scenarioStack()._cacheID eq s1._cacheID)} U1.ips=${ipsStr(U1, names)} s2SrcStarted=$okLow P still in R.down=${Option(ipsField(R, "promisedToDown")).exists(_.asInstanceOf[java.util.ArrayList[AnyRef]].contains(P))}")
    // 4. release U_old's source
    latchRoot.countDown()
    ta.join(5000)
    tb.join(5000)
    val pv = rb.get
    step(s"4 latchRoot released: root=${Option(ra.get).map(_.fold(_.toString, n => n.resultObject.toString)).orNull} P.done=${P.isDone} P.value=${if (P.isDoneWithResult) P.resultObject else "-"} P.ips=${ipsStr(P, names)} " +
      s"U1.done=${if (U1 == null) "-" else U1.asInstanceOf[NodeTask].isDone} U1.value=${if (U1 != null && U1.asInstanceOf[NodeTask].isDoneWithResult) U1.asInstanceOf[NodeTask].resultObject else "-"} U1.ips=${ipsStr(U1, names)} s2 requester got=${Option(pv).map(_.fold(_.toString, n => n.resultObject.toString)).getOrElse("<still waiting>")}")
    // 5. a second requester at s1
    val s1r = withTimeout(5000, "w9-s1") { sys.lookupNode(s1, kf) }
    val s1Str = s1r.fold(e => e, n => s"${nodeStr(n)} sameAsU1=${n eq U1}")
    step(s"5 F@s1 requester: $s1Str")
    latchLow.countDown()
    tc.join(5000)
    val pAfter = withTimeout(5000, "w9-s2-again") { sys.eval(s2, kf) }
    val s1After = withTimeout(5000, "w9-s1-again") { sys.eval(s1, kf) }
    step(s"6 latchLow released: F@s2 again=${pAfter.merge} F@s1 again=${s1After.merge} P.ips=${ipsStr(P, names)} U1.ips=${ipsStr(U1, names)}")
    val pVal = if (P.isDoneWithResult) P.resultObject.asInstanceOf[java.lang.Long].longValue else Long.MinValue
    val s1Val = s1r.toOption.filter(_.isDoneWithResult).map(_.resultObject.asInstanceOf[java.lang.Long].longValue)
    val wrongP = P.isDoneWithResult && pVal == ora_root && pVal != ora_s2
    val wrongU1 = s1Val.exists(_ != ora_s1)
    val hangU1 = s1r.isLeft
    result("W9", if (wrongP || wrongU1 || hangU1) "DEMONSTRATED" else "NOT_REPRODUCED",
      s"P(F@s2)=${if (P.isDoneWithResult) pVal.toString else "not done"} oracle(s2)=$ora_s2 oracle(root)=$ora_root wrongAtP=$wrongP; F@s1=${s1Val.getOrElse(s1r.left.getOrElse("?"))} oracle(s1)=$ora_s1 wrongAtU1=$wrongU1 U1hang=$hangU1; steps: $steps")
  }

  // ================================================================================================================
  // W10 (D8): tweakUsageQWords=0
  // ================================================================================================================
  def w10Child(): Unit = {
    val dag = new ProbeDag
    val k = dag.prop("w10K").base(1)
    val f = dag.prop("w10F").notTweakable().reads(k).combine((_, xs) => xs(0) * 10)
    val g = dag.prop("w10G").notTweakable().reads(k).combine((_, xs) => xs(0) * 100) // default policy, not forced
    val sys = new ProbeSystem(dag)
    CacheFixtures.setPolicy(sys.info(f), NCPolicy.XSFT)
    val root = CacheFixtures.newRoot()
    val sb = new StringBuilder
    sb ++= s"width=${DiagnosticSettings.tweakUsageQWords} enablePerNodeTPDMask=${DiagnosticSettings.enablePerNodeTPDMask} F.policy=${sys.info(f).cachePolicy} G.policy=${sys.info(g).cachePolicy} "
    for ((p, nm) <- Seq(f -> "F", g -> "G")) {
      val kp = ProbeDag.key(p)
      val vr = sys.eval(root, kp)
      for ((lvl, lname) <- Seq(Level.of(Tw.property(k, 5)) -> "propTweak", Level.of(Tw.instance(ProbeDag.key(k), 6)) -> "instTweak")) {
        val ss = sys.stack(root, Stack.of(lvl))
        val runs0 = sys.pp(p).runs.get
        val vc = sys.eval(ss, kp)
        val o = oracle(Stack.of(lvl), kp)
        sb ++= s"$nm[$lname]: root=$vr child=$vc oracle=$o runs+${sys.pp(p).runs.get - runs0} wrong=${vc != o} "
      }
      sb ++= s"$nm.dependsOnMask=${sys.info(p).dependsOnTweakMask.stringEncoded} "
    }
    println("W10CHILD " + sb.toString)
  }

  def w10(): Unit = {
    def run(flags: Seq[String]): (Int, String) = {
      val (code, out) = CacheFixtures.runChild(flags, "optimus.graph.Tier0Probes", Seq("w10-child"), 120000)
      (code, out.linesIterator.find(_.startsWith("W10CHILD ")).map(_.stripPrefix("W10CHILD ")).getOrElse("<no line> " + out.takeRight(1500)))
    }
    val (c0, l0) = run(Seq("-Doptimus.graph.tweakUsageQWords=0"))
    val (cd, ld) = run(Seq())
    val wrong0 = l0.contains("wrong=true")
    val wrongD = ld.contains("wrong=true")
    result("W10", if (wrong0 && !wrongD) "DEMONSTRATED" else if (wrong0) "DEMONSTRATED(control also wrong)" else "NOT_REPRODUCED",
      s"[tweakUsageQWords=0 exit=$c0] $l0 | [default width exit=$cd] $ld")
  }

  // ================================================================================================================
  // W13 (D2): traversal ids
  // ================================================================================================================
  def w13(): Unit = {
    val a = w13EndToEnd()
    val b = Tier0TrackingAccess.w13Direct(200)
    val demonstrated = a._1 || b._1
    result("W13", if (demonstrated) "DEMONSTRATED" else if (a._3 || b._3) "QUANTIFIED" else "NOT_REPRODUCED",
      s"(a) concurrent children end-to-end: ${a._2} | (b) direct shared graph: ${b._2}")
  }

  /** Returns (falsified, detail, duplicate ids seen). */
  def w13EndToEnd(): (Boolean, String, Boolean) = {
    val dag = new ProbeDag
    val k = dag.prop("w13K").base(1)
    val n = dag.prop("w13N").notTweakable().reads(k, ProbeDag.MODULE, 0)
    val m = dag.prop("w13M").notTweakable().reads(n) // reads N with the same arg
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    val c1 = T.concurrentChild(root, "w13c1")
    val c2 = T.concurrentChild(root, "w13c2")
    val queues = s"root.queue=${ident(T.queue(root))} c1.queue=${ident(T.queue(c1))} c2.queue=${ident(T.queue(c2))} distinct=${(T.queue(c1) ne T.queue(c2)) && (T.queue(c1) ne T.queue(root)) && (T.queue(c2) ne T.queue(root))}"
    val rec1 = Tier0TrackingAccess.instrumentIdSource(c1)
    val rec2 = Tier0TrackingAccess.instrumentIdSource(c2)
    val kk = ProbeDag.key(k)
    def keysOf(arg: Int) = Seq(ProbeDag.key(n, ProbeDag.MODULE, arg), ProbeDag.key(m, ProbeDag.MODULE, arg))
    val rounds = 200
    var prev = Map(1 -> Option.empty[Long], 2 -> Option.empty[Long])
    var dupRounds = 0; var staleNodes = 0; var wrongValues = 0; var lowAdvance = 0; var missingNodes = 0
    var firstBad = ""
    var tidTotal = 0L; var traversals = 0L
    val barrier = new CyclicBarrier(2)
    for (r <- 0 until rounds) {
      val trs = Seq(1 -> c1, 2 -> c2)
      // evaluate in both children
      for ((i, c) <- trs) T.evaluateKeys(c, keysOf(i).map(sys.newKey(_)))
      val snap = for ((i, c) <- trs; key <- keysOf(i)) yield (i, key, sys.pp(key.prop).lastRun.get(key))
      val stacks = trs.map { case (i, _) => i -> (prev(i) match { case Some(v) => Stack.of(Level.of(Tw.instance(kk, v))); case None => Stack.of() }) }.toMap
      val tid0 = T.traversalId(root)
      rec1.clear(); rec2.clear()
      val futs = new java.util.concurrent.ConcurrentHashMap[Int, Throwable]()
      val ths = trs.map { case (i, c) =>
        val t = new Thread(() => {
          CacheFixtures.init()
          barrier.await()
          try T.await(T.addTweaksAsync(c, Seq(sys.tweak(Tw.instance(kk, 1000L * i + r)))), 20000)
          catch { case e: Throwable => futs.put(i, e) }
        }, s"w13-upd-$i")
        t.start(); t
      }
      ths.foreach(_.join(30000))
      val tid1 = T.traversalId(root)
      val ids = rec1.starts.asScala.toSeq ++ rec2.starts.asScala.toSeq
      traversals += ids.size
      tidTotal += tid1 - tid0
      if (ids.distinct.size < ids.size) dupRounds += 1
      if (tid1 - tid0 < ids.size) lowAdvance += 1
      // invalidation oracle per child
      for ((i, _) <- trs) {
        val ev = new SequentialEvaluator(stacks(i))
        keysOf(i).foreach(ev.eval)
        val mustInvalid = InvalidationOracle.dependents(ev, kk).asScala.toSet
        for ((j, key, node) <- snap if j == i) {
          if (node == null) missingNodes += 1
          else if (mustInvalid.contains(key) && !node.isInvalidCache) {
            staleNodes += 1
            if (firstBad.isEmpty) firstBad = s"round $r child $i $key not invalidated (ids=${ids.mkString(",")})"
          }
        }
      }
      prev = Map(1 -> Some(1000L + r), 2 -> Some(2000L + r))
      for ((i, c) <- trs; key <- keysOf(i)) {
        val v = T.evaluate(c, sys.newKey(key))
        val o = oracle(Stack.of(Level.of(Tw.instance(kk, prev(i).get))), key)
        if (v != o) { wrongValues += 1; if (firstBad.isEmpty) firstBad = s"round $r child $i $key value $v oracle $o" }
      }
      if (!futs.isEmpty) firstBad += s" update errors: $futs"
    }
    T.disposeAllRoots()
    val falsified = staleNodes > 0 || wrongValues > 0
    (falsified,
      s"$queues rounds=$rounds traversals=$traversals traversalIdAdvance=$tidTotal roundsWithDuplicateStartId=$dupRounds roundsWhereIdAdvancedLessThanTraversals=$lowAdvance staleCachedNodes=$staleNodes wrongValues=$wrongValues missingNodes=$missingNodes ${if (firstBad.nonEmpty) "first: " + firstBad else ""} (the two children's TTrack graphs are disjoint: no shared track reachable this way)",
      dupRounds > 0)
  }

  // ================================================================================================================
  def main(args: Array[String]): Unit = {
    if (args.headOption.contains("w10-child")) { CacheFixtures.init(); w10Child(); System.out.flush(); Runtime.getRuntime.halt(0) }
    CacheFixtures.init()
    val all = Seq("W2", "W4", "W8", "W9", "W10", "W13")
    val linger = args.contains("linger")
    val ids = args.toSeq.filter(_ != "linger")
    val which = if (ids.isEmpty) all else ids.map(_.toUpperCase)
    for (id <- which) {
      info(s"== $id")
      try id match {
        case "W2"  => w2()
        case "W4"  => w4()
        case "W8"  => w8()
        case "W9"  => w9()
        case "W10" => w10()
        case "W13" => w13()
        case other => result(other, "ERROR", "unknown probe id")
      } catch { case t: Throwable => t.printStackTrace(System.out); result(id, "ERROR", t.toString) }
    }
    println("\n== summary"); results.forEach(println(_))
    System.out.flush()
    if (linger) { info("lingering 20 s (does anything exit the JVM?)"); Thread.sleep(20000); info("still alive after 20 s")
      Thread.getAllStackTraces.asScala.filter(t => t._1.getName.startsWith("w9") || t._1.getName.startsWith("og.")).foreach { case (t, st) => info(s"thread ${t.getName} ${t.getState}: ${st.take(8).mkString(" | ")}") } }
    Runtime.getRuntime.halt(0)
  }
}
}

package optimus.graph.tracking {

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger

import optimus.graph.CacheFixtures
import optimus.graph.NodeTask
import optimus.graph.ProbeDag
import optimus.graph.ProbeSystem
import optimus.graph.tracking.ttracks.TTrack
import optimus.graph.tracking.ttracks.Tier0CountingInvalidator

/** Records the start id of every traversal and delegates to the real (racy) root.withFreshTraversalId. */
final class RecordingIdSource(val root: DependencyTrackerRoot) extends TraversalIdSource {
  val starts = new java.util.concurrent.ConcurrentLinkedQueue[Integer]()
  def clear(): Unit = starts.clear()
  override private[tracking] def withFreshTraversalId(func: Integer => Integer): Unit =
    root.withFreshTraversalId { s => starts.add(s); func(s) }
}

object Tier0TrackingAccess {
  import scala.jdk.CollectionConverters._
  /** Replace tracker t's TweakableTracker.idSource with a recording wrapper around the real root (reflection). */
  def instrumentIdSource(t: DependencyTracker): RecordingIdSource = {
    val rec = new RecordingIdSource(t.root)
    CacheTrackerAccess.setField(t.tweakableTracker, "idSource", rec)
    rec
  }

  /**
   * W13 direct: one tracker, two tweakables K1/K2 read by a shared combinator S, with `fan` callers above S. Two
   * threads released by a barrier invalidate from K1's and K2's TTrackRoot concurrently (marking traversals with ids
   * from the real root.withFreshTraversalId). Counts duplicate ids, tracks visited per traversal, and nodes left valid.
   * Returns (falsified, detail, duplicate ids seen).
   */
  def w13Direct(rounds: Int): (Boolean, String, Boolean) = {
    import ProbeDag._
    val fan = 64
    val depth = 40
    val dag = new ProbeDag
    val k1 = dag.prop("w13dK1").base(1)
    val k2 = dag.prop("w13dK2").base(2)
    val s = dag.prop("w13dS").notTweakable().reads(k1, MODULE, 0).reads(k2, MODULE, 0)
    // a chain S <- C1 <- ... <- C_depth, then `fan` callers of C_depth: a shared region behind S
    // each C_i also reads its own tweakable leaf L_i, so it gets its own TTrack instead of being merged into S's
    val chain = (1 to depth).scanLeft(s)((prev, i) =>
      dag.prop(s"w13dC$i").notTweakable().reads(prev, MODULE, 0).reads(dag.prop(s"w13dL$i").base(i), MODULE, 0)).tail
    val u = dag.prop("w13dU").notTweakable().reads(chain.last, MODULE, 0)
    val sys = new ProbeSystem(dag)
    var dupRounds = 0; var leftValid = 0
    var sumV1 = 0L; var sumV2 = 0L; var sumUnion = 0L
    var earlyReturnRounds = 0; var maxValidAtReturn = 0
    var example = ""
    for (r <- 0 until rounds) {
      val tr = CacheTrackerAccess.newRoot()
      val keys = (0 until fan).map(i => key(u, MODULE, i))
      CacheTrackerAccess.evaluateKeys(tr, keys.map(sys.newKey(_)))
      val nodes: Seq[NodeTask] = keys.map(k => sys.pp(u).lastRun.get(k)) ++ (s +: chain).map(p => sys.pp(p).lastRun.get(key(p)))
      val ttr = CacheTrackerAccess.ttracks(tr).asScala
      def rootOf(p: ProbeDag.Prop): TTrack = ttr.collectFirst {
        case (k, v) if k.propertyInfo eq sys.info(p) => v.asInstanceOf[TTrack]
      }.orNull
      val r1 = rootOf(k1); val r2 = rootOf(k2)
      if (r1 == null || r2 == null) return (false, s"could not find TTrackRoots (k1=$r1 k2=$r2 keys=${ttr.keys.mkString(",")})", false)
      val src = new RecordingIdSource(tr)
      val inv1 = new Tier0CountingInvalidator; val inv2 = new Tier0CountingInvalidator
      val barrier = new CyclicBarrier(2)
      // nodes still valid at the moment each traversal returns (= when its update would release evaluations)
      val validAtReturn = new java.util.concurrent.ConcurrentHashMap[Int, Integer]()
      def countValid() = nodes.count(n => n != null && !n.isInvalidCache)
      val t1 = new Thread(() => { barrier.await(); inv1.visit(r1, src); validAtReturn.put(1, countValid()) })
      val t2 = new Thread(() => { barrier.await(); inv2.visit(r2, src); validAtReturn.put(2, countValid()) })
      t1.start(); t2.start(); t1.join(10000); t2.join(10000)
      val ids = src.starts.asScala.toSeq
      val dup = ids.distinct.size < ids.size
      if (dup) dupRounds += 1
      val notInv = countValid()
      leftValid += notInv
      val vr = math.max(validAtReturn.getOrDefault(1, 0), validAtReturn.getOrDefault(2, 0))
      if (vr > 0) { earlyReturnRounds += 1; maxValidAtReturn = math.max(maxValidAtReturn, vr) }
      val union = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
      inv1.visited.forEach(t => union.put(t, true)); inv2.visited.forEach(t => union.put(t, true))
      sumV1 += inv1.visited.size; sumV2 += inv2.visited.size; sumUnion += union.size
      if (dup && (vr > 0 || example.isEmpty))
        example = s"round $r ids=${ids.mkString(",")} visited K1-trav=${inv1.visited.size} K2-trav=${inv2.visited.size} union=${union.size} validNodesWhenEachReturned=${validAtReturn} nodesLeftValidAtEnd=$notInv"
      CacheTrackerAccess.dispose(tr)
    }
    val falsified = leftValid > 0
    (falsified,
      s"rounds=$rounds chainDepth=$depth fan=$fan roundsWithDuplicateId=$dupRounds nodesLeftValidAtEnd=$leftValid roundsWhereATraversalReturnedWithDependentsStillValid=$earlyReturnRounds (max $maxValidAtReturn of ${depth + 1 + fan}) meanVisited K1-trav=${sumV1.toDouble / rounds} K2-trav=${sumV2.toDouble / rounds} union=${sumUnion.toDouble / rounds} ${if (example.nonEmpty) "dup example: " + example else ""}",
      dupRounds > 0)
  }
  import scala.jdk.CollectionConverters._
}
}
