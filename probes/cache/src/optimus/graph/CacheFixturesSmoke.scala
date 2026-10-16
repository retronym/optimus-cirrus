package optimus.graph

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import optimus.graph.ProbeDag._
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.UNodeCache
import optimus.graph.tracking.{CacheTrackerAccess => T}
import optimus.platform._
import optimus.platform.storable.EntityImpl
import probes.CacheDelay
import probes.Delay
import probes.Steps

import scala.jdk.CollectionConverters._

/**
 * Smoke test for the P0 fixtures: exercises every fixture once and checks it against the oracles.
 * probes/cache/run.sh optimus.graph.CacheFixturesSmoke [section...]   (sections: nodes entities scenarios tracker
 * priming hooks child; default all). Exit 0 iff every check passed.
 */
object CacheFixturesSmoke {
  private var pass = 0
  private var fail = 0
  def check(name: String, ok: Boolean, detail: => String = ""): Unit = {
    if (ok) pass += 1 else fail += 1
    println(f"${if (ok) "OK  " else "FAIL"} $name%-60s $detail")
  }
  def section(s: String): Unit = println(s"\n== $s")

  /** system value or exception class name, vs oracle value or "ISD" */
  def sysVal(sys: ProbeSystem, ss: ScenarioStack, k: Key): String =
    try sys.eval(ss, k).toString
    catch { case e: IllegalScenarioDependenceException => "ISD"; case e: Throwable => e.getClass.getSimpleName }
  def oraVal(st: Stack, k: Key): String =
    try new SequentialEvaluator(st).eval(k).toString
    catch { case _: OracleISD => "ISD" }
  def agree(sys: ProbeSystem, ss: ScenarioStack, st: Stack, k: Key, what: String, expect: String = null): Unit = {
    val s = sysVal(sys, ss, k)
    val o = oraVal(st, k)
    check(s"$what: $k", s == o && (expect == null || s == expect), s"system=$s oracle=$o${if (expect != null) " expect=" + expect else ""}")
  }

  /** The common test DAG. */
  final class D {
    val dag = new ProbeDag
    val a: Prop = dag.prop("a").base(1) // tweakable leaf: 1 + arg
    val b: Prop = dag.prop("b").base(10) // tweakable leaf: 10 + arg
    val c: Prop = dag.prop("c").notTweakable().reads(a).reads(b) // a + b
    val k: Prop = dag.prop("k").notTweakable().base(5)
    val s: Prop = dag.prop("s").notTweakable().si().reads(k) // SI over a non-tweakable: fine
    val sBad: Prop = dag.prop("sBad").notTweakable().si().reads(a) // SI reading a tweakable: ISD
    val t: Prop = dag.prop("t").notTweakable().kind(Kind.TAG_READER)
    val e: Prop = dag.prop("e").notTweakable().kind(Kind.ENV_READER)
    val bk1: Prop = dag.prop("bkMutable").notTweakable().reads(a).badKey(BadKey.MUTABLE_ARG)
    val bk2: Prop = dag.prop("bkAsym").notTweakable().reads(a).badKey(BadKey.ASYMMETRIC_EQUALS)
    val bk3: Prop = dag.prop("bkHash").notTweakable().reads(a).badKey(BadKey.UNSTABLE_HASH)
    val sys = new ProbeSystem(dag)
  }
  def plus(n: Long): Combine = (_, xs) => xs.sum + n
  def times(n: Long): Combine = (_, xs) => xs.sum * n

  // ---------------------------------------------------------------------------------------------------------------
  def nodes(): Unit = {
    section("1. hand-written property nodes (root stack) vs value oracle")
    val d = new D; import d._
    val root = CacheFixtures.newRoot()
    val st = Stack.of()
    for (p <- Seq(a, b, c, s, t, e, bk1, bk2, bk3)) agree(sys, root, st, key(p), "root")
    agree(sys, root, st, key(sBad), "SI reading tweakable", "ISD")
    agree(sys, root, st, ProbeDag.key(a, MODULE, 3), "arg", "4")
    // caching: c evaluated twice -> one run
    val before = sys.pp(c).runsOf(key(c))
    sys.eval(root, key(c)); sys.eval(root, key(c))
    check("combinator cached at root (second eval is a hit)", sys.pp(c).runsOf(key(c)) == before, s"runs ${sys.pp(c).runsOf(key(c))}")
    // TWEAKABLE nodes are tracking-value
    check("fresh tweakable key isTrackingValue (initTrackingValue ran with the ctor-param info)", sys.newKey(a).isTrackingValue)
    val leafNode = sys.lookupNode(root, key(a))
    println(s"     info: getNode(a) at root returned ${leafNode.getClass.getSimpleName} $leafNode (policy ${sys.info(a).cachePolicy})")
    // bad keys: UNSTABLE_HASH never hits
    val r0 = sys.pp(bk3).runs.get
    sys.eval(root, key(bk3)); sys.eval(root, key(bk3))
    check("BadKey UNSTABLE_HASH: every lookup misses", sys.pp(bk3).runs.get - r0 == 2, s"runs +${sys.pp(bk3).runs.get - r0}")
    // MUTABLE_ARG: two different args compare equal -> second is a (wrong) hit
    val r1 = sys.pp(bk1).runs.get
    val v0 = sys.eval(root, ProbeDag.key(bk1, MODULE, 0))
    val v1 = sys.eval(root, ProbeDag.key(bk1, MODULE, 1))
    check("BadKey MUTABLE_ARG: arg 1 served arg 0's node (wrong reuse)", sys.pp(bk1).runs.get == r1 && v0 == v1 && v1.toString != oraVal(st, ProbeDag.key(bk1, MODULE, 1)),
      s"v(0)=$v0 v(1)=$v1 oracle(1)=${oraVal(st, ProbeDag.key(bk1, MODULE, 1))}")
    // env reader with a distinct env
    val env = CacheFixtures.newEnv()
    sys.registerEnv(env, 123)
    agree(sys, CacheFixtures.newRoot(env), new Stack(java.util.List.of(), 123), key(e), "env reader, env token 123", "123")
  }

  // ---------------------------------------------------------------------------------------------------------------
  def entities(): Unit = {
    section("2. entities: FakeModuleEntity (property tweaks) and ProbeEntity (instance tweaks)")
    val d = new D; import d._
    val root = CacheFixtures.newRoot()
    val e3 = new ProbeEntity(3)
    check("ProbeEntity(3) == ProbeEntity(3), hash equal", e3 == new ProbeEntity(3) && e3.hashCode == new ProbeEntity(3).hashCode, s"hash=${e3.hashCode}")
    check("ProbeEntity(3) != ProbeEntity(4)", e3 != new ProbeEntity(4))
    val lv = Level.of(Tw.instance(ProbeDag.key(a, 3, 0), 100))
    val st = Stack.of(lv)
    val ss = sys.stack(root, st)
    agree(sys, ss, st, ProbeDag.key(a, 3, 0), "instance tweak on ProbeEntity(3)", "100")
    agree(sys, ss, st, ProbeDag.key(a, 4, 0), "same property, ProbeEntity(4) untweaked", "1")
    agree(sys, ss, st, ProbeDag.key(c, 3, 0), "combinator over instance-tweaked leaf", "110")
    agree(sys, ss, st, key(a), "module entity untweaked", "1")
    // a key on a *different but equal* entity instance resolves the same tweak
    val k2 = new TweakableLeaf(sys, sys.pp(a), ProbeDag.key(a, 3, 0)) {
      override val entity: optimus.platform.storable.Entity = new ProbeEntity(3)
    }
    check("instance tweak matches an equal (not identical) entity", CacheFixtures.eval(ss, k2).longValue == 100L)
    // property tweak on the module entity's property
    val st2 = Stack.of(Level.of(Tw.property(a, 7)))
    agree(sys, sys.stack(root, st2), st2, key(a), "property tweak (module entity)", "7")
    agree(sys, sys.stack(root, st2), st2, ProbeDag.key(a, 5, 0), "property tweak applies to ProbeEntity(5) too", "7")
    // LOCAL_CACHE on an entity property
    val d2 = new ProbeDag
    val lc = d2.prop("lc").notTweakable().base(3); lc.localCache = true
    val sys2 = new ProbeSystem(d2)
    sys2.eval(root, ProbeDag.key(lc, 6, 0))
    val local = EntityImpl.getLocalCache(sys2.entity(6))
    check("LOCAL_CACHE: node stored in entity local cache", local != null, s"$local")
  }

  // ---------------------------------------------------------------------------------------------------------------
  def scenarios(): Unit = {
    section("3. tweaks and scenarios")
    val d = new D; import d._
    val root = CacheFixtures.newRoot()
    // leaf under a child scenario resolves to the tweak
    val st1 = Stack.of(Level.of(Tw.property(a, 7)))
    val ss1 = sys.stack(root, st1)
    agree(sys, ss1, st1, key(a), "leaf under child scenario", "7")
    agree(sys, ss1, st1, key(c), "combinator under child scenario", "17")
    // by-name property tweak reading another tweakable (resolved through TwkResolver, RHS in parent)
    val aPlus1 = new Rhs(java.util.List.of(new Ref(a, SAME, SAME)), plus(1))
    val st2 = Stack.of(Level.of(Tw.property(a, 7)), Level.of(Tw.property(b, aPlus1, Mode.BYNAME)))
    val ss2 = sys.stack(root, st2)
    val rhs0 = sys.rhsRuns.get
    agree(sys, ss2, st2, key(b), "by-name property tweak b := a + 1", "8")
    check("by-name RHS node ran (TwkResolver -> TweakNode -> RHS)", sys.rhsRuns.get > rhs0, s"rhsRuns +${sys.rhsRuns.get - rhs0}")
    agree(sys, ss2, st2, key(c), "combinator over by-name tweak", "15")
    // by-name *instance* tweak with a node template
    val aTimes2 = new Rhs(java.util.List.of(new Ref(a, MODULE, 0)), times(2))
    val st3 = Stack.of(Level.of(Tw.property(a, 2)), Level.of(Tw.instance(key(b), aTimes2, Mode.BYNAME)))
    agree(sys, sys.stack(root, st3), st3, key(b), "by-name instance tweak b[M] := a * 2", "4")
    // self-referencing by-name: a := a + 1 (RHS resolves in parent)
    val st3b = Stack.of(Level.of(Tw.property(a, 2)), Level.of(Tw.property(a, aPlus1, Mode.BYNAME)))
    agree(sys, sys.stack(root, st3b), st3b, key(a), "a := a + 1 over a := 2", "3")
    // when-predicated property tweak
    val w = new When(i => i == 1, null, null)
    val st4 = Stack.of(Level.of(Tw.propertyWhen(a, w, 50)))
    val ss4 = sys.stack(root, st4)
    agree(sys, ss4, st4, ProbeDag.key(a, MODULE, 1), "when(arg == 1) true", "50")
    agree(sys, ss4, st4, ProbeDag.key(a, MODULE, 2), "when(arg == 1) false", "3")
    // when clause that reads a tweakable (b > 10) evaluated in the parent
    val w2 = new When(_ => true, new Ref(b, MODULE, 0), v => v > 10)
    val st4b = Stack.of(Level.of(Tw.property(b, 20)), Level.of(Tw.propertyWhen(a, w2, 60)))
    agree(sys, sys.stack(root, st4b), st4b, key(a), "when(b > 10) with b := 20 in parent", "60")
    val st4c = Stack.of(Level.of(Tw.property(b, 5)), Level.of(Tw.propertyWhen(a, w2, 60)))
    agree(sys, sys.stack(root, st4c), st4c, key(a), "when(b > 10) with b := 5 in parent", "1")
    // bind vs bindOnce vs byName: [a:=3] [b := a (mode)] [a := 9]
    val justA = new Rhs(java.util.List.of(new Ref(a, MODULE, 0)), plus(0))
    def modeStack(m: Mode) = Stack.of(Level.of(Tw.property(a, 3)), Level.of(Tw.property(b, justA, m)), Level.of(Tw.property(a, 9)))
    for ((m, exp) <- Seq(Mode.BYNAME -> "3", Mode.BIND_ONCE -> "3", Mode.BIND -> "9")) {
      val st = modeStack(m)
      agree(sys, sys.stack(CacheFixtures.newRoot(), st), st, key(b), s"b := a in mode $m, a := 9 above (fresh root)", exp)
    }
    // Same parent: the BIND scenario is == the BYNAME one (Tweak.equals ignores evaluateIn), so createChild hands back
    // the cached BYNAME stack and b resolves by-name. This is ring-3 S5 / campaign W5 (D13): expected to mismatch.
    val rS5 = CacheFixtures.newRoot()
    sys.eval(sys.stack(rS5, modeStack(Mode.BYNAME)), key(b))
    val s5 = sysVal(sys, sys.stack(rS5, modeStack(Mode.BIND)), key(b))
    check("S5: bind after byName on the same parent resolves by-name (wrong)", s5 == "3" && oraVal(modeStack(Mode.BIND), key(b)) == "9",
      s"system=$s5 oracle=${oraVal(modeStack(Mode.BIND), key(b))}  [demonstrates S5/W5: shared stack, wrong value]")
    // ordered / unordered / nested
    agree(sys, sys.stack(root, st1, unordered = true), st1, key(c), "unordered scenario", "17")
    val nestedSc = sys.scenario(st2.levels.get(0)).nest(sys.scenario(st2.levels.get(1)))
    val ssN = root.createChild(nestedSc, this)
    agree(sys, ssN, st2, key(b), "nested scenario == two levels", "8")
    check("nested scenario gives a 2-deep stack", ssN.parent.parent eq root, "")
    // plugin tag
    val stT = new Stack(java.util.List.of(new Level(java.util.List.of(), 77L)), 0)
    agree(sys, sys.stack(root, stT), stT, key(t), "tag reader sees tag 77", "77")
    // the same scenario applied twice to the same parent shares a cache id, and reuses cached nodes
    val r2 = CacheFixtures.newRoot()
    val sA = CacheFixtures.child(r2, sys.scenario(Level.of(Tw.property(a, 7))))
    val sB = CacheFixtures.child(r2, sys.scenario(Level.of(Tw.property(a, 7)))) // equal but separately built
    check("equal scenarios on the same parent share SSCacheID", sA._cacheID eq sB._cacheID,
      s"ids ${sA._cacheID.id} / ${sB._cacheID.id} (stacks ${if (sA eq sB) "identical" else "distinct"})")
    val runs0 = sys.pp(c).runsOf(key(c))
    val nA = sys.lookupNode(sA, key(c))
    val runs1 = sys.pp(c).runsOf(key(c))
    val nB = sys.lookupNode(sB, key(c))
    val runs2 = sys.pp(c).runsOf(key(c))
    check("combinator computed under the first is a hit under the second", runs1 == runs0 + 1 && runs2 == runs1 && (nA eq nB),
      s"runs $runs0 -> $runs1 -> $runs2, same node ${nA eq nB}, value ${nB.result}")
    val sC = CacheFixtures.child(r2, sys.scenario(Level.of(Tw.property(a, 8))))
    check("different scenario gets a different cache id", sC._cacheID ne sA._cacheID, s"${sC._cacheID.id}")
    // bind vs bindOnce scenarios: equal by Scenario.equals? (W5 / D13 territory: just report)
    val tB = sys.tweak(Tw.property(b, justA, Mode.BIND)); val tO = sys.tweak(Tw.property(b, justA, Mode.BIND_ONCE))
    println(s"     info: Tweak.equals(bind, bindOnce) = ${tB == tO}; evaluateIn ${tB.evaluateIn} vs ${tO.evaluateIn}")
    // low-level builders
    val lowLevel = CacheFixtures.valueTweak(sys.newKey(a), java.lang.Long.valueOf(5))
    val ssL = CacheFixtures.child(root, CacheFixtures.scenario(lowLevel))
    check("SimpleValueTweak(key)(v) builder", sys.eval(ssL, key(a)) == 5)
    val pt = CacheFixtures.propertyTweak(sys.pp(a).info, java.lang.Long.valueOf(6))
    check("new Tweak(info, TweakTemplate(v)) builder", sys.eval(CacheFixtures.child(root, CacheFixtures.scenario(pt)), key(a)) == 6)
  }

  // ---------------------------------------------------------------------------------------------------------------
  def tracker(): Unit = {
    section("4. tracker driver + invalidation oracle")
    val dag = new ProbeDag
    val a = dag.prop("ta").base(1)
    val b = dag.prop("tb").base(10)
    val c = dag.prop("tc").notTweakable().reads(a).reads(b)
    val dd = dag.prop("td").notTweakable().reads(c).base(100)
    val u = dag.prop("tu").notTweakable().reads(b)
    val sys = new ProbeSystem(dag)
    val root = T.newRoot()
    val keys = Seq(key(c), key(dd), key(u))
    T.evaluateKeys(root, keys.map(sys.newKey))
    val ev = new SequentialEvaluator(Stack.of())
    keys.foreach(k => ev.eval(k))
    val cached = (Seq(a, b, c, dd, u).map(p => key(p) -> sys.pp(p).lastRun.get(key(p)))).toMap
    check("tracker evaluated c/d/u once each", Seq(c, dd, u).forall(p => sys.pp(p).runsOf(key(p)) == 1))
    check("ttracks registered for the tweakables", T.ttracks(root).size >= 2, s"ttracks=${T.ttracks(root).size}")
    val tid0 = T.traversalId(root)
    T.addTweaks(root, sys.tweak(Tw.instance(key(a), 42)))
    val expected = InvalidationOracle.dependents(ev, key(a)).asScala.toSet
    val actual = cached.collect { case (k, n) if n != null && n.isInvalidCache => k }.toSet
    check("invalidation == oracle after a := 42", expected == actual,
      s"oracle=${expected.map(_.toString).toSeq.sorted} system=${actual.map(_.toString).toSeq.sorted}")
    check("traversalId advanced by the update", T.traversalId(root) > tid0, s"$tid0 -> ${T.traversalId(root)}")
    check("combinator re-evaluates to the new value", T.evaluate(root, sys.newKey(c)) == 52L && sys.pp(c).runsOf(key(c)) == 2,
      s"value=${T.evaluate(root, sys.newKey(c))} runs=${sys.pp(c).runsOf(key(c))}")
    check("unrelated node not re-run", { T.evaluate(root, sys.newKey(u)); sys.pp(u).runsOf(key(u)) == 1 })
    // property-level change
    val ev2 = new SequentialEvaluator(Stack.of(Level.of(Tw.instance(key(a), 42))))
    keys.foreach(k => ev2.eval(k))
    T.evaluateKeys(root, keys.map(sys.newKey))
    // only nodes that are valid now (a's last ProbePropertyNode was already invalidated by the a := 42 update)
    val cached2 = Seq(a, b, c, dd, u).map(p => key(p) -> sys.pp(p).lastRun.get(key(p))).filter { case (_, n) => n != null && !n.isInvalidCache }.toMap
    val exp2all = InvalidationOracle.dependents(ev2, b).asScala.toSet
    T.addTweaks(root, sys.tweak(Tw.property(b, 20)))
    val exp2 = exp2all.filter(cached2.contains)
    val act2 = cached2.collect { case (k, n) if n != null && n.isInvalidCache => k }.toSet
    check("invalidation == oracle after property tweak b := 20", exp2 == act2,
      s"oracle=${exp2.map(_.toString).toSeq.sorted} system=${act2.map(_.toString).toSeq.sorted}")
    val st = Stack.of(Level.of(Tw.instance(key(a), 42), Tw.property(b, 20)))
    check("tracker value == oracle", T.evaluate(root, sys.newKey(dd)) == new SequentialEvaluator(st).eval(key(dd)))
    // children
    val cons = T.consistentChild(root, "cons")
    val conc = T.concurrentChild(root, "conc")
    T.addTweaks(conc, sys.tweak(Tw.property(b, 5)))
    T.addTweaks(cons, sys.tweak(Tw.instance(key(a), 7)))
    check("concurrent child: own queue", T.queue(conc) ne T.queue(root))
    check("consistent child: shared queue", T.queue(cons) eq T.queue(root))
    val stConc = st.push(Level.of(Tw.property(b, 5)))
    val stCons = st.push(Level.of(Tw.instance(key(a), 7)))
    check("concurrent child value == oracle", T.evaluate(conc, sys.newKey(c)) == new SequentialEvaluator(stConc).eval(key(c)),
      s"${T.evaluate(conc, sys.newKey(c))}")
    check("consistent child value == oracle", T.evaluate(cons, sys.newKey(c)) == new SequentialEvaluator(stCons).eval(key(c)),
      s"${T.evaluate(cons, sys.newKey(c))}")
    // async + low-priority + batch
    val (ns, _) = T.await(T.evaluateKeysAsync(root, Seq(sys.newKey(dd))))
    check("evaluateNodeKeysAsync", ns.head.resultObject == java.lang.Long.valueOf(162), s"${ns.head.resultObject}")
    val (nl, _) = T.await(T.evaluateKeysLowPriorityAsync(root, Seq(sys.newKey(c))))
    check("evaluateNodeKeysLowPriorityAsync", nl.head.resultObject == java.lang.Long.valueOf(62), s"${nl.head.resultObject}")
    T.batch(root)(_.addTweaksImmediate(Seq(sys.tweak(Tw.instance(key(a), 43))), throwOnDuplicate = false))
    check("executeBatchUpdate addTweaksImmediate", T.evaluate(root, sys.newKey(c)) == 63L)
    // callbacks
    val scope = T.newScope("probe")
    val calls = new AtomicInteger
    T.registerNodesInvalidated(root, scope)((nodes, _) => calls.addAndGet(nodes.size))
    T.track(root, sys.newKey(dd), scope)
    T.evaluate(root, sys.newKey(dd), scope)
    T.addTweaks(root, sys.tweak(Tw.instance(key(a), 44)))
    check("registerNodesInvalidated fires for a tracked node", calls.get >= 1, s"invalidated tracked nodes: ${calls.get}")
    // internals
    println(s"     info: workQueue=${T.workQueueSize(root)} lowPriorityQueue=${T.lowPriorityQueueSize(root)} " +
      s"_currentCancellationScope=${T.currentCancellationScopeRaw(root)} traversalId=${T.traversalId(root)} " +
      s"ttracks=${T.ttracks(root).size} state=${T.queueState(root)}")
    check("reflection accessors work", T.workQueueSize(root) == 0 && T.lowPriorityQueueSize(root) == 0)
    T.dispose(conc)
    check("dispose(child)", conc.isDisposed)
    T.disposeAllRoots()
    check("TEST_ONLY_disposeAllRoots", root.isDisposed)
  }

  // ---------------------------------------------------------------------------------------------------------------
  def priming(): Unit = {
    section("6. state priming")
    val dag = new ProbeDag
    val a = dag.prop("pa").base(1)
    val c = dag.prop("pc").notTweakable().reads(a)
    val sys = new ProbeSystem(dag)
    val old = UNodeCache.global
    val tiny = CacheFixtures.installGlobalCache(16)
    try {
      check("UNodeCache.global replaced by a 16-entry cache", (UNodeCache.global eq tiny) && tiny.getMaxSize == 16)
      val root = CacheFixtures.newRoot()
      var ok = true
      for (i <- 0 until 100) ok &= sys.eval(root, ProbeDag.key(c, MODULE, i)) == 2L * i + 1
      val size = tiny.getSizeIndicative(true)
      check("100 distinct keys through a 16-entry cache: values right, size bounded", ok && size <= 16, s"size=$size")
      tiny.setMaxSize(64)
      check("setMaxSize(64)", tiny.getMaxSize == 64)
    } finally UNodeCache.global = old
    val info = sys.info(a)
    CacheFixtures.setTweakableID(info, 5)
    check("TEST_ONLY_setTweakableID(5)", info.tweakableID == 5 && info.tweakMask.stringEncoded.nonEmpty, s"mask=${info.tweakMask.stringEncoded}")
    val ci = sys.info(c)
    CacheFixtures.setAssumeDependsOn(ci, 5)
    check("TEST_ONLY_setAssumeDependsOn(5) -> XSFT", ci.cachePolicy == NCPolicy.XSFT, s"${ci.cachePolicy} dependsOn=${ci.dependsOnTweakMask.stringEncoded}")
    val prev = Settings.ttrackClearedNodeRefWatermark
    CacheFixtures.setTTrackWatermark(1)
    check(s"ttrackClearedNodeRefWatermark=1 (-D${CacheFixtures.watermarkProperty})", Settings.ttrackClearedNodeRefWatermark == 1)
    CacheFixtures.setTTrackWatermark(prev)
    println(s"     info: EnableXSFT default = ${CacheFixtures.xsftEnabledByDefault}; default policy of a fresh info = ${sys.info(a).cachePolicy}")
  }

  def child(): Unit = {
    section("6b. child JVM priming")
    if (System.getenv("CACHE_JAVA_CMD") == null) { check("child JVM (needs run.sh)", false, "CACHE_JAVA_CMD unset"); return }
    val (code, out) = CacheFixtures.runChild(
      Seq("-Doptimus.scheduler.cacheSize=16", s"-D${CacheFixtures.watermarkProperty}=1"),
      "optimus.graph.CacheFixturesSmoke", Seq("child-report"))
    val line = out.linesIterator.find(_.startsWith("CHILD ")).getOrElse("<none>")
    check("child JVM: global cache maxSize 16 and watermark 1", code == 0 && line == "CHILD maxSize=16 watermark=1", s"exit=$code $line")
  }
  def childReport(): Unit = {
    CacheFixtures.init()
    val root = CacheFixtures.newRoot()
    val d = new D
    d.sys.eval(root, key(d.c))
    println(s"CHILD maxSize=${UNodeCache.global.getMaxSize} watermark=${Settings.ttrackClearedNodeRefWatermark}")
  }

  // ---------------------------------------------------------------------------------------------------------------
  val allHooks = Seq(
    "nc.put.head", "nc.put.cas", "lru.remove", "lru.catchup", "lru.setMaxSize.swapped",
    "ttrack.completed.cas", "ttrack.addCaller", "ttrack.merge",
    "ptracks.nextFree", "ptracks.resize.cas", "ptracks.slot.cas",
    "xsft.promised", "xsft.beforePromisedToUp", "xsft.offer.beforeSync", "xs.run",
    "dtq.schedule", "dtq.barrier.beforeWait", "sched.waitCancelled.loop")

  def hooks(): Unit = {
    section("7. CacheDelayAgent hooks: arm, drive, count")
    println(s"     instrumented: ${CacheDelay.instrumented.asScala.toSeq.sorted.mkString(" ")}")
    CacheDelay.startCounting()
    // cache: tiny private cache for eviction + resize
    val dag = new ProbeDag
    val x = dag.prop("hx").base(1)
    val y = dag.prop("hy").base(2)
    val shared = dag.prop("hs").base(3)
    val f = dag.prop("hf").notTweakable().reads(x) // XSFT
    val g = dag.prop("hg").notTweakable().reads(x) // XS
    val m = dag.prop("hm").notTweakable().reads(shared, MODULE, 0).reads(y, MODULE, SAME) // shared + own leaf -> merge; 40 callers on shared's TTrack
    val latch = new CountDownLatch(1)
    val started = new CountDownLatch(1)
    val slow = dag.prop("hslow").notTweakable().reads(x).combine { (_, xs) =>
      started.countDown(); latch.await(10, TimeUnit.SECONDS); xs.sum
    }
    val sys = new ProbeSystem(dag)
    val root = CacheFixtures.newRoot()
    val pc = CacheFixtures.privateCache(sys.info(m), 16)
    // XSFT / XS policies
    sys.info(f).setCachePolicy(NCPolicy.XSFT)
    sys.info(g).setFavorReuse(true)
    println(s"     info: f policy=${sys.info(f).cachePolicy} g policy=${sys.info(g).cachePolicy} favorReuseIsXS=${sys.info(g).getFavorReuseIsXS}")
    val sY1 = sys.stack(root, Stack.of(Level.of(Tw.property(y, 5))))
    val sY2 = sys.stack(root, Stack.of(Level.of(Tw.property(y, 6))))
    check("XSFT node under child before root", sys.eval(sY1, key(f)) == 1 && sys.eval(root, key(f)) == 1 && sys.eval(sY2, key(f)) == 1)
    check("XS node under two sibling scenarios", sys.eval(sY1, key(g)) == 1 && sys.eval(sY2, key(g)) == 1)
    // tracker: ttracks, ptracks resize (one tweakable read by 40 distinct merged callers), barrier, waitCancelled loop
    val troot = T.newRoot()
    T.evaluateKeys(troot, (0 until 40).map(i => sys.newKey(m, MODULE, i)))
    check("tracker evaluation of 40 merge callers", sys.pp(m).runs.get == 40, s"runs=${sys.pp(m).runs.get} privateCache size=${pc.getSizeIndicative(true)}")
    pc.setMaxSize(256)
    T.addTweaks(troot, sys.tweak(Tw.property(y, 9))) // update after evaluation -> evaluationBarrier
    // cancelled-but-running node: low-priority evaluation blocks; an update cancels its scope and polls it
    CacheDelay.onHit("sched.waitCancelled.loop", _ => latch.countDown())
    val lp = T.evaluateKeysLowPriorityAsync(troot, Seq(sys.newKey(slow)))
    check("slow low-priority node started", started.await(5, TimeUnit.SECONDS))
    val upd = T.addTweaksAsync(troot, Seq(sys.tweak(Tw.property(x, 2))))
    val updated = try { T.await(upd, 15000); true } catch { case t: Throwable => println(s"     update: $t"); false }
    latch.countDown()
    check("update completed while low-priority node was running", updated)
    try T.await(lp, 15000) catch { case t: Throwable => println(s"     low-priority eval ended with ${t.getClass.getSimpleName}") }
    CacheDelay.clearHandler("sched.waitCancelled.loop")
    T.disposeAllRoots()
    // Delay.arm through CacheDelay: two threads, each armed on nc.put.head, schedule "A1 B1" (then free-running)
    val rArm = CacheFixtures.newRoot()
    val taken = new AtomicInteger
    def participant(i: Int): java.util.function.Consumer[Steps.Ctl] = ctl => {
      CacheFixtures.init()
      Delay.arm(ctl, "nc.put.head")
      try sys.eval(rArm, ProbeDag.key(f, MODULE, 100 + i)) finally Delay.disarm()
      taken.addAndGet(ctl.stepsTaken)
    }
    Steps.schedule("A1 B1").thread(participant(0)).thread(participant(1)).run()
    check("Delay.arm(ctl, \"nc.put.head\") steps both threads via CacheDelay", taken.get >= 2, s"steps taken ${taken.get}")
    val counts = CacheDelay.counts()
    CacheDelay.stopCounting()
    for (h <- allHooks) {
      val n = counts.getOrDefault(h, 0L)
      check(s"hook $h fired", n >= 1, s"count=$n")
    }
    val extra = counts.asScala.keySet -- allHooks
    if (extra.nonEmpty) println(s"     info: other hooks fired: ${extra.map(h => s"$h=${counts.get(h)}").mkString(" ")}")
  }

  // ---------------------------------------------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    if (args.headOption.contains("child-report")) { childReport(); return }
    CacheFixtures.init()
    val all = Seq("nodes", "entities", "scenarios", "tracker", "priming", "child", "hooks")
    val which = if (args.isEmpty) all else args.toSeq
    for (s <- which) {
      try s match {
        case "nodes"     => nodes()
        case "entities"  => entities()
        case "scenarios" => scenarios()
        case "tracker"   => tracker()
        case "priming"   => priming()
        case "child"     => child()
        case "hooks"     => hooks()
        case other       => check(s"unknown section $other", false)
      } catch { case t: Throwable => check(s"section $s threw", false, t.toString); t.printStackTrace(System.out) }
    }
    println(s"\nSMOKE: $pass passed, $fail failed")
    System.out.flush()
    Runtime.getRuntime.halt(if (fail == 0) 0 else 1)
  }
}
