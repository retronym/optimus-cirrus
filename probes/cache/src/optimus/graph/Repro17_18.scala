package optimus.graph

import java.util.concurrent.TimeUnit

import optimus.graph.ProbeDag._
import optimus.graph.cache.NCPolicy
import optimus.platform._

import scala.jdk.CollectionConverters._

/**
 * Minimal reproducers for CACHE_REVIEW.md D17 (fuzzer N2, CANCEL_LEAK) and D18 (fuzzer N3, stale after a when flip).
 *   Repro17_18 d17 [reps]
 *   Repro17_18 d18 [reps]
 */
object Repro17_18 {
  private def brief(t: Throwable): String = s"${t.getClass.getSimpleName}: ${t.getMessage}"
  private def tryEval(sys: ProbeSystem, ss: ScenarioStack, k: Key): String =
    try sys.eval(ss, k).toString catch { case t: Throwable => "EXC " + brief(t).linesIterator.next() }

  // ============================================================================================================== D17
  /**
   * root -> S1 = given(a := byName(b*4+134)) -> S2 = given(z := 1)   (z is unrelated; it only makes S2 a real level)
   *   r1: in S2X = S2.withChildCancellationScope (scope X) a requester cancels X, then looks up a.
   *       TwkResolver walks S2X (no tweak) -> S1, finds the by-name tweak; the TweakNode is attached to givenSS = S1
   *       (NullCancellationScope) and cached there, but its RHS runs in computeSS = S1.parent.withSIParamsFrom(S2X),
   *       which carries X. The RHS is cancelled, the TweakNode completes with X's OptimusCancellationException.
   *   r2: plain lookup of a in S1 (no cancel scope anywhere): cache hit on the TweakNode, same packed
   *       cancelScopeAndEnvId -> NCSupport fast path -> OptimusCancellationException.
   *   r3: plain lookup of a in S2: same.
   */
  def d17Once(mode: Mode, policy: String, requester: String, target: String, verbose: Boolean): (Boolean, String) = {
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val b = dag.prop("b").base(5)
    val z = dag.prop("z").base(2)
    val c = dag.prop("c").notTweakable().reads(a) // target "c": the poisoned TweakNode sits below a cached combinator
    val sys = new ProbeSystem(dag)
    if (policy == "basic") for (p <- Seq(a, b, z, c)) CacheFixtures.setPolicy(sys.info(p), NCPolicy.Basic)
    val rhs = new Rhs(java.util.List.of(new Ref(b, SAME, SAME)), (_: Key, xs: Array[Long]) => xs.sum * 4 + 134)
    val root = CacheFixtures.newRoot()
    val s1 = sys.stack(root, Stack.of(Level.of(Tw.property(a, rhs, mode))))
    val s2 = CacheFixtures.child(s1, sys.scenario(Level.of(Tw.property(z, 1))))
    val base = if (requester == "S2") s2 else s1 // "S1": the control, X is a copy of the level holding the tweak
    val sX = base.withChildCancellationScope()
    val k = if (target == "c") ProbeDag.key(c) else ProbeDag.key(a)
    val expected = sys.eval(CacheFixtures.newRoot(), ProbeDag.key(b)) * 4 + 134 // b in root = 5 -> 154
    val expK = if (target == "c") expected + 1 /*base*/ + 0 /*arg*/ else expected
    // r1: requester under X; X is cancelled after the requester runs but before the TweakNode's RHS runs.
    val r1 =
      try CacheFixtures.evalIn(sX) { sX.cancelScope.cancel("D17-X"); sys.newKey(k).lookupAndGet.longValue.toString }
      catch { case t: Throwable => "EXC " + brief(t).linesIterator.next() }
    val diag =
      if (!verbose) ""
      else {
        val n = CacheFixtures.lookupNode(s1, sys.newKey(ProbeDag.key(a))).asInstanceOf[NodeTask]
        s" | node a@S1=${n.getClass.getSimpleName} nodeCS=${n.scenarioStack.cancelScope.getClass.getSimpleName}" +
          s" nodeCS.cancelled=${n.scenarioStack.cancelScope.isCancelled} nodeSS==S1:${n.scenarioStack eq s1}" +
          s" packedIdEq=${n.scenarioStack.cancelScopeAndEnvId == s1.cancelScopeAndEnvId}" +
          s" exc=${if (n.isDoneWithException) brief(n.exception) else "-"} hideOnDiffCS=${n.isDoneWithExceptionToHide}" +
          s" excIsX'sCause=${n.isDoneWithException && (n.exception eq sX.cancelScope.cancellationCause)}"
      }
    val r2 = tryEval(sys, s1, k)
    val r3 = tryEval(sys, s2, k)
    val leak = r2.contains("CancellationException") || r3.contains("CancellationException")
    (leak, f"mode=$mode%-9s policy=$policy%-7s requester=$requester target=$target r1(X)=${r1.take(52)}%-52s r2(S1)=${r2.take(52)}%-52s r3(S2)=${r3.take(52)}%-52s expected=$expK rhsRuns=${sys.rhsRuns.get}$diag")
  }

  /**
   * Mid-flight variant (the fuzzer's shape: a timer cancels X while the RHS is suspended, with an uncancelled
   * requester waiting on the same TweakNode). RHS = s + b where s blocks on a gate; X is cancelled while s blocks;
   * the RHS's next read (b, enqueued under X) is cancelled instead of run, so the RHS and the TweakNode fail with X's
   * exception, and the concurrent uncancelled waiter r2 receives it too.
   */
  def d17MidFlight(mode: Mode): (Boolean, String) = {
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val b = dag.prop("b").base(5)
    val z = dag.prop("z").base(2)
    val started = new java.util.concurrent.CountDownLatch(1)
    val release = new java.util.concurrent.CountDownLatch(1)
    val s = dag.prop("s").notTweakable().combine { (_, _) => started.countDown(); release.await(5, TimeUnit.SECONDS); 0L }
    val sys = new ProbeSystem(dag)
    val rhs = new Rhs(java.util.List.of(new Ref(s, SAME, SAME), new Ref(b, SAME, SAME)), (_: Key, xs: Array[Long]) => xs.sum * 4 + 134)
    val root = CacheFixtures.newRoot()
    val s1 = sys.stack(root, Stack.of(Level.of(Tw.property(a, rhs, mode))))
    val s2 = CacheFixtures.child(s1, sys.scenario(Level.of(Tw.property(z, 1))))
    val sX = s2.withChildCancellationScope()
    val k = ProbeDag.key(a)
    def fut(name: String, ss: ScenarioStack) = {
      val f = new java.util.concurrent.FutureTask[String](() => tryEval(sys, ss, k))
      val t = new Thread(() => { CacheFixtures.init(); f.run() }, name); t.setDaemon(true); t.start(); f
    }
    val r1 = fut("d17-r1-X", sX)
    started.await(5, TimeUnit.SECONDS)  // RHS is running in computeSS (carries X), blocked inside s
    val r2 = fut("d17-r2-none", s1)     // uncancelled requester, finds the running TweakNode in S1's cache and waits
    Thread.sleep(50)
    sX.cancelScope.cancel("D17-X")
    release.countDown()
    val res1 = scala.util.Try(r1.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
    val res2 = scala.util.Try(r2.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
    val res3 = tryEval(sys, s1, k)
    val leak = res2.contains("CancellationException") || res3.contains("CancellationException")
    (leak, f"midflight mode=$mode%-9s r1(X)=${res1.take(48)}%-48s r2(none, concurrent)=${res2.take(48)}%-48s r3(none, later)=${res3.take(48)}%-48s expected=154 rhsRuns=${sys.rhsRuns.get}")
  }

  def d17(reps: Int): Unit = {
    CacheFixtures.init()
    println("D17 matrix (one run each; LEAK = an uncancelled lookup threw OptimusCancellationException)")
    for (mode <- Seq(Mode.BYNAME, Mode.BIND_ONCE, Mode.BIND); policy <- Seq("default", "basic"); req <- Seq("S2", "S1"); tgt <- Seq("a", "c")) {
      val (leak, line) = d17Once(mode, policy, req, tgt, verbose = mode == Mode.BYNAME && policy == "default" && tgt == "a")
      println(s"D17 ${if (leak) "LEAK" else "ok  "} $line")
    }
    for (mode <- Seq(Mode.BYNAME, Mode.BIND_ONCE, Mode.BIND)) {
      val (leak, line) = d17MidFlight(mode)
      println(s"D17 ${if (leak) "LEAK" else "ok  "} $line")
    }
    var m = 0
    for (_ <- 0 until reps) if (d17MidFlight(Mode.BYNAME)._1) m += 1
    println(s"D17 RATE midflight mode=BYNAME leaked=$m/$reps")
    var n = 0
    for (_ <- 0 until reps) if (d17Once(Mode.BYNAME, "default", "S2", "a", verbose = false)._1) n += 1
    println(s"D17 RATE mode=BYNAME policy=default requester=S2 target=a leaked=$n/$reps")
  }

  // ============================================================================================================== D18
  import optimus.graph.tracking.{CacheTrackerAccess => T}

  /**
   * t0 (tracker root): w := 8.  t1 (consistent child of t0, or concurrent with conc=true):
   *   x := byName(b*10+g) when ((w >>> 3) & 1) == 1        (the when clause reads w in t1's parent, i.e. t0)
   *   1. evaluate x in t1: when true, TwkResolver caches a TweakNode for x under t1's cache id (TwkResolver:132).
   *      The when clause's dependency on w is merged into the resolver / requester (TwkResolver:76-89), not into
   *      the TweakNode, so the TweakNode does not depend on w.
   *   2. t0: w := 0. Invalidation reaches the requesters of x, not the TweakNode, which stays valid in the cache.
   *   3. evaluate x in t1: the when clause re-runs and says "no tweak", so getNode looks up the *untweaked* key x@t1
   *      (TwkResolver:116-117 -> ScenarioStack.completeGetNode -> lookupAndInsert). The cached TweakNode matches
   *      that key: ProxyPropertyNode reports srcNodeTemplate's propertyInfo/args (ProxyPropertyNode:38-46), so
   *      equalsForCaching and matchesScenario (same cache id) accept it (NodeCacheBase:176-179). The tweaked value
   *      is served where the base value is due, and stays until something invalidates the TweakNode itself.
   * proxy=true: before the flip, two requesters of x in t1 under different cancellation scopes overlap while the RHS
   * is blocked on g, so the second gets a DelayedCSProxyNode (also a ProxyPropertyNode): the same aliasing, and the
   * node the fuzzer saw served. rhsKind=CONST is the control (a stable tweak is not cached, so nothing to alias).
   */
  def d18when(conc: Boolean, proxy: Boolean, order: String, rhsKind: String = "BYNAME"): (Boolean, String) = {
    val dag = new ProbeDag
    val w = dag.prop("w").base(1)
    val b = dag.prop("b").base(5)
    val x = dag.prop("x").base(100)
    val started = new java.util.concurrent.CountDownLatch(1)
    val release = new java.util.concurrent.CountDownLatch(1)
    @volatile var gate = false
    val g = dag.prop("g").notTweakable().combine { (_, _) => if (gate) { started.countDown(); release.await(5, TimeUnit.SECONDS) }; 0L }
    val sys0 = new ProbeSystem(dag)
    val t0 = T.newRoot()
    val t1 = if (conc) T.concurrentChild(t0, "t1") else T.consistentChild(t0, "t1")
    val when = new When((_: Int) => true, new Ref(w, SAME, SAME), (v: Long) => ((v >>> 3) & 1L) == 1L)
    val rhs = new Rhs(java.util.List.of(new Ref(b, SAME, SAME), new Ref(g, SAME, SAME)), (_: Key, xs: Array[Long]) => xs.sum * 10 + 7)
    T.addTweaks(t0, sys0.tweak(Tw.property(w, 8)))
    val xTw = rhsKind match {
      case "CONST" => Tw.propertyWhen(x, when, 57L)
      case m       => Tw.propertyWhen(x, when, rhs, Mode.valueOf(m))
    }
    T.addTweaks(t1, sys0.tweak(xTw))
    val kx = ProbeDag.key(x)
    val log = new StringBuilder
    if (proxy) {
      gate = true
      def evalPlain(): String = try T.evaluate(t1, sys0.newKey(kx)).toString catch { case e: Throwable => brief(e) }
      def evalChildCS(): String = try T.executeEvaluate(t1) { sys0.eval(T.evaluationStack(t1).withChildCancellationScope(), kx) }.toString catch { case e: Throwable => brief(e) }
      val (first, second) = if (order == "childFirst") (evalChildCS _, evalPlain _) else (evalPlain _, evalChildCS _)
      val f1 = new java.util.concurrent.FutureTask[String](() => first())
      new Thread(() => { CacheFixtures.init(); f1.run() }).start()
      started.await(5, TimeUnit.SECONDS)
      val f2 = new java.util.concurrent.FutureTask[String](() => second())
      new Thread(() => { CacheFixtures.init(); f2.run() }).start()
      Thread.sleep(100)
      gate = false
      release.countDown()
      log ++= s"r1=${f1.get(10, TimeUnit.SECONDS)} r2=${f2.get(10, TimeUnit.SECONDS)} "
    } else log ++= s"r1=${T.evaluate(t1, sys0.newKey(kx))} "
    val nb = CacheFixtures.lookupNode(T.rawStack(t1), sys0.newKey(kx)).asInstanceOf[NodeTask]
    val nodeBefore = nb.getClass.getSimpleName
    val whenRunsBefore = sys0.whenRuns.get
    T.addTweaks(t0, sys0.tweak(Tw.property(w, 0))) // when clause now false: x in t1 must be its base value
    val after = T.evaluate(t1, sys0.newKey(kx)).longValue
    val whenRunsFlip = sys0.whenRuns.get - whenRunsBefore
    // Direct cache probe: the *untweaked* key x@t1 (what getNode looks up once the when clause says "no tweak")
    val probe = CacheFixtures.evalIn(T.rawStack(t1)) {
      val ec = OGSchedulerContext.current()
      val base = sys0.newKey(kx)
      val info = base.propertyInfo
      val k = base.prepareForExecutionIn(ec.scenarioStack).asInstanceOf[PropertyNode[java.lang.Long]]
      val hit = optimus.graph.cache.UNodeCache.global.getIfPresent(info, k, ec)
      if (hit == null) "miss" else s"${hit.getClass.getSimpleName} same=${hit eq nb} propertyInfo==x.info:${hit.propertyInfo eq info}"
    }
    log ++= s"[cache probe for untweaked x@t1 after flip: $probe] "
    val na = CacheFixtures.lookupNode(T.rawStack(t1), sys0.newKey(kx)).asInstanceOf[NodeTask]
    val nodeAfter = s"${na.getClass.getSimpleName} sameAsBefore=${na eq nb} beforeInvalid=${nb.isInvalidCache} whenRunsOnFlipEval=$whenRunsFlip"
    val expected = new SequentialEvaluator(Stack.of(Level.of(Tw.property(w, 0)), Level.of(xTw))).eval(kx)
    val wInT1 = T.evaluate(t1, sys0.newKey(ProbeDag.key(w)))
    val t1b = T.consistentChild(t0, "t1b")
    T.addTweaks(t1b, sys0.tweak(xTw))
    val fresh = T.evaluate(t1b, sys0.newKey(kx))
    log ++= s"[sanity w@t1=$wInT1 x@fresh-t1b=$fresh] "
    val stale = after != expected
    (stale, s"rhs=$rhsKind conc=$conc proxy=$proxy order=$order ${log}rawNodeBefore=$nodeBefore after-flip: x@t1=$after expected=$expected rawNode=$nodeAfter${if (stale) " STALE" else ""}")
  }

  def d18(reps: Int): Unit = {
    CacheFixtures.init()
    println("D18 matrix (STALE = x@t1 after the when clause turned false is not its base value)")
    for (k <- Seq("CONST", "BYNAME", "BIND_ONCE", "BIND")) {
      val (st, line) = d18when(conc = false, proxy = false, "-", k)
      println(s"D18 ${if (st) "STALE" else "ok   "} $line")
    }
    for (c <- Seq(false, true); o <- Seq("plainFirst", "childFirst")) {
      val (st, line) = d18when(conc = c, proxy = true, o)
      println(s"D18 ${if (st) "STALE" else "ok   "} $line")
    }
    { val (st, line) = d18when(conc = true, proxy = false, "-"); println(s"D18 ${if (st) "STALE" else "ok   "} $line") }
    var n = 0
    for (_ <- 0 until reps) if (d18when(conc = false, proxy = false, "-")._1) n += 1
    println(s"D18 RATE rhs=BYNAME conc=false proxy=false stale=$n/$reps")
    var m = 0
    for (_ <- 0 until reps) if (d18when(conc = true, proxy = true, "plainFirst")._1) m += 1
    println(s"D18 RATE rhs=BYNAME conc=true proxy=true (DelayedCSProxyNode, the fuzzer's served node) stale=$m/$reps")
  }

  def main(args: Array[String]): Unit = {
    val code = args.toSeq match {
      case Seq("d17", rest @ _*) => d17(rest.headOption.map(_.toInt).getOrElse(10)); 0
      case Seq("d18", rest @ _*) => d18(rest.headOption.map(_.toInt).getOrElse(10)); 0
      case _                     => System.err.println("usage: d17 [reps] | d18 [reps]"); 2
    }
    System.out.flush()
    Runtime.getRuntime.halt(code)
  }
}
