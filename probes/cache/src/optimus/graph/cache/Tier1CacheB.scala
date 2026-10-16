package optimus.graph.cache

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import optimus.core.TPDMask
import optimus.graph._
import optimus.graph.ProbeDag._
import optimus.platform._
import Tier1Cache._

import scala.jdk.CollectionConverters._

object T1cProxyInfo {
  private def fld(c: Class[_], n: String) = { val f = c.getDeclaredField(n); f.setAccessible(true); f }
  private lazy val ipsF = fld(classOf[DelayedXSFTProxyNode[_]], "ips")
  private lazy val completed = fld(classOf[DelayedXSFTProxyNode[_]], "COMPLETED").get(null)
  private lazy val ipsCls = Class.forName("optimus.graph.cache.DelayedXSFTProxyNode$InProgressState")
  private def g(o: AnyRef, n: String): AnyRef = fld(ipsCls, n).get(o)
  def ipsOf(p: AnyRef): AnyRef = p match {
    case x: DelayedXSFTProxyNode[_] => ipsF.get(x)
    case _                          => null
  }
  def describeIps(p: AnyRef): String = ipsOf(p) match {
    case null => "ips=null"
    case s if s eq completed => "ips=COMPLETED"
    case s =>
      val by = g(s, "promisedBy")
      val down = g(s, "promisedToDown").asInstanceOf[java.util.ArrayList[_]]
      s"ips{promisedBy=${if (by eq p) "self" else String.valueOf(by)} doSelfEvaluate=${g(s, "doSelfEvaluate")} mustHaveValue=${g(s, "mustHaveValue")} " +
        s"promisedToUp=${g(s, "promisedToUp") ne null} promisedToDown=${if (down eq null) 0 else down.size}}"
  }
  def isXSFTProxy(n: AnyRef): Boolean = n.isInstanceOf[DelayedXSFTProxyNode[_]]
  def kind(n: AnyRef): String = if (n == null) "null" else n.getClass.getSimpleName
}

// =====================================================================================================================
// T13 (R11): batcher stall -> fixWaiterChainForXSFTFromBatcher (S1._lock, _visitLock) -> breakProxyChains ->
// offerValue -> cancelled proxy completes -> waiter's onChildCompleted with affinity -> S2.enqueue (S2._lock), while
// S2 is in detectCycles (S2._lock -> _visitLock).
// =====================================================================================================================
object T1cProbe13 {
  def run(variant: String): Unit = {
    val dag = new ProbeDag
    val u = dag.prop("t13U").base(1) // unrelated tweakable for the child scenario
    val sys = new ProbeSystem(dag)
    val fp = new T1cProp(new ProbeProperty(dag.prop("t13F").notTweakable()))
    val stub = new T1cBatchStub("t13")
    stub.armed = false
    val s1 = T1cGraphAccess.newScheduler("t13S1-", 2)
    val s2 = T1cGraphAccess.newScheduler("t13S2-", 2)
    val gate = new T1cGate("t13-batch", (g, ec) => stub.register(g, ec.scheduler))
    fp.deps = n => if (n.scenarioStack()._cacheID eq rootID) Seq(gate) else Nil
    fp.compute = (n, xs) => xs.headOption.map(_.asInstanceOf[java.lang.Long].longValue).getOrElse(0L) + n.arg
    CacheFixtures.setAssumeDependsOn(fp.info) // empty mask: minSS is the root for every child; policy XSFT
    val root = CacheFixtures.newRoot()
    rootID = root._cacheID
    val cs = CancellationScope.newScope()
    val ssC = sys.stack(root, Stack.of(Level.of(Tw.property(u, 5)))).withCancellationScope(cs)
    kv("keepTaskSchedulerAffinity", Settings.keepTaskSchedulerAffinity, "policy", fp.info.cachePolicy)

    // hook: the first offerValue on a thread holding _visitLock sleeps, widening the S1._lock+_visitLock window
    val offersUnderVisitLock = new AtomicInteger
    val slept = new AtomicBoolean(false)
    _root_.probes.CacheDelay.onHit("xsft.offer.beforeSync", _ => {
      if (T1cGraphAccess.holdsVisitLock) {
        offersUnderVisitLock.incrementAndGet()
        if (variant != "nodelay" && slept.compareAndSet(false, true)) {
          println(s"T13-OFFER-UNDER-VISITLOCK thread=${Thread.currentThread().getName} heldS1Lock=${T1cBatchStub.holdsSchedulerLock(s1)} -> sleep 300 ms")
          Thread.sleep(300)
        }
      }
    })
    // cross-scheduler enqueue: observe CL.run thread
    val r1 = new T1cClient(fp.key(0), "R1@root")
    r1.attach(root)
    val cl = new T1cClient(fp.key(0), "CL@C")
    cl.attach(ssC)
    val r1Done = new CountDownLatch(1)
    val clDone = new CountDownLatch(1)
    def daemon(name: String)(f: => Unit): Thread = { val t = new Thread(() => f, name); t.setDaemon(true); t.start(); t }
    // 1. S1: root client -> U (XSFT, self-evaluating at root) -> src F@root -> gate (pending batch member)
    daemon("t13-A-S1") {
      val ec = s1.newEvaluationContext(root); OGSchedulerContext.setCurrent(ec)
      try ec.runAndWait(r1) catch { case t: Throwable => println(s"R1 threw $t") }
      r1Done.countDown()
    }
    val t0 = System.nanoTime()
    while (stub.pending.isEmpty && System.nanoTime() - t0 < 5e9) Thread.sleep(1)
    tick()
    val u0 = r1.looked
    kv("gateRegistered", !stub.pending.isEmpty, "gateRanOn", gate.ranOn, "U", T1cProxyInfo.kind(u0), "Uips", T1cProxyInfo.describeIps(u0).replace(' ', '_'))
    // 2. S2: client under child scenario C (own cancel scope) -> P (XSFT proxy at C) WAITs on U
    daemon("t13-B-S2") {
      val ec = s2.newEvaluationContext(ssC); OGSchedulerContext.setCurrent(ec)
      try ec.runAndWait(cl) catch { case t: Throwable => println(s"CL threw ${t.getClass.getSimpleName}") }
      clDone.countDown()
    }
    while ((cl.looked == null || T1cGraphAccess.waitingOn(cl.looked) == null) && System.nanoTime() - t0 < 5e9) Thread.sleep(1)
    tick()
    val p = cl.looked
    val pWaitsOnU = p != null && (T1cGraphAccess.waitingOn(p) eq u0)
    val uIpsBefore = T1cProxyInfo.describeIps(u0)
    val pIpsBefore = T1cProxyInfo.describeIps(p)
    val maskBefore = fp.info.dependsOnTweakMask
    kv("P", T1cProxyInfo.kind(p), "PwaitsOnU", pWaitsOnU, "Pips", pIpsBefore.replace(' ', '_'), "Uips", uIpsBefore.replace(' ', '_'), "CLranOn", cl.ranOn)
    // 3. cancel P's scope (P is parked in WAIT; nothing runs)
    cs.cancel("T13: cancel the child scope during the batch stall")
    // 4. S2 in detectCycles: rounds of two-node cycles
    val stopCycles = new AtomicBoolean(false)
    val cycRounds = new AtomicInteger
    daemon("t13-C-S2cyc") {
      val ec = s2.newEvaluationContext(root); OGSchedulerContext.setCurrent(ec)
      while (!stopCycles.get) {
        val a = new T1cCycNode("cA" + cycRounds.get); val b = new T1cCycNode("cB" + cycRounds.get)
        a.partner = b; b.partner = a; a.first = true
        a.attach(root); b.attach(root)
        try ec.runAndWait(a) catch { case _: Throwable => }
        cycRounds.incrementAndGet()
      }
    }
    while (cycRounds.get < 20 && System.nanoTime() - t0 < 5e9) Thread.sleep(1)
    tick()
    // 5. release the batcher: S1 (stalled, under _lock in runOnStalledCallbacks) runs fixWaiterChainForXSFTFromBatcher
    stub.armed = true
    val cyc0 = cycRounds.get
    watchdogExtra = () => s"fixCalls=${stub.fixCalls.get} offersUnderVisitLock=${offersUnderVisitLock.get} cycRounds=${cycRounds.get} " +
      s"P.done=${p != null && p.isDone} CL.ranOn=${cl.ranOn}"
    val finished = r1Done.await(8, TimeUnit.SECONDS) & clDone.await(2, TimeUnit.SECONDS)
    tick()
    var deadlocked = 0
    if (!finished) {
      val dl = java.lang.management.ManagementFactory.getThreadMXBean.findDeadlockedThreads()
      deadlocked = if (dl == null) 0 else dl.length
      println(s"T13-NOT-FINISHED deadlocked=$deadlocked cycRounds=${cycRounds.get}")
      dumpThreads("t13", "og.")
    }
    stopCycles.set(true)
    val maskAfter = fp.info.dependsOnTweakMask
    val pOutcome =
      if (p == null) "null" else if (!p.isDone) "not done" else if (p.isDoneWithException) s"EX(${exClass(p.exception)})" else s"value ${p.resultObject}"
    v(if (deadlocked > 0) "DEMONSTRATED" else if (finished) "NOT_REPRODUCED" else "ERROR",
      s"variant=$variant findDeadlockedThreads=$deadlocked keepTaskSchedulerAffinity=${Settings.keepTaskSchedulerAffinity} policy=${fp.info.cachePolicy} | U=${T1cProxyInfo.kind(u0)} " +
        s"P=${T1cProxyInfo.kind(p)} P.waitingOn(U)=$pWaitsOnU before: P[$pIpsBefore] U[$uIpsBefore] | stub: checks=${stub.checks.get} fixCalls=${stub.fixCalls.get} " +
        s"fixReturnedTrue=${stub.fixReturnedTrue.get} fixThread=${stub.fixThread} heldS1Lock=${stub.fixThreadHeldSchedulerLock} stalls=${stub.stalls.get} " +
        s"offerValue under _visitLock=${offersUnderVisitLock.get} | mask before=${maskBefore.stringEncoded} after=${maskAfter.stringEncoded} poison=${maskAfter.allBitsAreSet} | " +
        s"P outcome=$pOutcome CL re-ran on=${cl.ranOn} R1=${if (r1.isDone) (if (r1.isDoneWithException) "EX" else r1.resultObject) else "not done"} " +
        s"| S2 cycle rounds before release=$cyc0 total=${cycRounds.get} finished=$finished")
  }
  @volatile var rootID: SSCacheID = _
}

// =====================================================================================================================
// T16 (P1): XSFT hit rate before and after one batcher stall poisons the property mask
// =====================================================================================================================
object T1cProbe16 {
  def run(variant: String): Unit = {
    val dag = new ProbeDag
    val k = dag.prop("t16K").base(100) // F's real dependency
    val us = (1 to 4).map(i => dag.prop(s"t16U$i").base(0)) // unrelated tweakables
    val uf = dag.prop("t16Ufresh").base(0)
    val sys = new ProbeSystem(dag)
    CacheFixtures.setTweakableID(sys.info(k), 3)
    us.zipWithIndex.foreach { case (p, i) => CacheFixtures.setTweakableID(sys.info(p), 11 + i) }
    CacheFixtures.setTweakableID(sys.info(uf), 20)
    val fp = new T1cProp(new ProbeProperty(dag.prop("t16F").notTweakable()))
    val stub = new T1cBatchStub("t16")
    val gate = new T1cGate("t16-batch", (g, ec) => stub.register(g, ec.scheduler))
    fp.deps = n => if (n.arg == 999) Seq(sys.newKey(key(k)), gate) else Seq(sys.newKey(key(k)))
    fp.compute = (n, xs) => xs(0).asInstanceOf[java.lang.Long].longValue + n.arg
    CacheFixtures.setAssumeDependsOn(fp.info, 3)
    val root = CacheFixtures.newRoot()
    val sibs = us.zipWithIndex.map { case (p, i) => sys.stack(root, Stack.of(Level.of(Tw.property(p, i + 1)))) }
    var mismatches = 0
    def req(ss: ScenarioStack, arg: Int): Unit = {
      val r = CacheFixtures.eval(ss, fp.key(arg)).longValue
      if (r != 100L + arg) mismatches += 1
    }
    val N = 10000
    def phaseSibs(arg: Int): (Int, Long) = {
      val r0 = fp.runs.get; val t = System.nanoTime()
      for (i <- 0 until N) { req(sibs(i % 4), arg); if (i % 1000 == 0) tick() }
      (fp.runs.get - r0, (System.nanoTime() - t) / 1000000)
    }
    var freshCounter = 0
    def phaseFresh(arg: Int): (Int, Long) = {
      val r0 = fp.runs.get; val t = System.nanoTime()
      for (i <- 0 until N) {
        freshCounter += 1
        req(sys.stack(root, Stack.of(Level.of(Tw.property(uf, freshCounter)))), arg)
        if (i % 1000 == 0) tick()
      }
      (fp.runs.get - r0, (System.nanoTime() - t) / 1000000)
    }
    def pct(runs: Int) = f"${100.0 * (N - runs) / N}%.2f%%"
    val mask0 = fp.info.dependsOnTweakMask
    val (b1, tb1) = phaseSibs(0)
    val (b2, tb2) = phaseFresh(0)
    val mask1 = fp.info.dependsOnTweakMask
    // one stall: root request of F(999) whose source waits on a pending batch member
    val r999 = tryEval(CacheFixtures.eval(root, fp.key(999)).longValue)
    tick()
    val mask2 = fp.info.dependsOnTweakMask
    val (a1, ta1) = phaseSibs(1)
    val (a2, ta2) = phaseFresh(1)
    val (a3, ta3) = phaseFresh(0) // the same key as before the stall, fresh scenarios
    val mask3 = fp.info.dependsOnTweakMask
    val collapsed = mask2.allBitsAreSet && a2 > N / 2
    v(if (collapsed) "DEMONSTRATED" else if (mask2.allBitsAreSet) "QUANTIFIED" else "NOT_REPRODUCED",
      s"$xsftLabel policy=${fp.info.cachePolicy} K.id=3 unrelated ids=11..14,20 | mask initial=${mask0.stringEncoded} after phase1=${mask1.stringEncoded} | " +
        s"BEFORE stall: 4 fixed siblings arg0: runs=$b1/$N hit=${pct(b1)} (${tb1}ms); fresh sibling per request arg0: runs=$b2/$N hit=${pct(b2)} (${tb2}ms) | " +
        s"stall: F(999)@root=${show(r999)} stub checks=${stub.checks.get} fixCalls=${stub.fixCalls.get} fixReturnedTrue=${stub.fixReturnedTrue.get} heldLock=${stub.fixThreadHeldSchedulerLock} stalls=${stub.stalls.get} " +
        s"mask after=${mask2.stringEncoded.take(60)}${if (mask2.stringEncoded.length > 60) "..." else ""} poison(allBitsAreSet)=${mask2.allBitsAreSet} bits=${mask2.countOfSetBits} | " +
        s"AFTER: 4 fixed siblings arg1: runs=$a1/$N hit=${pct(a1)} (${ta1}ms); fresh sibling per request arg1: runs=$a2/$N hit=${pct(a2)} (${ta2}ms); " +
        s"fresh siblings arg0 (key cached at root before the stall): runs=$a3/$N hit=${pct(a3)} (${ta3}ms) | mask at end poison=${mask3.allBitsAreSet} | value mismatches=$mismatches")
  }
}

// =====================================================================================================================
// T19 (W3, W4): cancel-scope matrix on one running key
// =====================================================================================================================
object T1cProbe19 {
  val relations = Array("same", "child", "sibling", "cancelledSibling")

  def run(variant: String): Unit = {
    val rounds = Integer.getInteger("t19.rounds", 100000).intValue
    val threads = 8
    val dag = new ProbeDag
    val fp = new T1cProp(new ProbeProperty(dag.prop("t19F").notTweakable()))
    val ext = java.util.concurrent.Executors.newScheduledThreadPool(2, r => { val t = new Thread(r, "t19-ext"); t.setDaemon(true); t })
    val root = CacheFixtures.newRoot()
    val sched = optimus.platform.EvaluationContext.scheduler
    // per-round state hangs off the gate
    final class Round(val arg: Int, val rnd: java.util.SplittableRandom) {
      val rs: CancellationScope = CancellationScope.newScope()
      val o: CancellationScope = rs.childScope()
      val cancelOwner: Boolean = rnd.nextInt(4) == 0
      val others: Array[(Int, CancellationScope, T1cClient)] = Array.tabulate(3) { _ =>
        val rel = rnd.nextInt(4)
        val s = rel match { case 0 => o; case 1 => o.childScope(); case _ => rs.childScope() }
        (rel, s, new T1cClient(fp.key(arg), relations(rel)))
      }
      val done = new CountDownLatch(4)
    }
    val rounds0 = new java.util.concurrent.ConcurrentHashMap[Integer, Round]()
    fp.deps = n => {
      val r = rounds0.get(n.arg)
      Seq(new T1cGate("t19", (g, ec) => {
        // owner's F is running and waiting on this gate: now issue the other requests
        r.others.foreach { case (_, _, c) => ec.enqueue(c) }
        val d = r.rnd.nextInt(300)
        ext.schedule((() => {
          // all requests are made while F is running: wait (bounded) until every other client has looked up
          val t0 = System.nanoTime()
          while (r.others.exists { case (_, _, c) => c.looked == null && !c.isDone } && System.nanoTime() - t0 < 50000000L) Thread.onSpinWait()
          if (r.cancelOwner) r.o.cancel("T19 owner scope cancelled while F is running")
          g.complete(7, sched)
        }): Runnable, d.toLong, TimeUnit.MICROSECONDS)
      }))
    }
    fp.compute = (n, xs) => 1000L * n.arg + xs(0).asInstanceOf[java.lang.Long].longValue
    // counters
    val stats = new java.util.concurrent.ConcurrentHashMap[String, AtomicLong]()
    def inc(k: String): Unit = stats.computeIfAbsent(k, _ => new AtomicLong).incrementAndGet()
    val mismatch = new AtomicLong
    val leak = new AtomicLong
    val examples = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    _root_.probes.CacheDelay.startCounting()
    def record(r: Round, rel: String, scope: CancellationScope, c: T1cClient): Unit = {
      val n = c.looked
      val kind = T1cProxyInfo.kind(n)
      inc(s"lookup.$rel.$kind")
      val expected = 1000L * r.arg + 7
      val scopeCancelled = scope.isCancelled
      if (n.isDoneWithException) {
        val ex = n.exception
        val cancellation = ex.isInstanceOf[optimus.graph.OptimusCancellationException] ||
          rootCause(ex).isInstanceOf[optimus.graph.OptimusCancellationException]
        inc(s"outcome.$rel.${if (cancellation) "cancelled" else exClass(ex)}${if (scopeCancelled) ".scopeCancelled" else ""}")
        if (!scopeCancelled) {
          leak.incrementAndGet()
          if (examples.size < 5) examples.add(s"round=${r.arg} rel=$rel kind=$kind ex=${exClass(ex)} ownerCancelled=${r.cancelOwner} requesterScopeCancelled=false")
        }
      } else {
        val got = n.resultObject.asInstanceOf[java.lang.Long].longValue
        inc(s"outcome.$rel.value${if (scopeCancelled) ".scopeCancelled" else ""}")
        if (got != expected) {
          mismatch.incrementAndGet()
          if (examples.size < 5) examples.add(s"round=${r.arg} rel=$rel kind=$kind got=$got expected=$expected")
        }
      }
    }
    val nextRound = new AtomicInteger
    val hung = new AtomicBoolean(false)
    val workers = (0 until threads).map { ti =>
      val t = new Thread(() => {
        CacheFixtures.init()
        val rnd = new java.util.SplittableRandom(ti * 7919L + 1)
        var arg = nextRound.getAndIncrement()
        while (arg < rounds && !hung.get) {
          val r = new Round(arg, rnd.split())
          rounds0.put(arg, r)
          val owner = new T1cClient(fp.key(arg), "owner")
          owner.attach(root.withCancellationScope(r.o))
          // record each requester when the node it looked up completes (independent of the client's own cancellation)
          def watch(rel: String, scope: CancellationScope, c: T1cClient): Unit =
            c.afterLookup = cc => cc.looked.continueWith(new NodeAwaiter {
              override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = { record(r, rel, scope, cc); r.done.countDown() }
            }, sched)
          watch("owner", r.o, owner)
          r.others.foreach { case (rel, s, c) =>
            c.attach(root.withCancellationScope(s))
            c.continueWith(new NodeAwaiter {
              override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit =
                if (c.looked == null) { inc(s"notLookedUp.${relations(rel)}"); r.done.countDown() }
            }, sched)
            watch(relations(rel), s, c)
            if (rel == 3) {
              val prev = c.afterLookup
              c.afterLookup = cc => { prev(cc); s.cancel("T19 cancelled sibling after its lookup") }
            }
          }
          sched.enqueue(owner)
          if (!r.done.await(10, TimeUnit.SECONDS)) {
            hung.set(true)
            println(s"T19-ROUND-HANG round=$arg ownerCancelled=${r.cancelOwner} owner.looked=${T1cProxyInfo.kind(owner.looked)} done=${owner.looked != null && owner.looked.isDone} " +
              r.others.map { case (rel, s, c) => s"${relations(rel)}:${T1cProxyInfo.kind(c.looked)}:${c.looked != null && c.looked.isDone}:clientDone=${c.isDone}" }.mkString(" "))
          }
          rounds0.remove(arg)
          if ((arg & 1023) == 0) tick()
          arg = nextRound.getAndIncrement()
        }
      }, s"t19-w$ti")
      t.setDaemon(true); t.start(); t
    }
    val t0 = System.nanoTime()
    workers.foreach { w => while (w.isAlive) { w.join(1000); if (!hung.get) tick() } }
    val wall = (System.nanoTime() - t0) / 1000000
    val counts = _root_.probes.CacheDelay.counts()
    val st = stats.asScala.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(" ")
    val proxies = stats.asScala.filter { case (k, _) => k.startsWith("lookup.") && k.contains("Proxy") }.values.map(_.get).sum
    val lookups = stats.asScala.filter(_._1.startsWith("lookup.")).values.map(_.get).sum
    val demo = mismatch.get > 0 || leak.get > 0
    v(if (demo) "DEMONSTRATED" else if (hung.get) "ERROR" else "QUANTIFIED",
      s"$xsftLabel policy=${fp.info.cachePolicy} rounds=${math.min(nextRound.get, rounds)} threads=$threads wallMs=$wall F.runs=${fp.runs.get} lookups=$lookups proxiesReturned=$proxies " +
        s"oracleMismatches=${mismatch.get} cancelledExceptionToUncancelledRequester=${leak.get} hung=${hung.get} nc.put.cas=${counts.getOrDefault("nc.put.cas", 0L)} | $st" +
        (if (examples.isEmpty) "" else " | examples: " + examples.asScala.mkString("; ")))
  }
}
