package optimus.graph.cache

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import optimus.graph._
import optimus.graph.ProbeDag._
import optimus.platform._
import Tier1Cache._

import scala.jdk.CollectionConverters._

/** Describes a node's XS state: owner flag, scenario stack's listener / recorded tweakables. */
object T1cXSInspect {
  def listenerOf(n: NodeTask): String = {
    if (n == null) return "null"
    val ss = n.scenarioStack()
    if (ss == null) return "ss=null"
    val tl = ss.tweakableListener
    val rt = tl match {
      case r: RecordedTweakables => r
      case _                     => null
    }
    val desc =
      if (rt != null)
        s"RecordedTweakables(empty=${rt.isEmpty} tweaked=${rt.tweaked.length} tweakable=${rt.tweakable.length} hashes=${rt.tweakableHashes.length}${
            if (rt.tweaked.nonEmpty) " " + rt.tweaked.map(_.toString.replaceAll("\\s+", " ").take(80)).mkString("[", ";", "]") else ""
          })"
      else tl.getClass.getSimpleName
    s"owner=${n.isXScenarioOwner} cs=${ss.cancelScope.isCancelled} listener=$desc"
  }
}

// =====================================================================================================================
// T1 (D1): cancelled XS proxy scrubs its published owner's recording
// =====================================================================================================================
object T1cProbe1 {
  def run(variant: String): Unit = {
    val cancel = variant == "cancel"
    val dag = new ProbeDag
    val k = dag.prop("t1K").base(0) // tweakable leaf: 0 + arg
    val cs = CancellationScope.newScope()
    @volatile var armed = false
    val atCompletion = new AtomicReference[String]("<not captured>")
    val f = dag.prop("t1F").notTweakable().reads(k).combine { (_, xs) =>
      if (armed) {
        armed = false
        // Owner is running: register an awaiter that snapshots the owner at completion (after onOwnerCompleted, before
        // the proxy's re-run), then cancel the requester's scope so that the proxy's re-run goes to cancelInsteadOfRun.
        val ec = OGSchedulerContext.current()
        val owner = ec.getCurrentNodeTask
        owner.continueWith(new NodeAwaiter {
          override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
            atCompletion.set(T1cXSInspect.listenerOf(node) + s" done=${node.isDone} usable=${node.isDoneWithUsableResult}")
            // owner is DONE (recording frozen); the proxy was at most enqueued by its own onChildCompleted, not re-run
            if (cancel) cs.cancel("T1 cancel after owner completion, before proxy re-run")
          }
        }, ec)
      }
      xs.sum
    }
    val sys = new ProbeSystem(dag)
    sys.info(f).setFavorReuse(true)
    val policy = sys.info(f).cachePolicy
    _root_.probes.CacheDelay.startCounting()
    val xsRunsBefore = new AtomicInteger
    _root_.probes.CacheDelay.onHit("xs.run", _ => xsRunsBefore.incrementAndGet())
    val root = CacheFixtures.newRoot()
    val stA = Stack.of(Level.of(Tw.property(k, 1)))
    val stB = Stack.of(Level.of(Tw.property(k, 2)))
    val ssA = sys.stack(root, stA)
    val ssB = sys.stack(root, stB)
    val ssAc = ssA.withCancellationScope(cs)
    val oracleA = new SequentialEvaluator(stA).eval(key(f))
    val oracleB = new SequentialEvaluator(stB).eval(key(f))
    armed = true
    val ra = tryEval(sys.eval(ssAc, key(f)))
    tick()
    val owner = sys.pp(f).lastRun.get(key(f))
    val afterA = T1cXSInspect.listenerOf(owner)
    val runsAfterA = sys.pp(f).runs.get
    val rb = tryEval(sys.eval(ssB, key(f)))
    val runsAfterB = sys.pp(f).runs.get
    val afterB = T1cXSInspect.listenerOf(owner)
    val xsRuns = _root_.probes.CacheDelay.count("xs.run")
    val bWrong = rb.isRight && rb.toOption.get != oracleB
    val bGotA = rb.isRight && rb.toOption.get == oracleA
    val outcome = if (bWrong) "DEMONSTRATED" else "NOT_REPRODUCED"
    v(outcome, s"variant=$variant $xsftLabel policy=$policy favorReuseIsXS=${sys.info(f).getFavorReuseIsXS} " +
      s"F@A(child scope${if (cancel) ", cancelled at owner completion" else ""})=${show(ra)} oracleA=$oracleA scopeCancelled=${cs.isCancelled} | " +
      s"owner at completion: [${atCompletion.get}] | owner after A's request: [$afterA] | " +
      s"F@B=${show(rb)} oracleB=$oracleB B-got-A's-value=$bGotA F.runs after A=$runsAfterA after B=$runsAfterB xs.run hits=$xsRuns | owner after B: [$afterB]")
  }
}

// =====================================================================================================================
// T2 (D4): SI node -> non-SI cacheable -> never-tweaked tweakable; then the non-SI node at the root
// =====================================================================================================================
object T1cProbe2 {
  def run(variant: String): Unit = {
    val dag = new ProbeDag
    val l = dag.prop("t2L").base(7) // tweakable, never tweaked
    val n = dag.prop("t2N").notTweakable().reads(l) // cacheable, non-SI
    val s = dag.prop("t2S").notTweakable().si().reads(n) // SI
    val sys = new ProbeSystem(dag)
    val root = CacheFixtures.newRoot()
    val oracleN = new SequentialEvaluator(Stack.of()).eval(key(n))
    val rs = tryEval(sys.eval(root, key(s)))
    val sEx = rs.left.toOption.orNull
    val nNodeAfterS = sys.pp(n).lastRun.get(key(n))
    val nStateAfterS =
      if (nNodeAfterS == null) "never ran"
      else s"ss=${if (nNodeAfterS.scenarioStack() eq null) "null" else "cacheID " + nNodeAfterS.scenarioStack()._cacheID.id + " SI=" + nNodeAfterS.scenarioStack().isScenarioIndependent} " +
        s"done=${nNodeAfterS.isDone} withException=${nNodeAfterS.isDoneWithException} usable=${nNodeAfterS.isDoneWithUsableResult} " +
        s"ex=${exClass(nNodeAfterS.exception)} RT=${nNodeAfterS.exception.isInstanceOf[optimus.exceptions.RTExceptionTrait]}"
    val nRuns1 = sys.pp(n).runs.get
    val rn = tryEval(sys.eval(root, key(n)))
    val nRuns2 = sys.pp(n).runs.get
    val got = tryEval(sys.lookupNode(root, key(n)))
    val gotDesc = got match {
      case Right(node) => s"${node.getClass.getSimpleName} isDoneWithException=${node.isDoneWithException} ex=${exClass(node.exception)}"
      case Left(t)     => s"lookupNode threw ${exClass(t)}"
    }
    val isd = rn.left.toOption.exists(t => t.isInstanceOf[IllegalScenarioDependenceException] || rootCause(t).isInstanceOf[IllegalScenarioDependenceException])
    v(if (isd) "DEMONSTRATED" else "NOT_REPRODUCED",
      s"$xsftLabel N.policy=${sys.info(n).cachePolicy} root cacheID=${root._cacheID.id} siStack cacheID=${root.siRoot._cacheID.id} | " +
        s"S@root=${show(rs)} (ISD expected; RT=${sEx != null && sEx.isInstanceOf[optimus.exceptions.RTExceptionTrait]}) | N node after S: [$nStateAfterS] | " +
        s"N@root=${show(rn)} oracle=$oracleN N.runs after S=$nRuns1 after root request=$nRuns2 (no new run = cache hit) | root lookup returned [$gotDesc]")
  }
}

// =====================================================================================================================
// T15 (F8): cacheable tag reader under two sibling stacks with different plugin-tag values
// =====================================================================================================================
object T1cProbe15 {
  def run(variant: String): Unit = {
    val dag = new ProbeDag
    val t = dag.prop("t15T").notTweakable().kind(Kind.TAG_READER)
    val a = dag.prop("t15A").base(1)
    val sys = new ProbeSystem(dag)
    val root = CacheFixtures.newRoot()
    val sb = new StringBuilder(s"$xsftLabel T.policy=${sys.info(t).cachePolicy}")
    var demo = false
    // (a) withTag directly on the root
    val s1 = CacheFixtures.withTag(root, 1)
    val s2 = CacheFixtures.withTag(root, 2)
    val v1 = tryEval(sys.eval(s1, key(t)))
    val v2 = tryEval(sys.eval(s2, key(t)))
    val r0 = sys.pp(t).runs.get
    demo |= v1.isRight && v1 == v2
    sb.append(s" | (a) root.withPluginTag(1/2): ids ${s1._cacheID.id}/${s2._cacheID.id} sameCacheID=${s1._cacheID eq s2._cacheID} values ${show(v1)}/${show(v2)} oracle 1/2 runs=$r0")
    // (b) siblings created with createChild(scenario, tag) on the same parent (same scenario)
    val sc = sys.scenario(Level.of(Tw.property(a, 5)))
    val c1 = CacheFixtures.child(root, sc, java.lang.Long.valueOf(10))
    val c2 = CacheFixtures.child(root, sc, java.lang.Long.valueOf(20))
    val w1 = tryEval(sys.eval(c1, key(t)))
    val w2 = tryEval(sys.eval(c2, key(t)))
    demo |= w1.isRight && w1 == w2
    sb.append(s" | (b) createChild([a:=5], tag 10/20): ids ${c1._cacheID.id}/${c2._cacheID.id} sameCacheID=${c1._cacheID eq c2._cacheID} values ${show(w1)}/${show(w2)} oracle 10/20 runs=${sys.pp(t).runs.get - r0}")
    // (c) tag on a child, then a non-tag-reader child under each: does the stale value propagate through a combinator?
    val dag2 = new ProbeDag
    val t2 = dag2.prop("t15T2").notTweakable().kind(Kind.TAG_READER)
    val c = dag2.prop("t15C").notTweakable().reads(t2)
    val sys2 = new ProbeSystem(dag2)
    val x1 = tryEval(sys2.eval(CacheFixtures.withTag(root, 100), key(c)))
    val x2 = tryEval(sys2.eval(CacheFixtures.withTag(root, 200), key(c)))
    demo |= x1.isRight && x1 == x2
    sb.append(s" | (c) combinator over tag reader, tags 100/200: values ${show(x1)}/${show(x2)} oracle 100/200 C.runs=${sys2.pp(c).runs.get} T2.runs=${sys2.pp(t2).runs.get}")
    v(if (demo) "DEMONSTRATED" else "NOT_REPRODUCED", sb.toString)
  }
}

// =====================================================================================================================
// T3 (D5): impure tweak translator; two concurrent sync requesters of the same cached expander
// =====================================================================================================================
/** Key class for the tweak-handler property: transformTweak is scripted by T1cProbe3. */
final class T1cT3HandlerNode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key, val target: ProbeDag.Prop)
    extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = pp.prop.combine.apply(dkey, Array.emptyLongArray)
  override def transformTweak(value: Any): Seq[Tweak] = {
    T1cProbe3.onTransform()
    Seq(SimpleValueTweak(sys.newKey(target): NodeKey[java.lang.Long])(value.asInstanceOf[java.lang.Long]))
  }
}

object T1cProbe3 {
  final val TWEAKHANDLER = 1L << 7 // private in NodeTaskInfo
  val calls = new AtomicInteger
  @volatile var impure = true
  val t2AboutToRequest = new CountDownLatch(1)
  val t2Done = new CountDownLatch(1)
  @volatile var t1WaitedForT2Ms = -1L
  @volatile var t2DoneDuringRetry = false
  @volatile var activity = false
  val activityRuns = new AtomicInteger

  def onTransform(): Unit = {
    val n = calls.incrementAndGet()
    if (n == 1 && impure) {
      // first call (thread 1, inside the cached expander): wait until thread 2 has looked up the same expander and is
      // waiting on it, then fail the trust check
      t2AboutToRequest.await(5, TimeUnit.SECONDS)
      Thread.sleep(500)
      throw new GraphFailedTrustException("T3 probe: impure tweak translator")
    } else if (n == 2 && impure) {
      // retry inside the same expander, after replace + invalidateCache: the node is not done, but isDoneEx() is true.
      val t0 = System.nanoTime()
      if (activity) {
        // unrelated graph activity while req1 is inside the retry: global enqueues hand work to waiting threads
        // (OGScheduler.enqueue -> wakeupThread), so a parked requester re-checks isDoneEx()
        val sched = OGSchedulerContext.current().scheduler.asInstanceOf[OGScheduler]
        val ss = OGSchedulerContext.current().scenarioStack
        val helper = new Thread(() => {
          var i = 0
          while (t2Done.getCount > 0 && i < 200) {
            val n = new CompletableNode[Integer] { override def run(ec: OGSchedulerContext): Unit = { activityRuns.incrementAndGet(); completeWithResult(1, ec) } }
            n.attach(ss)
            sched.enqueue(n)
            i += 1
            Thread.sleep(10)
          }
        }, "t3-activity")
        helper.setDaemon(true); helper.start()
      }
      t2DoneDuringRetry = t2Done.await(3, TimeUnit.SECONDS)
      t1WaitedForT2Ms = (System.nanoTime() - t0) / 1000000
    }
  }

  def run(variant: String): Unit = {
    impure = variant != "control"
    activity = variant != "quiet"
    val dag = new ProbeDag
    val h = dag.prop("t3H").base(0) // tweakable, with TWEAKHANDLER
    val l2 = dag.prop("t3L2").base(0) // what the handler tweaks
    val sys = new ProbeSystem(dag)
    // TwkPropertyInfo0 (a PropertyTarget) refuses TWEAKHANDLER in its constructor; plugin-generated properties with a
    // tweak handler use a non-PropertyTarget info. Set the flag after construction instead.
    val ff = classOf[NodeTaskInfo].getDeclaredField("_flags"); ff.setAccessible(true)
    ff.setLong(sys.info(h), ff.getLong(sys.info(h)) | TWEAKHANDLER)
    val root = CacheFixtures.newRoot()
    val hKey = new T1cT3HandlerNode(sys, sys.pp(h), key(h), l2)
    val tweak = SimpleValueTweak(hKey: NodeKey[java.lang.Long])(java.lang.Long.valueOf(42))
    def scenario = Scenario(Seq(tweak))
    val r1 = new AtomicReference[Either[Throwable, Long]]()
    val r2 = new AtomicReference[Either[Throwable, Long]]()
    val expanderNodes = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    def request(r: AtomicReference[Either[Throwable, Long]]): Unit = {
      CacheFixtures.init()
      r.set(tryEval {
        val ss = root.createChild(scenario, this)
        sys.eval(ss, key(l2))
      })
    }
    val th1 = new Thread(() => request(r1), "t3-req1")
    val th2 = new Thread(() => {
      CacheFixtures.init()
      // wait until thread 1 is inside the expander (first transformTweak call), then request the same scenario
      while (calls.get == 0) Thread.onSpinWait()
      t2AboutToRequest.countDown()
      request(r2)
      t2Done.countDown()
    }, "t3-req2")
    th1.start(); th2.start()
    th1.join(9000); tick(); th2.join(9000); tick()
    val oracle = 42L
    def bad(e: Either[Throwable, Long]) = e == null || e.isLeft || e.toOption.get != oracle
    val incomplete = Seq(r1.get, r2.get).exists(e => e != null && e.left.toOption.exists(t => String.valueOf(t.getMessage).contains("before its completion")))
    val demo = impure && (bad(r1.get) || bad(r2.get))
    val r2ex = Option(r2.get).flatMap(_.left.toOption)
    if (r2ex.isDefined) { println("T3 requester 2 exception:"); r2ex.get.printStackTrace(System.out) }
    v(if (demo) "DEMONSTRATED" else "NOT_REPRODUCED",
      s"variant=$variant $xsftLabel TweakExpansion.policy=${NodeTaskInfo.TweakExpansion.cachePolicy} tweakTranslatorsMustBePure=${Settings.tweakTranslatorsMustBePure} " +
        s"transformTweak calls=${calls.get} req1=${Option(r1.get).map(show).orNull} req2=${Option(r2.get).map(show).orNull} oracle=$oracle " +
        s"req2 returned while req1 was inside the retry=$t2DoneDuringRetry (req1 waited ${t1WaitedForT2Ms} ms) incompleteNodeRead=$incomplete activityNodesRun=${activityRuns.get}" +
        r2ex.map(t => s" req2.message=${String.valueOf(t.getMessage).take(160)}").getOrElse(""))
  }
}
