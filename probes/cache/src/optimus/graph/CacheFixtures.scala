package optimus.graph

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import optimus.entity.ClassEntityInfo
import optimus.entity.EntityInfo
import optimus.graph.ProbeDag.BadKey
import optimus.graph.ProbeDag.Kind
import optimus.graph.ProbeDag.Mode
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.UNodeCache
import optimus.platform._
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityImpl
import optimus.platform.storable.FakeModuleEntity

import scala.jdk.CollectionConverters._

/* =====================================================================================================================
 * P0 fixtures for the cache campaign (CACHE_CAMPAIGN.md "Fixtures (P0)"). See probes/cache/FIXTURES.md for the API.
 * Everything lives in package optimus.graph so private[graph]/private[optimus] members are reachable; tracker
 * internals are in optimus.graph.tracking.CacheTrackerAccess.
 * ===================================================================================================================*/

/** Plugin tag read by TAG_READER nodes. */
object ProbeTagKey extends NonForwardingPluginTagKey[java.lang.Long]

/**
 * A real heap ("applied") entity: EntityImpl with DefaultAppliedEF flavour, so equals/hashCode are argsEquals/argsHash
 * (here: the id), i.e. two ProbeEntity(3) are == and hash alike, as plugin-generated @entity classes with one ctor arg.
 * Instance tweaks need entity ne null; this is enough (no need for a class in optimus.platform.storable).
 */
final class ProbeEntity(val id: Int) extends EntityImpl {
  override def $info: EntityInfo = ProbeEntity.info
  override protected def argsEquals(other: Entity): Boolean = other.asInstanceOf[ProbeEntity].id == id
  override protected def argsHash: Int = id
}
object ProbeEntity {
  val info: ClassEntityInfo = new ClassEntityInfo(classOf[ProbeEntity], false, Nil)
}

/** The module ("object") entity for property-level tweaks and module-level nodes (model entity MODULE). */
object ProbeModule extends FakeModuleEntity(Nil)

/** One "property": its singleton NodeTaskInfo plus run accounting. */
final class ProbeProperty(val prop: ProbeDag.Prop) {
  val flags: Long = ProbeProperty.flagsOf(prop)
  /** Singleton info; TwkPropertyInfo0 is itself a PropertyTarget, so property tweaks target it directly. */
  val info: TwkPropertyInfo0[Entity, AnyRef] = new TwkPropertyInfo0[Entity, AnyRef](prop.name, flags)
  info.setEntityInfo(ProbeEntity.info)
  /** Number of times run() executed (any key of this property). */
  val runs = new AtomicInteger
  /** Per model key: run count and the last node instance that ran (for isInvalidCache / identity checks). */
  val runsByKey = new ConcurrentHashMap[ProbeDag.Key, AtomicInteger]()
  val lastRun = new ConcurrentHashMap[ProbeDag.Key, ProbePropertyNode]()
  def runsOf(k: ProbeDag.Key): Int = { val c = runsByKey.get(k); if (c eq null) 0 else c.get }
  override def toString: String = s"ProbeProperty(${prop.name}, ${NodeTaskInfo.flagsAsString(flags)})"
}
object ProbeProperty {
  final val LOCAL_CACHE = 1L << 19 // private in NodeTaskInfo
  def flagsOf(p: ProbeDag.Prop): Long = {
    var f = p.extraFlags
    if (p.tweakable) f |= NodeTaskInfo.TWEAKABLE
    if (!p.cacheable) f |= NodeTaskInfo.DONT_CACHE
    if (p.si) f |= NodeTaskInfo.SCENARIOINDEPENDENT
    if (p.atMostOneWaiter) f |= NodeTaskInfo.AT_MOST_ONE_WAITER
    if (p.localCache) f |= LOCAL_CACHE
    if (p.dontTrack) f |= NodeTaskInfo.DONT_TRACK_FOR_INVALIDATION
    f
  }
}

/**
 * Base hand-written property node. Follows the $newNode convention: a fresh instance with scenarioStack == null is a
 * key; getNode attaches it (or clones it) and the cache stores it. run() evaluates `compute`, which reads other probe
 * properties through the real path (fresh key + lookupAndGet), so tweak resolution, caching, recording and TTrack
 * tracking all fire. Exceptions complete the node exceptionally.
 *
 * Scala assigns ctor-param fields before the super constructor, so propertyInfo is valid inside PropertyNode's
 * initTrackingValue() (Java subclasses would have to defer that).
 */
abstract class ProbePropertyNode(val sys: ProbeSystem, val pp: ProbeProperty, val dkey: ProbeDag.Key)
    extends PropertyNode[java.lang.Long] {
  final val arg: Int = dkey.arg
  override val entity: Entity = sys.entity(dkey.entity)
  override def propertyInfo: NodeTaskInfo = pp.info
  override def args: Array[AnyRef] = Array(Integer.valueOf(arg))
  override def argsHash: Int = arg
  override def argsEquals(that: NodeKey[_]): Boolean = that match {
    case p: ProbePropertyNode => p.arg == arg
    case _                    => false
  }
  /** For when clauses and TweakTemplate.byKey: the generator is one of ours. */
  override def argsCopy(generator: Any): Node[Any] = generator match {
    case g: ProbeGen => g.make(this).asInstanceOf[Node[Any]]
    case _           => super.argsCopy(generator)
  }

  protected def compute(ec: OGSchedulerContext): Long

  override def run(ec: OGSchedulerContext): Unit = {
    pp.runs.incrementAndGet()
    pp.runsByKey.computeIfAbsent(dkey, _ => new AtomicInteger).incrementAndGet()
    pp.lastRun.put(dkey, this)
    sys.onRun(this)
    val v =
      try java.lang.Long.valueOf(compute(ec))
      catch { case t: Throwable => completeWithException(t, ec); null }
    if (v ne null) completeWithResult(v, ec)
  }

  /** Read one declared dependency through the real path. */
  protected final def read(r: ProbeDag.Ref): Long = sys.newKey(r.resolve(dkey)).lookupAndGet.longValue

  protected final def readAll(): Array[Long] = {
    val rs = pp.prop.reads
    val xs = new Array[Long](rs.size)
    var i = 0
    while (i < xs.length) { xs(i) = read(rs.get(i)); i += 1 }
    xs
  }

  override def toString: String = s"${dkey}${if (scenarioStack() ne null) "@" + scenarioStack()._cacheID.id else ""}"
}

/** Tweakable leaf: no reads; value = combine(key, []) (default base + arg) unless tweaked. */
class TweakableLeaf(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = pp.prop.combine.apply(dkey, Array.emptyLongArray)
}

/** Combinator: value = combine(key, children) with children read via lookupAndGet. */
class Combinator(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = pp.prop.combine.apply(dkey, readAll())
}

/** SI node: same as Combinator; the info carries SCENARIOINDEPENDENT, so getNode moves it to asSiStack. */
class SINode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends Combinator(s, p, k)

/** Reads ProbeTagKey via EvaluationContext.findPluginTag (the marking accessor path); 0 if absent. */
class TagReader(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long = {
    val t = EvaluationContext.findPluginTag(ProbeTagKey)
    pp.prop.combine.apply(dkey, Array(if (t.isDefined) t.get.longValue else 0L))
  }
}

/** Reads ScenarioStack.env directly (no marking); maps the env to a token via ProbeSystem.envToken. */
class EnvReader(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends ProbePropertyNode(s, p, k) {
  override protected def compute(ec: OGSchedulerContext): Long =
    pp.prop.combine.apply(dkey, Array(sys.envToken(scenarioStack().env)))
}

/** Deliberately bad key, per prop.badKey. Value as Combinator. */
class BadKeyNode(s: ProbeSystem, p: ProbeProperty, k: ProbeDag.Key) extends Combinator(s, p, k) {
  private def mode = pp.prop.badKey
  override def argsHash: Int = mode match {
    case BadKey.MUTABLE_ARG       => pp.prop.mutableArg(0).toInt
    case BadKey.ASYMMETRIC_EQUALS => 0
    case BadKey.UNSTABLE_HASH     => BadKeyNode.counter.incrementAndGet()
    case _                        => arg
  }
  override def argsEquals(that: NodeKey[_]): Boolean = that match {
    case b: BadKeyNode =>
      mode match {
        case BadKey.MUTABLE_ARG       => true // all keys compare equal "by the mutable cell"
        case BadKey.ASYMMETRIC_EQUALS => arg <= b.arg
        case _                        => arg == b.arg
      }
    case _ => false
  }
}
object BadKeyNode { val counter = new AtomicInteger }

/** Generator for when-clauses and by-key RHS: argsCopy(gen) == gen.make(key). Case classes: equality by content. */
sealed trait ProbeGen { def make(key: ProbePropertyNode): Node[_] }
final case class WhenGen(sys: ProbeSystem, w: ProbeDag.When) extends ProbeGen {
  def make(key: ProbePropertyNode): Node[_] = new ProbeWhenNode(sys, w, key.dkey)
}
final case class RhsGen(sys: ProbeSystem, rhs: ProbeDag.Rhs) extends ProbeGen {
  def make(key: ProbePropertyNode): Node[_] = new ProbeRhsNode(sys, rhs, key.dkey)
}

/** By-name RHS node: reads rhs.reads (resolved against the tweaked key) via lookupAndGet in its own stack. */
final class ProbeRhsNode(val sys: ProbeSystem, val rhs: ProbeDag.Rhs, val tweaked: ProbeDag.Key)
    extends CompletableNode[java.lang.Long] {
  override def run(ec: OGSchedulerContext): Unit = {
    sys.rhsRuns.incrementAndGet()
    val v =
      try {
        val xs = rhs.reads.asScala.map(r => sys.newKey(r.resolve(tweaked)).lookupAndGet.longValue).toArray
        java.lang.Long.valueOf(rhs.combine.apply(tweaked, xs))
      } catch { case t: Throwable => completeWithException(t, ec); null }
    if (v ne null) completeWithResult(v, ec)
  }
}

/** When-clause node: argPred(arg) && (read == null || valuePred(read)). */
final class ProbeWhenNode(val sys: ProbeSystem, val w: ProbeDag.When, val tweaked: ProbeDag.Key)
    extends CompletableNode[Boolean] {
  override def run(ec: OGSchedulerContext): Unit = {
    sys.whenRuns.incrementAndGet()
    try {
      val r =
        w.argPred.test(tweaked.arg) &&
          ((w.read eq null) || w.valuePred.test(sys.newKey(w.read.resolve(tweaked)).lookupAndGet.longValue))
      completeWithResult(r, ec)
    } catch { case t: Throwable => completeWithException(t, ec) }
  }
}

/**
 * Binds a ProbeDag to the runtime: one ProbeProperty (singleton info) per Prop, entities, fresh keys, tweaks,
 * scenarios and stacks built from the model.
 */
final class ProbeSystem(val dag: ProbeDag) {
  val props: Array[ProbeProperty] = dag.props.asScala.map(new ProbeProperty(_)).toArray
  val rhsRuns = new AtomicInteger
  val whenRuns = new AtomicInteger
  @volatile var runHook: ProbePropertyNode => Unit = null
  private[graph] def onRun(n: ProbePropertyNode): Unit = { val h = runHook; if (h ne null) h(n) }

  def pp(p: ProbeDag.Prop): ProbeProperty = props(p.id)
  def info(p: ProbeDag.Prop): NodeTaskInfo = pp(p).info

  private val entities = new ConcurrentHashMap[Integer, ProbeEntity]()
  /** MODULE -> ProbeModule; k >= 0 -> a canonical ProbeEntity(k) (any other ProbeEntity(k) is == to it). */
  def entity(e: Int): Entity =
    if (e == ProbeDag.MODULE) ProbeModule else entities.computeIfAbsent(e, i => new ProbeEntity(i))

  private val envTokens = new java.util.IdentityHashMap[RuntimeEnvironment, java.lang.Long]()
  def registerEnv(env: RuntimeEnvironment, token: Long): Unit = envTokens.synchronized { envTokens.put(env, token) }
  def envToken(env: RuntimeEnvironment): Long = envTokens.synchronized {
    val t = envTokens.get(env); if (t eq null) 0L else t.longValue
  }

  /** A fresh key node ($newNode convention): scenarioStack null, never reused. */
  def newKey(k: ProbeDag.Key): ProbePropertyNode = {
    val p = pp(k.prop)
    val pr = k.prop
    pr.kind match {
      case Kind.TAG_READER => new TagReader(this, p, k)
      case Kind.ENV_READER => new EnvReader(this, p, k)
      case Kind.BAD_KEY    => new BadKeyNode(this, p, k)
      case _ =>
        if (pr.si) new SINode(this, p, k)
        else if (pr.reads.isEmpty) new TweakableLeaf(this, p, k)
        else new Combinator(this, p, k)
    }
  }
  def newKey(p: ProbeDag.Prop): ProbePropertyNode = newKey(ProbeDag.key(p))
  def newKey(p: ProbeDag.Prop, entity: Int, arg: Int): ProbePropertyNode = newKey(ProbeDag.key(p, entity, arg))

  // ---- tweaks ------------------------------------------------------------------------------------------------------
  def tweak(tw: ProbeDag.Tw): Tweak = {
    val base =
      if (tw.instanceKey ne null) {
        val key = newKey(tw.instanceKey)
        if (tw.rhs eq null) SimpleValueTweak(key: NodeKey[java.lang.Long])(java.lang.Long.valueOf(tw.constant))
        else new Tweak(new InstancePropertyTarget(key), TweakTemplate(new ProbeRhsNode(this, tw.rhs, tw.instanceKey)))
      } else {
        val pinfo = pp(tw.property).info
        val target: TweakTarget[_, _] =
          if (tw.when eq null) pinfo else PredicatedPropertyTweakTarget(pinfo, WhenGen(this, tw.when))
        val template =
          if (tw.rhs eq null) TweakTemplate(java.lang.Long.valueOf(tw.constant))
          else TweakTemplate.byKey(RhsGen(this, tw.rhs))
        new Tweak(target, template)
      }
    CacheFixtures.withMode(base, tw.mode)
  }

  def scenario(level: ProbeDag.Level, unordered: Boolean = false): Scenario = {
    val tws = level.tweaks.asScala.map(tweak).toSeq
    if (unordered) Scenario.unordered(tws) else Scenario(tws)
  }

  /** root.createChild(level_1).createChild(level_2)...; a level with a tag also gets ProbeTagKey. */
  def stack(root: ScenarioStack, s: ProbeDag.Stack, unordered: Boolean = false): ScenarioStack = {
    var ss = root
    for (l <- s.levels.asScala) ss = CacheFixtures.child(ss, scenario(l, unordered), if (l.tag eq null) null else l.tag)
    ss
  }

  /** Evaluate model key k under ss through the real path (a wrapper node attached to ss calls lookupAndGet). */
  def eval(ss: ScenarioStack, k: ProbeDag.Key): Long = CacheFixtures.eval(ss, newKey(k)).longValue
  /** Like eval, but returns the node getNode handed back (cached node / tweak node), after it completed. */
  def lookupNode(ss: ScenarioStack, k: ProbeDag.Key): Node[java.lang.Long] = CacheFixtures.lookupNode(ss, newKey(k))
}

object CacheFixtures {
  // ---- context -----------------------------------------------------------------------------------------------------
  /**
   * Idempotent per thread: EvaluationContext.initializeWithoutRuntime() (RuntimeEnvironment.minimal), and forces
   * NodeCacheConfigs' initialiser, which creates UNodeCache.global/siGlobal (null until then: lookups NPE).
   */
  def init(): Unit = {
    optimus.config.NodeCacheConfigs.getCacheConfigs
    if (!EvaluationContext.isInitialised) EvaluationContext.initializeWithoutRuntime()
  }

  /** A root with its own SSCacheID (no interning through RuntimeEnvironmentCacheKey). */
  def newRoot(env: RuntimeEnvironment = RuntimeEnvironment.minimal): ScenarioStack =
    ScenarioStack.newRoot(env, uniqueID = true)

  /** A distinct RuntimeEnvironment (config and resolver null, like minimal) for env-reader probes. */
  def newEnv(): RuntimeEnvironment = new RuntimeEnvironment(null, null)

  def child(ss: ScenarioStack, sc: Scenario): ScenarioStack = ss.createChild(sc, this)
  def child(ss: ScenarioStack, sc: Scenario, tag: java.lang.Long): ScenarioStack =
    if (tag eq null) ss.createChild(sc, this)
    else
      ss.createChild(sc, FrozenNodeInputMap.empty.withExtraInputs(Seq(PluginTagKeyValue(ProbeTagKey, tag))), 0, this)
  def withTag(ss: ScenarioStack, tag: Long): ScenarioStack = ss.withPluginTag(ProbeTagKey, java.lang.Long.valueOf(tag))

  // ---- evaluation --------------------------------------------------------------------------------------------------
  /** Runs f inside a node attached to ss (EvaluationContext.scenarioStack == ss) and returns its result. */
  def evalIn[T](ss: ScenarioStack)(f: => T): T = {
    val n = new CompletableNode[T] {
      override def run(ec: OGSchedulerContext): Unit = completeWithResult(f, ec)
    }
    n.attach(ss)
    n.get
  }
  def eval[T](ss: ScenarioStack, key: PropertyNode[T]): T = evalIn(ss)(key.lookupAndGet)
  /** getNode + runAndWait + combineInfo (= lookupAndGet) but returns the node itself. */
  def lookupNode[T](ss: ScenarioStack, key: PropertyNode[T]): Node[T] = evalIn(ss) {
    val ec = OGSchedulerContext.current()
    val node = ec.scenarioStack.getNode(key, ec)
    ec.runAndWait(node)
    ec.getCurrentNodeTask.combineInfo(node, ec)
    node
  }

  // ---- low-level tweak builders (for probes that do not go through ProbeDag) ----------------------------------------
  def valueTweak[T](key: NodeKey[T], v: T): Tweak = SimpleValueTweak(key)(v)
  def instanceTweak(key: NodeKey[_], template: TweakTemplate): Tweak = new Tweak(new InstancePropertyTarget(key), template)
  def propertyTweak(info: TwkPropertyInfo0[_, _], v: Any): Tweak = new Tweak(info, TweakTemplate(v))
  def byNameInstanceTweak(key: NodeKey[_], rhs: Node[_]): Tweak = new Tweak(new InstancePropertyTarget(key), TweakTemplate(rhs))
  def bind(t: Tweak): Tweak = Tweaks.bind(t).head
  def bindOnce(t: Tweak): Tweak = Tweaks.bindOnce(t).head
  def withMode(t: Tweak, m: Mode): Tweak = m match {
    case Mode.BYNAME    => t
    case Mode.BIND      => t.bind(Tweak.evaluateInCurrent)
    case Mode.BIND_ONCE => t.bind(Tweak.evaluateInGiven)
  }
  def scenario(tweaks: Tweak*): Scenario = Scenario(tweaks)
  def unordered(tweaks: Tweak*): Scenario = Scenario.unordered(tweaks)
  def nested(outer: Scenario, inner: Scenario): Scenario = outer.nest(inner)

  // ---- state priming -----------------------------------------------------------------------------------------------
  /**
   * Install a new UNodeCache(name, maxSize) as UNodeCache.global. The UNodeCache constructor registers it
   * (Caches.register), which is what the schedulerAsserts check in global_= wants. For a child JVM the simplest route
   * is -Doptimus.scheduler.cacheSize=16 (Settings.cacheSize; also -Doptimus.cache.concurrency=N).
   */
  def installGlobalCache(maxSize: Int, name: String = "probeGlobal"): UNodeCache = {
    val c = new UNodeCache(name, maxSize)
    UNodeCache.global = c
    c
  }
  /** Give one property its own small cache (NodeTaskInfo.setCustomCache). */
  def privateCache(info: NodeTaskInfo, maxSize: Int): UNodeCache = {
    val c = new UNodeCache("probe-" + info.name + "-" + System.identityHashCode(info), maxSize)
    info.setCustomCache(c)
    c
  }
  def setTweakableID(info: NodeTaskInfo, id: Int): Unit = info.TEST_ONLY_setTweakableID(id)
  /** Sets dependsOnTweakMask from the bit ids and switches the policy to XSFT. */
  def setAssumeDependsOn(info: NodeTaskInfo, ids: Int*): Unit = info.TEST_ONLY_setAssumeDependsOn(ids.toArray)
  def setPolicy(info: NodeTaskInfo, p: NCPolicy): Unit = info.setCachePolicy(p)
  /** Property optimus.tracking.cleanup.ttrackClearedNodeRefWatermark (Settings.ttrackClearedNodeRefWatermark). */
  val watermarkProperty = "optimus.tracking.cleanup.ttrackClearedNodeRefWatermark"
  def setTTrackWatermark(n: Int): Unit = Settings.ttrackClearedNodeRefWatermark = n
  def xsftEnabledByDefault: Boolean = ProcessGraphInputs.EnableXSFT.currentValueOrThrow()

  // ---- child JVM ---------------------------------------------------------------------------------------------------
  /**
   * Run `mainClass args` in a child JVM built from CACHE_JAVA_CMD (exported by probes/cache/run.sh), with extra JVM
   * flags inserted after "java". Returns (exit code, combined output). Times out after timeoutMs (exit -1).
   */
  def runChild(jvmFlags: Seq[String], mainClass: String, args: Seq[String], timeoutMs: Long = 120000): (Int, String) = {
    val cmd = System.getenv("CACHE_JAVA_CMD")
    if (cmd == null) throw new IllegalStateException("CACHE_JAVA_CMD not set; run via probes/cache/run.sh")
    val full = cmd.replaceFirst("^java ", "java " + jvmFlags.mkString(" ") + " ") + " " + mainClass + " " + args.mkString(" ")
    val p = new ProcessBuilder("sh", "-c", full).redirectErrorStream(true).start()
    val out = new java.io.ByteArrayOutputStream
    val t = new Thread(() => p.getInputStream.transferTo(out))
    t.setDaemon(true); t.start()
    val done = p.waitFor(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    if (!done) p.destroyForcibly()
    t.join(2000)
    (if (done) p.exitValue else -1, out.toString)
  }

  val idGen = new AtomicLong
}
