package optimus.graph {

import java.util.BitSet
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import optimus.graph.ProbeDag._
import optimus.graph.cache.NCPolicy
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerBatchUpdater
import optimus.graph.tracking.{CacheTrackerAccess => T}
import optimus.graph.tracking.ttracks.CacheHunterTT
import optimus.platform._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/* =====================================================================================================================
 * H2 (tweak-and-reuse fuzzer) and H3 (invalidation fuzzer), CACHE_CAMPAIGN.md Tier 2. Structure follows the
 * scheduler hunter (probes/src/optimus/graph/Hunter.java): a seed generates a spec (pure); a child JVM materialises it
 * against the real runtime and checks oracles; the parent sweeps seeds x configurations, reruns failures, minimises
 * and classifies them with feature toggles.
 *
 *   CacheHunter sweep h2|h3 <firstSeed> <count>
 *   CacheHunter one h2|h3 <seed> <cfg> [reps] [knob=value ...]     knobs: off=a,b maxProps=N drop=i,j clients=N
 *   CacheHunter spec h2|h3 <seed> [knob=value ...]                  print the (knob-reduced) spec
 *   CacheHunter selftest [n]                                        memoised oracle vs ProbeDag.SequentialEvaluator
 *   CacheHunter --child h2|h3 <seed> <cfg>                          (internal; knobs arrive as -Dhunter.*)
 *
 * cfg: h2 i{ideal}-a{asserts 0|1}-d{delay 0|1}; h3 adds -w{watermark1 0|1}.
 * off= toggles (classification): tags modes byname when si xs xsft alias pin nest unordered twice cancel dup
 * ===================================================================================================================*/

// ------------------------------------------------------------------------------------------------ spec (pure)
final class GRef(val prop: Int, val ent: Int, val arg: Int) {
  override def toString: String =
    s"p$prop[${if (ent == SAME) "=" else if (ent == MODULE) "M" else "e" + ent}${if (arg == SAME) ",=" else "," + arg}]"
}
final class GWhen(val mod: Int, val rem: Int, val read: GRef, val bit: Int) {
  override def toString: String = s"when(arg%$mod==$rem${if (read != null) s" && bit$bit($read)" else ""})"
}
final class GRhs(val reads: Vector[GRef], val mul: Long, val add: Long) {
  override def toString: String = s"${reads.mkString("+")}*$mul+$add"
}
final class GTw(val inst: Boolean, val prop: Int, val ent: Int, val arg: Int, val const: Long, val rhs: GRhs,
    val mode: Mode) {
  def target(A: Int, E: Int): Long = if (inst) GSpec.keyIdx(prop, ent, arg, E, A).toLong else -1L - prop
  override def toString: String = {
    val t = if (inst) s"p$prop[${if (ent == MODULE) "M" else "e" + ent},$arg]" else s"p$prop"
    s"$t:=${if (rhs == null) const.toString else s"$mode($rhs)"}"
  }
}
final class GLevel(val tws: Vector[GTw], val tag: java.lang.Long) {
  override def toString: String = s"{${tws.mkString(", ")}${if (tag != null) s" tag=$tag" else ""}}"
}
final class GNode(val id: Int, val parent: Int, val depth: Int, val level: GLevel, val unordered: Boolean,
    val nested: Boolean, val twice: Boolean, val origin: String) {
  var modeTwin = false
}
final class GProp(val id: Int) {
  var tweakable = false
  var kind: Kind = Kind.PLAIN
  var si = false
  var isdProne = false
  var base = 0L
  val reads = mutable.ArrayBuffer[GRef]()
  var pol = 0 // GSpec.POL_*
  var pin = 0 // 0: not pinned
  var aliasOf = -1
  var when: GWhen = null // property tweaks on this prop are predicated with this clause
  def kindStr: String =
    if (si) (if (isdProne) "SI!" else "SI") else if (kind == Kind.TAG_READER) "TAG" else if (kind == Kind.ENV_READER) "ENV"
    else if (tweakable) (if (reads.isEmpty) "leaf" else "tcomb") else if (reads.isEmpty) "const" else "comb"
}
final class GTracker(val id: Int, val parent: Int, val concurrent: Boolean, val late: Boolean, val depth: Int)
final class GSpec(val seed: Long, val props: Vector[GProp], val E: Int, val A: Int, val nodes: Vector[GNode],
    val trackers: Vector[GTracker], val clients: Int, val tagsOn: Boolean) {
  def nKeys: Int = props.size * (E + 1) * A
}

object GSpec {
  final val POL_DEFAULT = 0
  final val POL_BASIC = 1
  final val POL_XSFT_REAL = 2
  final val POL_XSFT_SMALL = 3
  final val POL_XS = 4
  final val POL_DONTCACHE = 5
  val polNames: Array[String] = Array("default", "basic", "xsftReal", "xsftSmall", "xs", "dontCache")

  def keyIdx(p: Int, e: Int, a: Int, E: Int, A: Int): Int = (p * (E + 1) + (e + 1)) * A + a

  private def pick[A](r: Random, xs: IndexedSeq[A]): A = xs(r.nextInt(xs.size))

  def ref(r: Random, prop: Int, E: Int, A: Int, pSame: Double): GRef =
    new GRef(prop, if (r.nextDouble() < pSame) SAME else r.nextInt(E + 1) - 1, if (r.nextDouble() < pSame) SAME else r.nextInt(A))

  def generate(seed: Long, h3: Boolean): GSpec = {
    val r = new Random(seed * 0x9E3779B97F4A7C15L + 0x5DEECE66DL)
    val n = math.round(10 * math.pow(50, r.nextDouble())).toInt // 10..500, log-uniform
    val E = 1 + r.nextInt(4)
    val A = 1 + r.nextInt(3)
    val nLeaves = math.max(2, (n * (0.2 + 0.2 * r.nextDouble())).toInt)
    val tagsOn = r.nextDouble() < 0.4
    val props = new mutable.ArrayBuffer[GProp]
    val tweakFree = new mutable.ArrayBuffer[Boolean]
    val tagEnv = new mutable.ArrayBuffer[Boolean]
    for (i <- 0 until n) {
      val p = new GProp(i)
      p.base = r.nextInt(1000)
      if (i < nLeaves) {
        p.tweakable = i < 2 || r.nextDouble() < 0.9
        val x = r.nextDouble()
        p.pol = if (x < 0.6) POL_DEFAULT else if (x < 0.9) POL_BASIC else POL_DONTCACHE
      } else {
        val x = r.nextDouble()
        val siOk = (0 until i).filter(j => !tagEnv(j))
        if (x < 0.04 && siOk.nonEmpty) {
          p.si = true
          p.isdProne = r.nextDouble() < 0.15
          val cands = if (p.isdProne) siOk else siOk.filter(tweakFree)
          if (cands.isEmpty) p.isdProne = true
          val cs = if (cands.isEmpty) siOk else cands
          for (_ <- 0 until 1 + r.nextInt(2)) p.reads += ref(r, pick(r, cs), E, A, 0.85)
        } else if (x < 0.07) p.kind = Kind.TAG_READER
        else if (x < 0.09) p.kind = Kind.ENV_READER
        else {
          p.tweakable = r.nextDouble() < 0.15
          for (_ <- 0 until 1 + r.nextInt(4)) {
            val j =
              if (r.nextDouble() < 0.2) r.nextInt(math.min(6, i))
              else math.max(0, i - 1 - (r.nextDouble() * r.nextDouble() * math.min(i, 40)).toInt)
            p.reads += ref(r, j, E, A, 0.85)
          }
          val y = r.nextDouble()
          p.pol = if (p.tweakable) { if (y < 0.6) POL_DEFAULT else if (y < 0.9) POL_BASIC else POL_DONTCACHE }
          else if (y < 0.35) POL_DEFAULT else if (y < 0.55) POL_BASIC else if (y < 0.7) POL_XSFT_REAL
          else if (y < 0.8) POL_XSFT_SMALL else if (y < 0.9) POL_XS else POL_DONTCACHE
        }
      }
      tweakFree += (!p.tweakable && p.kind == Kind.PLAIN && p.reads.forall(rf => tweakFree(rf.prop)))
      tagEnv += (p.kind != Kind.PLAIN || p.reads.exists(rf => tagEnv(rf.prop)))
      props += p
    }
    // pinned tweakable ids, some aliased; when-flavours
    var nextId = 40
    val pinned = new mutable.ArrayBuffer[Int]
    for (p <- props if p.tweakable) {
      if (r.nextDouble() < 0.7) {
        p.pin = nextId; nextId += 1
        if (pinned.nonEmpty && r.nextDouble() < 0.15) p.aliasOf = pick(r, pinned.toIndexedSeq)
        pinned += p.id
      }
      if (r.nextDouble() < 0.25) {
        val mod = 1 + r.nextInt(3)
        val rd = if (p.id > 0 && r.nextDouble() < 0.5) ref(r, r.nextInt(p.id), E, A, 0.7) else null
        p.when = new GWhen(mod, r.nextInt(mod), rd, r.nextInt(2))
      }
    }
    val tweakables = props.filter(_.tweakable).map(_.id).toIndexedSeq
    // scenario tree (H2)
    val nodes = new mutable.ArrayBuffer[GNode]
    nodes += new GNode(0, -1, 0, new GLevel(Vector.empty, null), false, false, false, "root")
    if (!h3) {
      val maxDepth = 1 + r.nextInt(4)
      var frontier = Vector(0)
      while (frontier.nonEmpty) {
        val next = new mutable.ArrayBuffer[Int]
        for (pid <- frontier) {
          val par = nodes(pid)
          if (par.depth < maxDepth) {
            val kids = if (par.depth == 0) 1 + r.nextInt(3) else r.nextInt(3)
            val sibs = new mutable.ArrayBuffer[GNode]
            for (_ <- 0 until kids) {
              val x = r.nextDouble()
              val twinSrc = sibs.find(_.level.tws.exists(_.rhs != null))
              val (level, origin) =
                if (x < 0.15 && sibs.nonEmpty) { val s = pick(r, sibs.toIndexedSeq); (s.level, s"dup${s.id}") }
                else if (x < 0.2 && twinSrc.isDefined) {
                  val s = twinSrc.get
                  val i = s.level.tws.indexWhere(_.rhs != null)
                  val t = s.level.tws(i)
                  val m2 = Mode.values()((t.mode.ordinal + 1 + r.nextInt(2)) % 3)
                  val sameReadsSelf = t.rhs.reads.exists(_.prop == t.prop)
                  val t2 = new GTw(t.inst, t.prop, t.ent, t.arg, t.const, t.rhs, if (sameReadsSelf) Mode.BYNAME else m2)
                  (new GLevel(s.level.tws.updated(i, t2), s.level.tag), s"modeTwin${s.id}")
                } else { val l = genLevel(r, props, tweakables, E, A); (if (tagsOn || l.tag == null) l else new GLevel(l.tws, null), "gen") }
              val nested = par.depth >= 1 && !par.nested && r.nextDouble() < 0.15 && level.tag == null && par.level.tag == null
              val g = new GNode(nodes.size, pid, par.depth + 1, level, r.nextDouble() < 0.2, nested, r.nextDouble() < 0.15, origin)
              if (origin.startsWith("modeTwin")) { g.modeTwin = true; nodes(origin.substring(8).toInt).modeTwin = true }
              nodes += g; sibs += g; next += g.id
            }
          }
        }
        frontier = next.toVector
      }
    }
    // tracker plan (H3)
    val trackers = new mutable.ArrayBuffer[GTracker]
    trackers += new GTracker(0, -1, false, false, 0)
    if (h3) {
      val nCons = 1 + r.nextInt(3)
      val nConc = r.nextInt(3)
      val kinds = scala.util.Random.javaRandomToRandom(r).shuffle(Seq.fill(nCons)(false) ++ Seq.fill(nConc)(true))
      for (conc <- kinds) {
        val par = pick(r, trackers.filter(_.depth < 3).toIndexedSeq)
        trackers += new GTracker(trackers.size, par.id, conc, r.nextDouble() < 0.25, par.depth + 1)
      }
    }
    val clients = if (h3) 2 + r.nextInt(3) else 1 + r.nextInt(6)
    new GSpec(seed, props.toVector, E, A, nodes.toVector, trackers.toVector, clients, tagsOn)
  }

  def genTw(r: Random, props: collection.IndexedSeq[GProp], tweakables: IndexedSeq[Int], E: Int, A: Int,
      used: mutable.Set[Long]): GTw = {
    var attempt = 0
    while (attempt < 6) {
      attempt += 1
      val p = tweakables(r.nextInt(tweakables.size))
      val inst = r.nextDouble() < 0.45
      val ent = r.nextInt(E + 1) - 1
      val arg = r.nextInt(A)
      val const = 1L + r.nextInt(1000000)
      val byName = r.nextDouble() < 0.4
      val mx = r.nextDouble()
      var mode = if (mx < 0.5) Mode.BYNAME else if (mx < 0.75) Mode.BIND else Mode.BIND_ONCE
      val nr = 1 + r.nextInt(2)
      val refs = (0 until nr).map { _ =>
        val q = if (p == 0 || r.nextDouble() < 0.2) p else r.nextInt(p)
        ref(r, q, E, A, 0.7)
      }.toVector
      if (refs.exists(_.prop == p)) mode = Mode.BYNAME
      val rhs = if (byName) new GRhs(refs, 1 + r.nextInt(5), r.nextInt(1000)) else null
      val t = new GTw(inst, p, ent, arg, const, rhs, mode)
      val tgt = t.target(A, E)
      if (used.add(tgt)) return t
    }
    null
  }

  def genLevel(r: Random, props: collection.IndexedSeq[GProp], tweakables: IndexedSeq[Int], E: Int, A: Int): GLevel = {
    val n = r.nextInt(7)
    val used = mutable.Set[Long]()
    val tws = (0 until n).flatMap(_ => Option(genTw(r, props, tweakables, E, A, used))).toVector
    new GLevel(tws, if (r.nextDouble() < 0.15) java.lang.Long.valueOf(1 + r.nextInt(5)) else null)
  }
}

// ------------------------------------------------------------------------------------------------ knobs
final class Knobs(val off: Set[String], val maxProps: Int, val drop: Set[Int], val clients: Int, val budgetMs: Long,
    val wdMs: Long, val iters: Int, val phases: Int) {
  def has(t: String): Boolean = off.contains(t)
  def describe: String =
    Seq(if (off.nonEmpty) s"off=${off.toSeq.sorted.mkString(",")}" else "", if (maxProps > 0) s"maxProps=$maxProps" else "",
      if (drop.nonEmpty) s"drop=${drop.toSeq.sorted.mkString(",")}" else "", if (clients > 0) s"clients=$clients" else "")
      .filter(_.nonEmpty).mkString(" ")
}
object Knobs {
  private def ints(s: String): Set[Int] = if (s == null || s.isEmpty) Set.empty else s.split(",").map(_.trim.toInt).toSet
  def fromProps(): Knobs = new Knobs(
    Option(System.getProperty("hunter.off")).map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet).getOrElse(Set.empty),
    Integer.getInteger("hunter.maxProps", 0), ints(System.getProperty("hunter.drop")), Integer.getInteger("hunter.clients", 0),
    java.lang.Long.getLong("hunter.budgetMs", 3000L), java.lang.Long.getLong("hunter.wdMs", 15000L),
    Integer.getInteger("hunter.iters", 200), Integer.getInteger("hunter.phases", 40))
  def fromMap(kv: Seq[(String, String)]): Knobs = {
    val m = kv.toMap
    new Knobs(m.get("off").map(_.split(",").toSet).getOrElse(Set.empty), m.get("maxProps").map(_.toInt).getOrElse(0),
      ints(m.getOrElse("drop", "")), m.get("clients").map(_.toInt).getOrElse(0), 3000, 15000, 200, 40)
  }
}

// ------------------------------------------------------------------------------------------------ model (spec + knobs)
/** The spec reduced by the knobs, as a ProbeDag plus memoised Tw/Level objects (identity matters for cache ids). */
final class Mat(val spec: GSpec, val k: Knobs) {
  import GSpec._
  val E: Int = spec.E
  val A: Int = spec.A
  val n: Int = if (k.maxProps > 0) math.min(k.maxProps, spec.props.size) else spec.props.size
  val gp: Vector[GProp] = spec.props.take(n)
  val dag = new ProbeDag
  val MIX: Combine = (key: Key, xs: Array[Long]) => {
    var h = key.prop.base * 1000003L + key.arg * 7919L + (key.entity + 3) * 104729L
    var i = 0
    while (i < xs.length) { h = h * 31 + xs(i); i += 1 }
    h
  }
  val props: Array[Prop] = {
    val out = new Array[Prop](n)
    for (g <- gp) {
      val p = dag.prop("p" + g.id)
      p.base(g.base)
      val tagOff = k.has("tags") && g.kind == Kind.TAG_READER
      if (!g.tweakable) p.notTweakable()
      if (g.si && !k.has("si")) p.si()
      if (!tagOff) p.kind(g.kind)
      if (!tagOff) for (rf <- g.reads) p.reads(out(rf.prop), rf.ent, rf.arg)
      if (g.pol == POL_DONTCACHE) p.dontCache()
      p.combine(MIX)
      out(g.id) = p
    }
    out
  }
  def policy(g: GProp): Int = g.pol match {
    case POL_XS if k.has("xs")                                                       => POL_BASIC
    case POL_DEFAULT | POL_XSFT_REAL | POL_XSFT_SMALL if k.has("xsft") && g.pol != POL_DONTCACHE => POL_BASIC
    case p                                                                           => p
  }
  def keyIdx(key: Key): Int = GSpec.keyIdx(key.prop.id, key.entity, key.arg, E, A)
  def keyIdx(p: Int, e: Int, a: Int): Int = GSpec.keyIdx(p, e, a, E, A)
  def keyOf(idx: Int): Key = {
    val a = idx % A
    val pe = idx / A
    new Key(props(pe / (E + 1)), pe % (E + 1) - 1, a)
  }
  def nKeys: Int = n * (E + 1) * A
  def gRef(r: GRef): Ref = new Ref(props(r.prop), r.ent, r.arg)

  private val whens = new java.util.IdentityHashMap[GWhen, When]()
  def whenOf(g: GProp): When =
    if (g.when == null || k.has("when") || (g.when.read != null && g.when.read.prop >= n)) null
    else whens.synchronized {
      var w = whens.get(g.when)
      if (w == null) {
        val gw = g.when
        val rd = if (gw.read == null) null else gRef(gw.read)
        w = new When((a: Int) => a % gw.mod == gw.rem, rd, (v: Long) => ((v >>> 3) & 1L) == gw.bit)
        whens.put(gw, w)
      }
      w
    }
  private val rhss = new java.util.IdentityHashMap[GRhs, Rhs]()
  def rhsOf(g: GRhs): Rhs = rhss.synchronized {
    var x = rhss.get(g)
    if (x == null) {
      val mul = g.mul; val add = g.add
      x = new Rhs(g.reads.map(gRef).asJava, (_: Key, xs: Array[Long]) => xs.sum * mul + add)
      rhss.put(g, x)
    }
    x
  }
  private val tws = new java.util.IdentityHashMap[GTw, Tw]()
  /** null when the target is dropped. */
  def tw(g: GTw): Tw =
    if (g.prop >= n) null
    else tws.synchronized {
      var t = tws.get(g)
      if (t == null) {
        val p = props(g.prop)
        val byName = g.rhs != null && !k.has("byname")
        val const = if (g.rhs != null && !byName) g.rhs.add + g.const else g.const
        val mode = if (k.has("modes")) Mode.BYNAME else g.mode
        t =
          if (g.inst) {
            val key = new Key(p, g.ent, g.arg)
            if (byName) Tw.instance(key, rhsOf(g.rhs), mode) else Tw.instance(key, const)
          } else {
            val w = whenOf(gp(g.prop))
            if (w == null) { if (byName) Tw.property(p, rhsOf(g.rhs), mode) else Tw.property(p, const) }
            else if (byName) Tw.propertyWhen(p, w, rhsOf(g.rhs), mode)
            else Tw.propertyWhen(p, w, const)
          }
        tws.put(g, t)
      }
      t
    }
  private val levels = new java.util.IdentityHashMap[AnyRef, Level]()
  def level(gl: GLevel): Level = levelOf(gl, gl.tws, gl.tag)
  def levelOf(id: AnyRef, gtws: Seq[GTw], tag: java.lang.Long): Level = levels.synchronized {
    var l = levels.get(id)
    if (l == null) {
      l = new Level(gtws.flatMap(g => Option(tw(g))).asJava, if (k.has("tags")) null else tag)
      levels.put(id, l)
    }
    l
  }

  // ---- H2 tree after drops
  val keptNode: Array[Boolean] = {
    val a = new Array[Boolean](spec.nodes.size)
    for (g <- spec.nodes) a(g.id) = g.id == 0 || (a(g.parent) && !k.drop.contains(g.id))
    a
  }
  def path(id: Int): List[GNode] = {
    var l = List.empty[GNode]
    var i = id
    while (i > 0) { l = spec.nodes(i) :: l; i = spec.nodes(i).parent }
    l
  }
  def nodeLevels(id: Int): Array[Level] = path(id).map(g => level(g.level)).toArray

  /** Static closure of props reachable from p: declared reads, plus RHS/when refs of the given tweaks on reached props. */
  def closure(p: Int, tweaks: Iterable[GTw]): BitSet = {
    val byProp = tweaks.filter(_.prop < n).groupBy(_.prop)
    val seen = new BitSet
    val st = new java.util.ArrayDeque[Integer]
    st.push(p); seen.set(p)
    def add(q: Int): Unit = if (q < n && !seen.get(q)) { seen.set(q); st.push(q) }
    while (!st.isEmpty) {
      val q = st.pop().intValue
      val g = gp(q)
      if (props(q).reads.size > 0) g.reads.foreach(r => add(r.prop))
      if (g.when != null && g.when.read != null) add(g.when.read.prop)
      byProp.getOrElse(q, Nil).foreach(t => if (t.rhs != null) t.rhs.reads.foreach(r => add(r.prop)))
    }
    seen
  }
  def kindsIn(bs: BitSet): Set[String] = {
    val s = mutable.Set[String]()
    var i = bs.nextSetBit(0)
    while (i >= 0) {
      val g = gp(i)
      if (props(i).kind == Kind.TAG_READER) s += "TAG"
      if (props(i).si) s += (if (g.isdProne) "SI!" else "SI")
      policy(g) match {
        case POL_XS          => s += "XS"
        case POL_XSFT_SMALL  => s += "XSFTsmall"
        case POL_XSFT_REAL   => s += "XSFT"
        case POL_DEFAULT     => s += "XSFT"
        case _               =>
      }
      if (g.tweakable && g.aliasOf >= 0 && !k.has("alias") && !k.has("pin")) s += "ALIAS"
      i = bs.nextSetBit(i + 1)
    }
    s.toSet
  }

  def describe(h3: Boolean): String = {
    val sb = new StringBuilder
    sb.append(s"props=$n E=$E A=$A clients=${spec.clients} tagsOn=${spec.tagsOn} ${k.describe}\n")
    for (g <- gp)
      sb.append(f"  p${g.id}%-4d ${g.kindStr}%-6s base=${g.base}%-4d pol=${GSpec.polNames(policy(g))}%-9s" +
        s"${if (g.pin > 0) s" pin=${g.pin}" else ""}${if (g.aliasOf >= 0) s" aliasOf=p${g.aliasOf}" else ""}" +
        s"${if (g.when != null) " " + g.when else ""} reads=${g.reads.mkString(" ")}\n")
    if (!h3)
      for (g <- spec.nodes if g.id > 0 && keptNode(g.id))
        sb.append(s"  node ${g.id} parent=${g.parent} depth=${g.depth}${if (g.unordered) " unordered" else ""}" +
          s"${if (g.nested) " nested" else ""}${if (g.twice) " twice" else ""}${if (g.modeTwin) " MODE-TWIN" else ""} ${g.origin} ${g.level}\n")
    else
      for (t <- spec.trackers)
        sb.append(s"  tracker ${t.id} parent=${t.parent}${if (t.concurrent) " concurrent" else " consistent"}${if (t.late) " late" else ""}" +
          s"${if (k.drop.contains(t.id)) " DROPPED" else ""}\n")
    sb.toString
  }
}

// ------------------------------------------------------------------------------------------------ memoised oracle
/**
 * Same semantics as ProbeDag.SequentialEvaluator (checked by `selftest`), memoised per (key, position), so it is
 * linear in the DAG. Optionally records the transitive tweakable reads per entry as key-index bitsets.
 */
final class HOracle(m: Mat, val levels: Array[Level], envToken: Long, withReads: Boolean, ttMode: Boolean = false) {
  // ttMode: reads made by a BYNAME RHS (evaluated in the parent of the given) and by when clauses are not recorded;
  // the tracker does not keep TTrack roots for those (ttprobe), it invalidates them through the tweak instead.
  final class R(val v: Long, val isd: Boolean, val reads: BitSet)
  private object Isd extends scala.util.control.ControlThrowable
  private val memo = new java.util.HashMap[java.lang.Long, R]()
  private var steps = 0L
  def eval(k: Key): R = synchronized(ev(k, levels.length, false))
  def evalAt(k: Key, top: Int): R = synchronized(ev(k, top, false))
  def size: Int = memo.size

  private def ev(k: Key, top: Int, si: Boolean): R = {
    val mk = java.lang.Long.valueOf((m.keyIdx(k).toLong << 8) | (top << 1) | (if (si) 1 else 0))
    val c = memo.get(mk)
    if (c != null) return c
    steps += 1
    if (steps > 20000000) throw new IllegalStateException("oracle runaway at " + k)
    val acc = if (withReads) new BitSet else null
    val r = try new R(uncached(k, top, si, acc), false, acc) catch { case Isd => new R(0, true, acc) }
    memo.put(mk, r)
    r
  }
  private def sub(k: Key, top: Int, si: Boolean, acc: BitSet): Long = {
    val r = ev(k, top, si)
    if (acc != null) acc.or(r.reads)
    if (r.isd) throw Isd
    r.v
  }
  private def uncached(k: Key, top: Int, si: Boolean, acc: BitSet): Long = {
    val p = k.prop
    if (p.tweakable) {
      if (acc != null) acc.set(m.keyIdx(k))
      if (si) throw Isd
      var lvl = top
      while (lvl >= 1) {
        val tw = findTweak(levels(lvl - 1), k, lvl, acc)
        if (tw != null) return evalTweak(tw, k, top, si, lvl, acc)
        lvl -= 1
      }
    }
    compute(k, top, si || p.si, acc)
  }
  private def findTweak(level: Level, k: Key, lvl: Int, acc: BitSet): Tw = {
    var inst: Tw = null
    var prop: Tw = null
    val it = level.tweaks.iterator
    while (it.hasNext) {
      val t = it.next()
      if (t.instanceKey != null && t.instanceKey == k) inst = t
      else if (t.property eq k.prop) prop = t
    }
    if (inst != null) return inst
    if (prop != null && prop.when != null) {
      val w = prop.when
      if (!w.argPred.test(k.arg)) return null
      if (w.read != null) {
        val rv = sub(w.read.resolve(k), lvl - 1, false, if (ttMode) null else acc)
        if (!w.valuePred.test(rv)) return null
      }
    }
    prop
  }
  private def evalTweak(tw: Tw, k: Key, top: Int, si: Boolean, lvl: Int, acc: BitSet): Long = {
    if (tw.rhs == null) return tw.constant
    val (t, s) = tw.mode match {
      case Mode.BYNAME    => (lvl - 1, false)
      case Mode.BIND_ONCE => (lvl, false)
      case Mode.BIND      => (top, si)
    }
    val rs = tw.rhs.reads
    val xs = new Array[Long](rs.size)
    val acc2 = if (ttMode && tw.mode == Mode.BYNAME) null else acc
    var i = 0
    while (i < xs.length) { xs(i) = sub(rs.get(i).resolve(k), t, s, acc2); i += 1 }
    tw.rhs.combine.apply(k, xs)
  }
  private def compute(k: Key, top: Int, si: Boolean, acc: BitSet): Long = {
    val p = k.prop
    p.kind match {
      case Kind.TAG_READER => p.combine.apply(k, Array(tagAt(top)))
      case Kind.ENV_READER => p.combine.apply(k, Array(envToken))
      case _ =>
        val rs = p.reads
        val xs = new Array[Long](rs.size)
        var i = 0
        while (i < xs.length) { xs(i) = sub(rs.get(i).resolve(k), top, si, acc); i += 1 }
        p.combine.apply(k, xs)
    }
  }
  private def tagAt(top: Int): Long = {
    var lvl = top
    while (lvl >= 1) { val t = levels(lvl - 1).tag; if (t != null) return t; lvl -= 1 }
    0
  }
}

// ------------------------------------------------------------------------------------------------ child runtime
final class HRt(val m: Mat, seed: Long) {
  import GSpec._
  val sys = new ProbeSystem(m.dag)
  val polFail = new AtomicInteger
  def info(p: Int): NodeTaskInfo = sys.info(m.props(p))
  def tweakIdOf(p: Int): Int = { val i = info(p); i.ensureTweakMaskIsSet(); i.tweakableID }
  def configure(tweaks: Iterable[GTw]): String = {
    val sb = new StringBuilder
    if (!m.k.has("pin"))
      for (g <- m.gp if g.tweakable && g.pin > 0) {
        val id =
          if (g.aliasOf >= 0 && g.aliasOf < m.n && !m.k.has("alias") && m.gp(g.aliasOf).pin > 0) m.gp(g.aliasOf).pin
          else g.pin
        CacheFixtures.setTweakableID(info(g.id), id)
      }
    for (g <- m.gp) {
      val inf = info(g.id)
      try m.policy(g) match {
          case POL_BASIC => CacheFixtures.setPolicy(inf, NCPolicy.Basic)
          case POL_XS    => inf.setFavorReuse(true)
          case pol @ (POL_XSFT_REAL | POL_XSFT_SMALL) =>
            val cl = m.closure(g.id, tweaks)
            val ids = mutable.ArrayBuffer[Int]()
            var i = cl.nextSetBit(0)
            while (i >= 0) { if (m.gp(i).tweakable) ids += tweakIdOf(i); i = cl.nextSetBit(i + 1) }
            val rr = new Random(seed * 131 + g.id)
            val use = if (pol == POL_XSFT_SMALL) ids.filter(_ => rr.nextDouble() < 0.4) else ids
            CacheFixtures.setAssumeDependsOn(inf, use.distinct.toSeq: _*)
            if (pol == POL_XSFT_SMALL && sb.length < 400) sb.append(s" p${g.id}:${use.size}/${ids.distinct.size}")
          case _ =>
        } catch { case t: Throwable => polFail.incrementAndGet(); println(s"POLICY-FAIL p${g.id} ${GSpec.polNames(g.pol)} $t") }
    }
    sb.toString
  }
}

final class NotDoneMarker(msg: String) extends RuntimeException(msg, null, false, false)
/** invalidateCache clears STATE_DONE_FLAG, so a node handed out by evaluateNodeKeysAsync and read after a later update throws. */
object InvalidatedBeforeRead extends RuntimeException("invalidated before read", null, false, false)

object HUtil {
  def chain(t: Throwable): List[Throwable] = {
    val b = mutable.ListBuffer[Throwable]()
    var x = t
    while (x != null && b.size < 20) { b += x; x = x.getCause }
    b.toList
  }
  def isISD(t: Throwable): Boolean = chain(t).exists(_.isInstanceOf[IllegalScenarioDependenceException])
  def isCancel(t: Throwable): Boolean =
    chain(t).exists(x => x.isInstanceOf[OptimusCancellationException] || x.getClass.getName.contains("Cancel"))
  def brief(t: Throwable): String = {
    val c = chain(t).last
    s"${t.getClass.getSimpleName}${if (c ne t) "<-" + c.getClass.getSimpleName else ""}: ${String.valueOf(c.getMessage).take(140)}"
  }
  def thread(name: String)(body: => Unit): Thread = {
    val t = new Thread(null, () => body, name, 512L << 20)
    t.setDaemon(true)
    t
  }
  val families = new ConcurrentHashMap[String, AtomicInteger]()
  val findings = new AtomicInteger
  val printed = new ConcurrentHashMap[String, AtomicInteger]()
  /** FINDING lines: counted per kind, printed up to `limit` per kind. */
  def finding(kind: String, family: String, detail: => String, limit: Int = 12): Unit = {
    findings.incrementAndGet()
    families.computeIfAbsent(s"$kind:$family", _ => new AtomicInteger).incrementAndGet()
    if (printed.computeIfAbsent(kind + family, _ => new AtomicInteger).incrementAndGet() <= limit) {
      println(s"FINDING $kind $family $detail"); System.out.flush()
    }
  }
  val cov = new ConcurrentHashMap[String, AtomicLong]()
  def inc(k: String, d: Long = 1): Unit = cov.computeIfAbsent(k, _ => new AtomicLong).addAndGet(d)
  def covLine: String = cov.asScala.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${v.get}" }.mkString(" ")
  def familyLines(): Unit =
    families.asScala.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"FAMILY $k ${v.get}") }

  // ---- H5-style scheduler accounting (ported from Hunter.java)
  private def fld(c: Class[_], n: String) = { val f = c.getDeclaredField(n); f.setAccessible(true); f }
  private lazy val F_WORKERS = fld(classOf[OGScheduler], "_workers")
  private lazy val F_WAITERS = fld(classOf[OGScheduler], "_waiters")
  private lazy val F_SPIN = fld(classOf[OGScheduler], "_spinningThreadID")
  private def cnt(o: AnyRef): Int = o.asInstanceOf[optimus.core.IndexedArrayList[_]].count
  def account(s: OGScheduler): List[String] = {
    val v = mutable.ListBuffer[String]()
    val w = cnt(F_WORKERS.get(s)); if (w != 0) v += s"workers=$w"
    val wt = cnt(F_WAITERS.get(s)); if (wt != 0) v += s"waiters=$wt"
    val sp = F_SPIN.getInt(s); if (sp != 0 && w == 0) v += s"spinningThreadID=$sp"
    for ((t, st) <- Thread.getAllStackTraces.asScala if t.isAlive && t.getName.startsWith("og.worker")) {
      val s0 = t.getState
      if (s0 != Thread.State.WAITING && s0 != Thread.State.TIMED_WAITING && s0 != Thread.State.TERMINATED)
        v += s"nonIdle:${t.getName}:$s0@${st.headOption.getOrElse("")}"
    }
    v.toList
  }
  def settle(maxMs: Long): List[String] = {
    val s = EvaluationContext.scheduler.asInstanceOf[OGScheduler]
    val t0 = System.nanoTime()
    var v = account(s)
    while (v.nonEmpty && (System.nanoTime() - t0) / 1000000 < maxMs) { Thread.sleep(5); v = account(s) }
    v
  }

  // ---- watchdog + hang dump
  @volatile var lastTick: Long = System.nanoTime()
  @volatile var tickWhat: String = "start"
  @volatile var extraDump: () => String = () => ""
  def tick(what: String): Unit = { lastTick = System.nanoTime(); tickWhat = what }
  def startWatchdog(wdMs: Long): Unit = {
    val t = thread("hunter-watchdog") {
      while (true) {
        Thread.sleep(200)
        val idle = (System.nanoTime() - lastTick) / 1000000
        if (idle > wdMs) hangAndHalt(s"no progress for $idle ms (last: $tickWhat)")
      }
    }
    t.start()
  }
  def stacks(all: Boolean): Map[Thread, Array[StackTraceElement]] = Thread.getAllStackTraces.asScala.toMap
  def hangAndHalt(why: String): Unit = {
    println(s"CHILD-HANG $why")
    val st = stacks(true)
    val interesting = st.filter { case (t, s) =>
      val n = t.getName
      n.startsWith("og.") || n.startsWith("hunter") || n == "main" || n.contains("Tracker") || n.contains("ForkJoin") ||
        s.exists(_.getClassName.startsWith("optimus"))
    }
    for ((t, s) <- interesting.toSeq.sortBy(_._1.getName)) {
      println(s"--- ${t.getName} ${t.getState}")
      s.take(45).foreach(e => println("    " + e))
    }
    val mx = java.lang.management.ManagementFactory.getThreadMXBean
    val dl = Option(mx.findDeadlockedThreads()).map(_.toSeq).getOrElse(Nil)
    val dlNames = dl.flatMap(id => Option(mx.getThreadInfo(id))).map(_.getThreadName)
    println(s"DEADLOCKED ${if (dl.isEmpty) "none" else dlNames.mkString(",")}")
    val extra = try {
      val f = new java.util.concurrent.FutureTask[String](() => extraDump())
      val th = thread("hunter-dump")(f.run()); th.start()
      f.get(3, TimeUnit.SECONDS)
    } catch { case t: Throwable => s"extra dump failed: $t" }
    println(extra)
    val txt = interesting.values.flatMap(_.map(_.toString)).mkString("\n")
    def has(s: String) = txt.contains(s)
    val cls =
      if (dl.nonEmpty) "DEADLOCK" + (if (has("maybeRunCleanupNow")) "(R3)" else if (has("fixWaiterChainForXSFT") || has("detectCycles")) "(R11)" else "")
      else if (has("logAndDie")) "LOGANDDIE(F1b)"
      else if (has("waitForCancelledNode") && has("cancelAndRescheduleLowPriorityActions") && has("afterActionCompleted")) "R6(lowPriCancel waits under queueLock; cancelled node's thread blocked in afterActionCompleted)"
      else if (has("waitForCancelledNode") && has("afterActionCompleted")) "R6(waits under queueLock; afterActionCompleted blocked)"
      else if (extra.contains("STRANDED_IN_GLOBAL_QUEUE")) "SCHED_R10b(runnable work on the global queue, workers=0, idle thread parked, schedulerInBadState)"
      else if (extra.contains("COUNTER_LOST")) "QUEUE_WEDGE(R2: many-node evaluation in flight, all its nodes done, completion never ran)"
      else if (extra.contains("WEDGE")) "QUEUE_WEDGE(R1/R2/R10?)"
      else if (has("waitForCancelledNode")) "WAIT_CANCELLED(R6?)"
      else if (extra.contains("STRANDED_IN_CLIENT_QUEUE")) "QUEUE_STALL(nodes stranded in a non-graph client thread's local queue)"
      else if (extra.contains("inFlight:")) "QUEUE_STALL(action in flight, nothing running)"
      else "STALL"
    println(s"HANG-CLASS $cls")
    System.out.flush()
    Runtime.getRuntime.halt(3)
  }
}

// ------------------------------------------------------------------------------------------------ H2 child
object H2Child {
  import HUtil._

  def run(seed: Long, cfg: String): Int = {
    val k = Knobs.fromProps()
    val spec = GSpec.generate(seed, h3 = false)
    val m = new Mat(spec, k)
    CacheFixtures.init()
    val rt = new HRt(m, seed)
    curSys = rt.sys
    val kept = spec.nodes.filter(g => m.keptNode(g.id))
    val treeTweaks = kept.flatMap(_.level.tws)
    val small = rt.configure(treeTweaks)
    val anyTag = kept.exists(g => g.level.tag != null) && !k.has("tags")
    val anyIsdProne = m.gp.exists(g => g.si && g.isdProne) && !k.has("si")
    println(s"KV seed=$seed cfg=$cfg mode=h2 ideal=${Settings.idealThreadCountToCount()} asserts=${Settings.schedulerAsserts}" +
      s" delayMaxNanos=${java.lang.Long.getLong("probes.delay.maxNanos", 0L)} xsftDefault=${CacheFixtures.xsftEnabledByDefault} ${k.describe}")
    println(s"KV spec props=${m.n} E=${m.E} A=${m.A} nodes=${kept.size} clients=${if (k.clients > 0) k.clients else spec.clients}" +
      s" polFail=${rt.polFail.get} xsftSmall=[$small]")
    // coverage of the static spec
    inc("props", m.n)
    for (g <- kept if g.id > 0; t <- g.level.tws if t.prop < m.n) {
      inc("tw.total")
      inc(if (t.inst) "tw.instance" else if (m.gp(t.prop).when != null && !k.has("when")) "tw.when" else "tw.property")
      if (t.rhs != null && !k.has("byname")) inc("tw." + (if (k.has("modes")) Mode.BYNAME else t.mode).toString.toLowerCase)
    }
    for (g <- kept if g.id > 0) {
      inc("nodes"); inc(s"depth${g.depth}")
      if (g.unordered) inc("node.unordered"); if (g.nested) inc("node.nested"); if (g.twice) inc("node.twice")
      if (g.modeTwin) inc("node.modeTwin"); if (g.origin.startsWith("dup")) inc("node.dupSibling"); if (g.level.tag != null) inc("node.tag")
    }
    for (g <- m.gp) inc("pol." + GSpec.polNames(m.policy(g)))
    val nClients = if (k.clients > 0) k.clients else spec.clients
    val ext = new ScheduledThreadPoolExecutor(2, (r: Runnable) => thread("hunter-cancel")(r.run()))
    val oracles = new Array[HOracle](spec.nodes.size)
    def oracle(nid: Int): HOracle = oracles.synchronized {
      if (oracles(nid) == null) oracles(nid) = new HOracle(m, m.nodeLevels(nid), 0, withReads = false)
      oracles(nid)
    }
    startWatchdog(k.wdMs)
    val t0 = System.nanoTime()
    var round = 0
    val roundMs = mutable.ArrayBuffer[Long]()
    while (round == 0 || (round < 20 && (System.nanoTime() - t0) / 1000000 < k.budgetMs)) {
      val tr = System.nanoTime()
      tick(s"round $round build")
      val root = CacheFixtures.newRoot()
      val ss1 = new Array[ScenarioStack](spec.nodes.size)
      val ss2 = new Array[ScenarioStack](spec.nodes.size)
      ss1(0) = root; ss2(0) = root
      for (g <- spec.nodes if g.id > 0 && m.keptNode(g.id) && ss1(g.parent) != null) {
        def scen(n: GNode) = rt.sys.scenario(m.level(n.level), n.unordered && !k.has("unordered"))
        def build(): ScenarioStack =
          if (g.nested && g.parent != 0 && !k.has("nest") && ss1(spec.nodes(g.parent).parent) != null) {
            val par = spec.nodes(g.parent)
            CacheFixtures.child(ss1(par.parent), scen(par).nest(scen(g)))
          } else CacheFixtures.child(ss1(g.parent), scen(g), m.level(g.level).tag)
        try {
          ss1(g.id) = build()
          ss2(g.id) = if (g.twice && !k.has("twice")) build() else ss1(g.id)
        } catch {
          case t: Throwable => inc("buildFail"); if (round == 0) println(s"BUILD-FAIL node ${g.id}: ${brief(t)}")
        }
      }
      val threads = (0 until nClients).map { c =>
        thread(s"hunter-client-$c") {
          CacheFixtures.init()
          val r = new Random(seed * 31 + round * 1000003L + c * 7919L)
          for (i <- 0 until k.iters) {
            val nid = r.nextInt(spec.nodes.size)
            val np = spec.props.size
            val pr = if (r.nextDouble() < 0.6) np / 2 + r.nextInt(np - np / 2) else r.nextInt(np)
            val e = r.nextInt(spec.E + 1) - 1
            val a = r.nextInt(spec.A)
            val cancel = r.nextInt(10) == 0 && !k.has("cancel")
            val delayUs = r.nextInt(3000)
            val v2 = r.nextBoolean()
            if (pr < m.n && m.keptNode(nid) && ss1(nid) != null) {
              val ss = if (v2) ss2(nid) else ss1(nid)
              val key = new Key(m.props(pr), e, a)
              val ssr = if (cancel) ss.withChildCancellationScope() else ss
              if (cancel) { inc("req.cancelScope"); ext.schedule((() => ssr.cancelScope.cancel("hunter")): Runnable, delayUs, TimeUnit.MICROSECONDS) }
              val res: Either[Throwable, java.lang.Long] = try Right(java.lang.Long.valueOf(rt.sys.eval(ssr, key))) catch { case t: Throwable => Left(t) }
              tick(s"round $round client $c iter $i")
              inc("req")
              check(m, spec, oracle(nid), nid, key, res, cancel, cancel && ssr.cancelScope.isCancelled, round, c, i, anyTag, anyIsdProne, ssr)
            }
          }
        }
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
      roundMs += (System.nanoTime() - tr) / 1000000
      round += 1
    }
    val workMs = (System.nanoTime() - t0) / 1000000
    tick("h5")
    val h5 = settle(3000)
    if (h5.nonEmpty) { println(s"FINDING H5 - ${h5.mkString(" ")}"); families.computeIfAbsent("H5:-", _ => new AtomicInteger).incrementAndGet() }
    println(s"KV summary rounds=$round workMs=$workMs maxRoundMs=${roundMs.max} findings=${findings.get} h5=${if (h5.isEmpty) "ok" else h5.mkString(",")}")
    println(s"KV cov $covLine")
    familyLines()
    System.out.flush()
    val oracleFail = families.keySet.asScala.exists(!_.startsWith("H5"))
    if (oracleFail) 1 else if (h5.nonEmpty) 2 else 0
  }

  /** Which node on the way down from key hands back a cancelled result in the uncancelled stack ss. */
  def leakDiag(m: Mat, ss: ScenarioStack, key: Key, depth: Int): Unit = if (depth < 6 && ss != null) {
    val sys = curSys
    val n = CacheFixtures.evalIn(ss) { val ec = OGSchedulerContext.current(); ec.scenarioStack.getNode(sys.newKey(key), ec) }.asInstanceOf[NodeTask]
    val ncs = n.scenarioStack.cancelScope
    println(s"LEAKDIAG ${"  " * depth}$key -> ${n.getClass.getSimpleName}@${Integer.toHexString(System.identityHashCode(n))} state=${n.stateAsString} done=${n.isDone} exc=${if (n.isDoneWithException) brief(n.exception).take(60) else "-"} nodeCS=$ncs cancelled=${ncs.isCancelled} requesterCS=${ss.cancelScope} sameCSId=${n.scenarioStack.cancelScopeAndEnvId == ss.cancelScopeAndEnvId} nodeSS=${n.scenarioStack} reqSS=$ss")
    if (n.isDoneWithException && isCancel(n.exception)) key.prop.reads.asScala.foreach(r => leakDiag(m, ss, r.resolve(key), depth + 1))
  }
  @volatile var curSys: ProbeSystem = null

  def check(m: Mat, spec: GSpec, o: HOracle, nid: Int, key: Key, res: Either[Throwable, java.lang.Long], cancel: Boolean,
      wasCancelled: Boolean, round: Int, c: Int, i: Int, anyTag: Boolean, anyIsdProne: Boolean, ssUsed: ScenarioStack = null): Unit = {
    val ex = o.eval(key)
    def suspects(sysIsd: Boolean): List[String] = {
      val path = m.path(nid)
      val cl = m.closure(key.prop.id, path.flatMap(_.level.tws))
      val kinds = m.kindsIn(cl)
      val s = mutable.ListBuffer[String]()
      if (kinds("TAG") && anyTag) s += "F8"
      if (path.exists(_.modeTwin) && !m.k.has("modes") && !m.k.has("byname")) s += "F1"
      if (sysIsd && !ex.isd && anyIsdProne) s += "D4"
      if (!sysIsd && ex.isd && (kinds("SI") || kinds("SI!"))) s += "D4" // dual: root value served into the SI stack (shared cache id)
      if (kinds("XS")) s += "XS"
      if (kinds("XSFTsmall")) s += "XSFTsmall"
      if (kinds("ALIAS")) s += "ALIAS"
      if (cancel) s += "CANCEL"
      if (kinds("XSFT")) s += "XSFT"
      s.toList
    }
    def report(what: String, sysV: String, sysIsd: Boolean): Unit = {
      val sus = suspects(sysIsd)
      val fam = sus.headOption.getOrElse("UNEXPLAINED")
      finding("ORACLE", fam, s"$what round=$round client=$c iter=$i node=$nid path=${m.path(nid).map(_.id).mkString("/")} key=$key " +
        s"system=$sysV oracle=${if (ex.isd) "ISD" else ex.v} cancelScope=$cancel suspects=${sus.mkString(",")}")
    }
    res match {
      case Right(v) =>
        inc("req.value")
        if (ex.isd) report("value-where-ISD", v.toString, false)
        else if (v.longValue != ex.v) report("wrong-value", v.toString, false)
      case Left(t) =>
        if (wasCancelled) inc("req.cancelled")
        else if (isCancel(t)) { if (!cancel && printed.computeIfAbsent("leakstack", _ => new AtomicInteger).incrementAndGet() <= 2) { println("CANCEL_LEAK-STACK"); chain(t).foreach { x => println("  " + x); x.getStackTrace.take(30).foreach(e => println("      " + e)) } }
          if (!cancel && printed.computeIfAbsent("leakdiag", _ => new AtomicInteger).incrementAndGet() <= 3) leakDiag(m, ssUsed, key, 0)
          if (cancel) inc("req.cancelled") else finding("CANCEL_LEAK", "-", s"round=$round client=$c iter=$i node=$nid key=$key pol=${GSpec.polNames(m.policy(m.gp(key.prop.id)))} closure=${m.kindsIn(m.closure(key.prop.id, m.path(nid).flatMap(_.level.tws))).toSeq.sorted.mkString(",")} ${brief(t)}") }
        else if (isISD(t)) { inc("req.isd"); if (!ex.isd) report("ISD-where-value", "ISD", true) }
        else if (String.valueOf(t.getMessage).contains("in cross-scenario node") && String.valueOf(t.getMessage).contains("is not supported")) inc("req.xsBindUnsupported")
        else finding("EXC", t.getClass.getSimpleName, s"node=$nid key=$key oracle=${if (ex.isd) "ISD" else ex.v} ${brief(t)}")
    }
  }
}

// ------------------------------------------------------------------------------------------------ H3 child
object H3Child {
  import HUtil._
  import GSpec._

  final class TState(val tws: Vector[(Long, GTw)], val tag: java.lang.Long) {
    def withTws(m: Mat, add: Seq[GTw]): TState = {
      var v = tws
      for (t <- add) {
        val tg = t.target(m.A, m.E)
        val i = v.indexWhere(_._1 == tg)
        v = if (i >= 0) v.updated(i, (tg, t)) else v :+ ((tg, t))
      }
      new TState(v, tag)
    }
    def withTag(t: java.lang.Long): TState = new TState(tws, t)
  }

  final class TI(val g: GTracker, val parent: TI) {
    val path: Vector[TI] = if (parent == null) Vector(this) else parent.path :+ this
    @volatile var t: DependencyTracker = _
    @volatile var states: Vector[TState] = Vector(new TState(Vector.empty, null))
    val submitted = new AtomicInteger
    val completed = new AtomicInteger
    @volatile var qlock: AnyRef = _
    @volatile var alive = false
    @volatile var created = false
    val touchedKeys = new BitSet
    val touchedProps = new BitSet
    def level(m: Mat, v: Int): Level = { val s = states(v); m.levelOf(s, s.tws.map(_._2), s.tag) }
    def id: Int = g.id
  }

  def onDone[A](f: CompletableFuture[A])(cb: (A, Throwable) => Unit): CompletableFuture[A] =
    f.whenComplete((a: A, e: Throwable) => cb(a, e))

  def run(seed: Long, cfg: String): Int = {
    val k = Knobs.fromProps()
    val spec = GSpec.generate(seed, h3 = true)
    val m = new Mat(spec, k)
    CacheFixtures.init()
    val rt = new HRt(m, seed)
    rt.configure(Nil)
    val tagsOn = spec.tagsOn && !k.has("tags")
    val anyIsdProne = m.gp.exists(g => g.si && g.isdProne) && !k.has("si")
    val tweakables = m.gp.filter(_.tweakable).map(_.id)
    val wm = Settings.ttrackClearedNodeRefWatermark
    println(s"KV seed=$seed cfg=$cfg mode=h3 ideal=${Settings.idealThreadCountToCount()} asserts=${Settings.schedulerAsserts}" +
      s" delayMaxNanos=${java.lang.Long.getLong("probes.delay.maxNanos", 0L)} watermark=$wm ${k.describe}")
    inc("props", m.n)
    for (g <- m.gp) inc("pol." + GSpec.polNames(m.policy(g)))
    // trackers
    val tis = new Array[TI](spec.trackers.size)
    for (g <- spec.trackers) {
      val par = if (g.parent < 0) null else tis(g.parent)
      tis(g.id) = if (g.id != 0 && (k.drop.contains(g.id) || par == null)) null else new TI(g, par)
    }
    val mainScheduler = EvaluationContext.scheduler.asInstanceOf[OGScheduler]
    val listenerToTi = new ConcurrentHashMap[AnyRef, TI]()
    def create(ti: TI): Unit = {
      ti.created = true
      ti.t =
        if (ti.parent == null) T.newRoot()
        else if (ti.g.concurrent) T.concurrentChild(ti.parent.t, s"t${ti.id}")
        else T.consistentChild(ti.parent.t, s"t${ti.id}")
      ti.qlock = if (ti.parent == null || ti.g.concurrent) new Object else ti.parent.qlock
      val tl = T.tweakableTracker(ti.t)
      listenerToTi.put(tl, ti)
      listenerToTi.put(ti.t.scenarioStack.tweakableListener, ti)
      ti.alive = true
      inc(if (ti.parent == null) "trk.root" else if (ti.g.concurrent) "trk.concurrent" else "trk.consistent")
    }
    for (ti <- tis if ti != null && !ti.g.late && (ti.parent == null || ti.parent.created)) create(ti)
    println(s"KV spec props=${m.n} E=${m.E} A=${m.A} trackers=${tis.count(_ != null)} clients=${if (k.clients > 0) k.clients else spec.clients}" +
      s" tagsOn=$tagsOn polFail=${rt.polFail.get} plan=${spec.trackers.map(t => s"${t.id}<${t.parent}${if (t.concurrent) "c" else ""}${if (t.late) "L" else ""}").mkString(",")}")
    val recorded = new ConcurrentHashMap[java.lang.Long, NodeTask]()
    rt.sys.runHook = (n: ProbePropertyNode) => {
      val ss = n.scenarioStack()
      if (ss != null) {
        val l = ss.tweakableListener
        var ti = listenerToTi.get(l)
        if (ti == null && l != null) ti = listenerToTi.get(l.underlyingListener)
        if (ti != null) recorded.put(java.lang.Long.valueOf((ti.id.toLong << 32) | m.keyIdx(n.dkey)), n)
      }
    }
    val ext = new ScheduledThreadPoolExecutor(2, (r: Runnable) => thread("hunter-cancel")(r.run()))
    val phaseLog = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val phaseQueues = ConcurrentHashMap.newKeySet[AnyRef]()
    val phaseFeatures = ConcurrentHashMap.newKeySet[String]()
    def log(s: String): Unit = phaseLog.add(s)

    val clientCtx = new ConcurrentHashMap[String, OGSchedulerContext]()
    def localQueueTasks(ctx: OGSchedulerContext): Seq[NodeTask] = {
      val q = T.field(ctx, "localQ")
      def f(n: String) = { val x = q.getClass.getDeclaredField(n); x.setAccessible(true); x.get(q) }
      val arr = f("_q").asInstanceOf[Array[NodeTask]]
      val top = f("_top").asInstanceOf[Int]; val base = f("_base").asInstanceOf[Int]
      (base until top).take(arr.length).map(i => arr(i & (arr.length - 1))).filter(t => t != null && (t ne NodeTask.nullTask))
    }
    extraDump = () => {
      val sb = new StringBuilder("TRACKERS")
      for (ti <- tis if ti != null && ti.alive) {
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
              sb.append(s"\n      nodes: ${ns.map(n => s"$n:${n.stateAsString}:cancelled=${n.scenarioStack.cancelScope.isCancelled}").mkString(" ")} allDoneOrInvalid=$allDone")
              if (allDone) sb.append(" COUNTER_LOST")
            }
          } catch { case t: Throwable => sb.append(s" (nodes: $t)") }
        }
      }
      clientCtx.forEach { (nm, ctx) =>
        val ts = try localQueueTasks(ctx) catch { case t: Throwable => Nil }
        if (ts.nonEmpty) sb.append(s"\n  $nm localQueue=${ts.size} STRANDED_IN_CLIENT_QUEUE: ${ts.take(6).map(t => s"$t:${t.stateAsString}").mkString(" ")}")
      }
      try {
        val sch = mainScheduler
        def sf(n: String) = { val x = classOf[OGScheduler].getDeclaredField(n); x.setAccessible(true); x.get(sch) }
        val qq = sf("_qqueue")
        def qf(n: String) = { val x = qq.getClass.getDeclaredField(n); x.setAccessible(true); x.get(qq) }
        val arr = qf("_q").asInstanceOf[Array[NodeTask]]; val top = qf("_top").asInstanceOf[Int]; val base = qf("_base").asInstanceOf[Int]
        val g = (base until top).take(arr.length).map(i => arr(i & (arr.length - 1))).filter(t => t != null && (t ne NodeTask.nullTask))
        val pq = sf("_queue").asInstanceOf[java.util.PriorityQueue[NodeTask]].asScala.toSeq
        val waiters = sf("_waiters").asInstanceOf[optimus.core.IndexedArrayList[_]].count
        val idle = sf("_idlePool").asInstanceOf[optimus.core.IndexedArrayList[_]].count
        val workers = sf("_workers").asInstanceOf[optimus.core.IndexedArrayList[_]].count
        val spin = { val x = classOf[OGScheduler].getDeclaredField("_spinningThreadID"); x.setAccessible(true); x.getInt(sch) }
        val ideal = { val x = classOf[OGScheduler].getDeclaredField("_idealCount"); x.setAccessible(true); x.getInt(sch) }
        val extB = sf("_externallyBlocked").asInstanceOf[optimus.core.IndexedArrayList[_]].count
        sb.append(s"\n  scheduler: qqueue=${g.size} queue=${pq.size} waiters=$waiters idle=$idle workers=$workers spinningThreadID=$spin ideal=$ideal externallyBlocked=$extB badState=${sch.schedulerInBadState()}")
        if (g.exists(!_.isDoneOrNotRunnable) || pq.exists(!_.isDoneOrNotRunnable)) sb.append(s" STRANDED_IN_GLOBAL_QUEUE: ${(g ++ pq).take(6).map(t => s"$t:${t.stateAsString}").mkString(" ")}")
      } catch { case t: Throwable => sb.append(s"\n  scheduler dump failed $t") }
      sb.append("\nPHASE-LOG ").append(phaseLog.asScala.mkString(" | "))
      sb.toString
    }

    val poisoned = ConcurrentHashMap.newKeySet[Integer]()
    def isPoisoned(ti: TI): Boolean = ti.path.exists(a => poisoned.contains(a.id))
    /** An update that threw: the model no longer knows what the tracker holds; stop checking its subtree. */
    def updateFailed(ti: TI, what: String, e: Throwable): Unit = {
      poisoned.add(ti.id)
      val fam = if (String.valueOf(e).contains("EventCauseInInvalidState") && HUtil.chain(e).exists(_.getStackTrace.exists(_.getMethodName.contains("cancelAndRescheduleLowPriorityActions")))) "R14(EventCause)"
        else e.getClass.getSimpleName
      finding("UPDATE_FAILED", fam, s"t${ti.id} $what ${brief(e)}")
    }
    // ---- oracles
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
      s.toList
    }
    val known = Set("F8", "D4", "XS")
    // poisoned trackers are excluded from the value checks too
    def checkEval(ti: TI, key: Key, lo: Seq[Int], hi: Seq[Int], res: Either[Throwable, java.lang.Long], how: String,
        cancel: Boolean, wasCancelled: Boolean): Unit = {
      if (isPoisoned(ti)) { inc("eval.skippedPoisoned"); return }
      inc("eval.checked")
      val ranges = lo.zip(hi).map { case (a, b) => a to math.max(a, b) }
      val nCombo = ranges.map(_.size.toLong).product
      if (nCombo > 48) { inc("eval.ambiguous"); return }
      val combos = ranges.foldLeft(Seq(Seq.empty[Int]))((acc, rg) => for (c <- acc; v <- rg) yield c :+ v)
      val ex = combos.map(c => oracleFor(ti, c).eval(key))
      def report(what: String, sysV: String, sysIsd: Boolean): Unit = {
        val sus = suspects(ti, key, sysIsd, ex.exists(_.isd), cancel, nCombo)
        val fam = sus.find(known).getOrElse("UNEXPLAINED")
        finding("ORACLE", fam, s"$what via=$how tracker=t${ti.id} path=${ti.path.map(_.id).mkString("/")} key=$key system=$sysV " +
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
          else if (chain(t).exists(x => x.isInstanceOf[ClassCastException] && String.valueOf(x.getMessage).contains("NodeInputMapValue"))) finding("EXC", "TAG_DOUBLE_WRAP(new)", s"via=$how tracker=t${ti.id} key=$key ${brief(t)}", 3)
          else if (t.isInstanceOf[NotDoneMarker]) finding("NOT_DONE_IN_CALLBACK", how, s"tracker=t${ti.id} key=$key node=${t.getMessage} oracle=${ex.map(r => if (r.isd) "ISD" else r.v.toString).distinct.mkString("|")} tweaks@path=${ti.path.map(a => s"t${a.id}:" + a.states.last.tws.map(_._2).filter(_.prop == key.prop.id).mkString(",")).mkString(" ")}")
          else if (String.valueOf(t.getMessage).contains("in cross-scenario node") && String.valueOf(t.getMessage).contains("is not supported")) inc("eval.xsBindUnsupported")
          else finding("EXC", t.getClass.getSimpleName, s"via=$how tracker=t${ti.id} key=$key ${brief(t)}")
      }
    }
    def nodeRes(n: AnyRef): Either[Throwable, java.lang.Long] = {
      val nt = n.asInstanceOf[NodeTask]
      if (!nt.isDone && nt.isInvalidCache) Left(InvalidatedBeforeRead)
      else if (!nt.isDone) {
        val st0 = nt.stateAsString
        val cs = nt.scenarioStack.cancelScope
        val t0 = System.nanoTime()
        while (!nt.isDone && System.nanoTime() - t0 < 2000000000L) Thread.sleep(1)
        val after = if (!nt.isDone) "still running after 2s" else if (nt.isDoneWithException) s"done after ${(System.nanoTime() - t0) / 1000}us with ${brief(nt.exception)}" else s"done after ${(System.nanoTime() - t0) / 1000}us value=${nt.resultObject}"
        Left(new NotDoneMarker(s"${nt.getClass.getSimpleName} $nt state=$st0 cancelScope=$cs cancelled=${cs.isCancelled} invalid=${nt.isInvalidCache} ss=${nt.scenarioStack} -> $after"))
      }
      else if (nt.isDoneWithException) Left(nt.exception) else Right(nt.resultObject.asInstanceOf[java.lang.Long])
    }
    def randomKeys(r: Random): Seq[Key] = (0 until 1 + r.nextInt(4)).map { _ =>
      val pr = if (r.nextDouble() < 0.6) m.n / 2 + r.nextInt(m.n - m.n / 2) else r.nextInt(m.n)
      new Key(m.props(pr), r.nextInt(m.E + 1) - 1, r.nextInt(m.A))
    }
    def window(ti: TI): Seq[Int] = ti.path.map(_.completed.get)
    def windowHi(ti: TI): Seq[Int] = ti.path.map(_.submitted.get)

    // ---- updates
    def bump(ti: TI, f: TState => TState, tws: Seq[GTw], tag: Boolean): Int = {
      ti.states = ti.states :+ f(ti.states.last)
      val j = ti.states.size - 1
      ti.submitted.set(j)
      ti.synchronized {
        tws.foreach(t => if (t.inst) ti.touchedKeys.set(m.keyIdx(t.prop, t.ent, t.arg)) else ti.touchedProps.set(t.prop))
      }
      phaseQueues.add(ti.qlock)
      for (t <- tws) {
        inc("tw.total"); inc(if (t.inst) "tw.instance" else if (m.gp(t.prop).when != null && !k.has("when")) "tw.when" else "tw.property")
        if (t.rhs != null && !k.has("byname")) inc("tw." + (if (k.has("modes")) Mode.BYNAME else t.mode).toString.toLowerCase)
      }
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
      for (a <- 0 until 1 + r.nextInt(3) if ti.alive) {
        val x0 = r.nextInt(100)
        val off = (x0 < 18 && k.has("addsync")) || (x0 >= 18 && x0 < 32 && k.has("addasync")) || (x0 >= 32 && x0 < 42 && k.has("batch")) ||
          (x0 >= 42 && x0 < 52 && k.has("evalsync")) || (x0 >= 52 && x0 < 62 && k.has("evalasync")) || (x0 >= 62 && x0 < 72 && k.has("lp")) ||
          (x0 >= 72 && x0 < 80 && k.has("newchild")) || (x0 >= 80 && x0 < 88 && k.has("tagupd")) || (x0 >= 88 && k.has("canceleval"))
        val x = if (off) 1000 else x0
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
                try futures.add(onDone(T.addTweaksAsync(ti.t, tweaks)) { (_, e) =>
                  done(ti, j); if (e != null) updateFailed(ti, "addTweaksAsync", e)
                })
                catch { case e: Throwable => updateFailed(ti, "addTweaksAsync(submit)", e); done(ti, j) }
            }
          } else if (x < 42) {
            val sync = r.nextBoolean()
            ti.qlock.synchronized {
              val kids = tis.filter(c => c != null && c.alive && (c.parent eq ti) && !c.g.concurrent).filter(_ => r.nextBoolean())
              val parts = (ti +: kids.toSeq).map(c => (c, genTweaks(r)))
              val tag = if (tagsOn && r.nextInt(3) == 0) Some(java.lang.Long.valueOf(1 + r.nextInt(5))) else None
              val js = parts.map { case (c, tws) =>
                (c, bump(c, s => { val s2 = s.withTws(m, tws); if ((c eq ti) && tag.isDefined) s2.withTag(tag.get) else s2 }, tws, (c eq ti) && tag.isDefined))
              }
              val prepared = parts.map { case (c, tws) => (c, tweaksOf(tws)) }
              log(s"t${ti.id} batch${if (sync) "" else "Async"} ${parts.map { case (c, tws) => s"t${c.id}:[${tws.mkString(", ")}]" }.mkString(" ")}${tag.map(" tag=" + _).getOrElse("")}")
              inc(if (sync) "upd.batchSync" else "upd.batchAsync"); if (kids.nonEmpty) inc("upd.batchMulti")
              phaseFeatures.add("batch")
              val f: DependencyTrackerBatchUpdater => Unit = u => {
                prepared.foreach { case (c, tw) => if (tw.nonEmpty) u.updaterFor(c.t).addTweaksImmediate(tw, throwOnDuplicate = false) }
                tag.foreach(v => u.setPluginTag(ProbeTagKey, v))
              }
              if (sync) { try T.batch(ti.t)(f) catch { case e: Throwable => js.foreach { case (c, _) => poisoned.add(c.id) }; updateFailed(ti, "batch", e) }; js.foreach { case (c, j) => done(c, j) } }
              else
                try futures.add(onDone(T.batchAsync(ti.t)(f)) { (_, e) =>
                  js.foreach { case (c, j) => done(c, j) }; if (e != null) updateFailed(ti, "batchAsync", e)
                })
                catch { case e: Throwable => js.foreach { case (c, j) => poisoned.add(c.id); done(c, j) }; updateFailed(ti, "batchAsync(submit)", e) }
            }
          } else if (x < 52) {
            val keys = randomKeys(r)
            inc("eval.driverSync")
            T.evaluateKeys(ti.t, keys.map(rt.sys.newKey(_)))
          } else if (x < 72) {
            val lp = x >= 62
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
            val late = tis.find(c => c != null && !c.created && (c.parent eq ti))
            late.foreach { c =>
              ti.qlock.synchronized { create(c) }
              log(s"t${ti.id} newChildScenario t${c.id}${if (c.g.concurrent) " concurrent" else ""}")
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
          } else if (x < 100) {
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
            finding("ACTION_FAILED", fam, s"t${ti.id} action=$x ${brief(t)}")
        }
      }
    }

    // ---- clients
    val nClients = if (k.clients > 0) k.clients else spec.clients
    @volatile var pause = false
    @volatile var stop = false
    val paused = new AtomicInteger
    val clients = (0 until nClients).map { c =>
      thread(s"hunter-client-$c") {
        CacheFixtures.init()
        clientCtx.put(s"client$c", OGSchedulerContext.current())
        val r = new Random(seed * 17 + c)
        var i = 0
        while (!stop) {
          if (pause) {
            paused.incrementAndGet()
            while (pause && !stop) Thread.sleep(1)
            paused.decrementAndGet()
          } else {
            val alive = tis.filter(t => t != null && t.alive)
            val ti = alive(r.nextInt(alive.length))
            val keys = randomKeys(r)
            val how0 = r.nextInt(3)
            val how = if (how0 == 2 && k.has("lp")) 1 else if (how0 == 1 && k.has("clientasync")) 0 else how0
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

    // ---- checkpoint
    final class Snap(val ti: TI, val keys: BitSet, val props: BitSet)
    var prevSnap = new java.util.IdentityHashMap[NodeTask, Snap]()
    val infoToProp = new java.util.IdentityHashMap[NodeTaskInfo, Integer]()
    for (i <- 0 until m.n) infoToProp.put(rt.info(i), i)
    val entByHash = (-1 until m.E).groupBy(e => rt.sys.entity(e).hashCode)
    def checkpoint(phase: Int): Unit = {
      tick(s"checkpoint $phase idle")
      val alive = tis.filter(t => t != null && t.alive)
      def idle(ti: TI) = T.workQueueSize(ti.t) == 0 && T.lowPriorityQueueSize(ti.t) == 0 && T.inFlightWork(ti.t).isEmpty && !T.isUpdating(ti.t)
      val ti0 = System.nanoTime()
      while (!alive.forall(idle)) {
        if ((System.nanoTime() - ti0) / 1000000 > k.wdMs) hangAndHalt(s"queues not idle at checkpoint $phase")
        Thread.sleep(2)
      }
      for (ti <- alive if ti.completed.get != ti.submitted.get && !isPoisoned(ti))
        finding("VERSION_LAG", "-", s"t${ti.id} submitted=${ti.submitted.get} completed=${ti.completed.get}")
      tick(s"checkpoint $phase oracles")
      val cur = alive.map(ti => ti -> new HOracle(m, ti.path.map(a => a.level(m, a.states.size - 1)).toArray, 0, withReads = true)).toMap
      val curTT = alive.map(ti => ti -> new HOracle(m, cur(ti).levels, 0, withReads = true, ttMode = true)).toMap
      // over/under invalidation against the previous snapshot
      val feats = phaseFeatures.asScala.toSeq.sorted.mkString(",")
      prevSnap.forEach { (n, s) =>
        val inv = n.isInvalidCache
        val touched = s.ti.path.exists(a => a.synchronized(a.touchedKeys.intersects(s.keys) || a.touchedProps.intersects(s.props)))
        if (inv && !touched) {
          inc("inv.over")
          if (printed.computeIfAbsent("SAMPLE-OVERINV", _ => new AtomicInteger).incrementAndGet() <= 6)
            println(s"SAMPLE OVERINV phase=$phase node=$n tracker=t${s.ti.id} features=[$feats] log=${phaseLog.asScala.mkString(" | ").take(600)}")
        } else if (!inv && touched) inc("inv.under") else if (inv) inc("inv.expected") else inc("inv.untouchedValid")
      }
      for (ti <- alive) ti.synchronized { ti.touchedKeys.clear(); ti.touchedProps.clear() }
      // stale + snapshot + ttrack
      tick(s"checkpoint $phase nodes")
      val snap = new java.util.IdentityHashMap[NodeTask, Snap]()
      val ttrackTodo = mutable.ArrayBuffer[(NodeTask, TI, Key, BitSet)]()
      recorded.forEach { (kk, n) =>
        val ti = tis(((kk.longValue >>> 32) & 0xffff).toInt)
        val key = m.keyOf((kk.longValue & 0xffffffffL).toInt)
        val g = m.gp(key.prop.id)
        if (ti != null && ti.alive && !g.si && g.pol != POL_DONTCACHE && !isPoisoned(ti)) {
          if (n.isInvalidCache) inc("chk.invalid")
          else if (!n.isDone) inc("chk.notDone")
          else if (!n.isDoneWithException && !n.isInvalidCache) {
            inc("chk.valid")
            val ex = cur(ti).eval(key)
            val v = n.resultObject.asInstanceOf[java.lang.Long]
            if (ex.isd || v.longValue != ex.v) {
              val served = try { val sv = T.evaluate(ti.t, rt.sys.newKey(key)); if (ex.isd) "value " + sv else sv.toString } catch { case t: Throwable => if (isISD(t)) "ISD" else brief(t) }
              val servedStale = served != (if (ex.isd) "ISD" else ex.v.toString)
              val sus = suspects(ti, key, false, ex.isd, false, 1) ++ (if (wm <= 1) List("CLEANUP_WM1") else Nil) ++ phaseFeatures.asScala.map("phase:" + _)
              val fam = sus.find(known).getOrElse(if (servedStale) "UNEXPLAINED" else "LATENT")
              finding(if (servedStale) "STALE_SERVED" else "STALE_LATENT", fam,
                s"phase=$phase tracker=t${ti.id} path=${ti.path.map(_.id).mkString("/")} key=$key node=$n cached=$v oracle=${if (ex.isd) "ISD" else ex.v} servedNow=$served " +
                  s"suspects=${sus.mkString(",")} log=${phaseLog.asScala.mkString(" | ").take(800)}")
            } else {
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
      // TTrack sanity: every valid cached node reachable from a root of every tweakable it read
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
            if (roots.isEmpty) {
              bad += s"NOROOT node=$key@t${ti.id} reads=$rk"
              if (java.lang.Boolean.getBoolean("hunter.ttdebug") && printed.computeIfAbsent("ttdebug", _ => new AtomicInteger).incrementAndGet() <= 3) {
                for (a <- alive) T.ttracks(a.t).forEach { (tk, _) =>
                  if (tk.propertyInfo eq rt.info(rk.prop.id)) println(s"  TTDEBUG t${a.id} key=${tk.getClass.getSimpleName} $tk")
                }
                println(s"  TTDEBUG trackers states: " + alive.map(a => s"t${a.id}:${a.states.last.tws.map(_._2).mkString(",")}").mkString(" ; "))
              }
            }
            else if (!roots.exists(r => reached(r).contains(n))) bad += s"UNREACHABLE node=$key@t${ti.id} reads=$rk roots=${roots.size}"
            i = reads.nextSetBit(i + 1)
          }
        }
        bad.toSeq
      }
      var bad = ttrackPass()
      if (bad.nonEmpty) { Thread.sleep(300); bad = ttrackPass() }
      inc("ttrack.nodesChecked", ttrackTodo.size)
      for (b <- bad) {
        val kind = b.takeWhile(_ != ' ')
        finding("TTRACK", kind + (if (wm <= 1) "-wm1" else ""), s"phase=$phase $b features=[$feats]")
      }
      inc("checkpoints")
    }

    // ---- main loop
    startWatchdog(k.wdMs)
    clients.foreach(_.start())
    val drivers = Executors.newFixedThreadPool(tis.length, (r: Runnable) => thread("hunter-driver") { CacheFixtures.init(); r.run() })
    val t0 = System.nanoTime()
    var phase = 0
    val phaseMs = mutable.ArrayBuffer[Long]()
    while (phase < 3 || (phase < k.phases && (System.nanoTime() - t0) / 1000000 < k.budgetMs)) {
      val tp = System.nanoTime()
      phaseLog.clear(); phaseQueues.clear(); phaseFeatures.clear()
      val alive = tis.filter(t => t != null && t.alive)
      val tasks = alive.map(ti => drivers.submit((() => driverPhase(ti, phase)): Runnable))
      tasks.foreach(_.get())
      tick(s"phase $phase futures")
      var f = futures.poll()
      while (f != null) {
        try f.get() catch { case _: Throwable => } // failures reported by the callbacks
        f = futures.poll()
      }
      if (phaseQueues.size > 1) phaseFeatures.add("multiQueueUpdates")
      tick(s"phase $phase pause")
      pause = true
      while (paused.get < nClients) Thread.sleep(1)
      checkpoint(phase)
      pause = false
      phaseMs += (System.nanoTime() - tp) / 1000000
      phase += 1
      inc("phases")
    }
    stop = true
    clients.foreach(_.join(k.wdMs))
    val workMs = (System.nanoTime() - t0) / 1000000
    tick("dispose")
    val dr = new Random(seed + 99)
    for (ti <- tis.reverse if ti != null && ti.alive && ti.parent != null && dr.nextBoolean()) { T.dispose(ti.t); ti.alive = false; inc("disposed") }
    T.disposeAllRoots()
    tick("h5")
    val h5 = settle(3000)
    if (h5.nonEmpty) { println(s"FINDING H5 - ${h5.mkString(" ")}"); families.computeIfAbsent("H5:-", _ => new AtomicInteger).incrementAndGet() }
    println(s"KV summary phases=$phase workMs=$workMs maxPhaseMs=${if (phaseMs.isEmpty) 0 else phaseMs.max} findings=${findings.get} h5=${if (h5.isEmpty) "ok" else h5.mkString(",")}")
    println(s"KV cov $covLine")
    familyLines()
    System.out.flush()
    val fail = families.keySet.asScala.exists(f => !f.startsWith("H5") && !f.startsWith("STALE_LATENT"))
    if (fail) 1 else if (h5.nonEmpty) 2 else 0
  }
}

// ------------------------------------------------------------------------------------------------ parent
object CacheHunter {
  final case class Res(mode: String, seed: Long, cfg: String, knobs: Seq[(String, String)], exit: Int, cls: String,
      families: Map[String, Int], kv: Map[String, String], wallMs: Long, log: String, firstFindings: Seq[String]) {
    def failed: Boolean = cls != "PASS"
    def famKeys: Set[String] =
      if (cls.startsWith("HANG") || cls.startsWith("CRASH")) Set(cls.replace("+F1b", ""))
      else families.keySet.filterNot(_.startsWith("STALE_LATENT"))
  }

  def cfgs(mode: String): Seq[String] =
    for (i <- Seq(1, 2, 4); a <- 0 to 1; d <- 0 to 1; w <- if (mode == "h3") Seq(0, 1) else Seq(-1))
      yield s"i$i-a$a-d$d" + (if (w >= 0) s"-w$w" else "")
  def baseline(mode: String): String = if (mode == "h3") "i4-a0-d0-w0" else "i4-a0-d0"

  def flags(cfg: String): Seq[String] = {
    val p = cfg.split("-")
    val b = mutable.ArrayBuffer[String]()
    for (x <- p) x.head match {
      case 'i' => b += s"-Doptimus.gthread.ideal=${x.tail}"
      case 'a' => b += s"-Doptimus.scheduler.asserts=${x.tail == "1"}"
      case 'd' => if (x.tail == "1") b += s"-Dprobes.delay.maxNanos=${System.getProperty("hunter.delayNanos", "50000")}"
      case 'w' => if (x.tail == "1") b += s"-D${CacheFixtures.watermarkProperty}=1"
      case 'x' => if (x.tail == "0") b += "-Doptimus.profile.enableXSFT=false"
      case _   =>
    }
    b.toSeq
  }

  def logDir: java.nio.file.Path = {
    val p = java.nio.file.Path.of(sys.env.getOrElse("HUNTER_LOGDIR", "/tmp/cache-hunter"))
    java.nio.file.Files.createDirectories(p)
    p
  }
  val runCounter = new AtomicInteger

  def runChild(mode: String, seed: Long, cfg: String, knobs: Seq[(String, String)], wdMs: Long, tag: String): Res = {
    val cmd = sys.env.getOrElse("HUNTER_JAVA_CMD", throw new IllegalStateException("HUNTER_JAVA_CMD not set; use run-cache-hunter.sh"))
    val argv = mutable.ArrayBuffer[String]("java")
    argv ++= flags(cfg)
    argv += s"-Dhunter.wdMs=$wdMs"
    for ((kk, v) <- knobs) argv += s"-Dhunter.$kk=$v"
    for (kk <- System.getProperties.stringPropertyNames.asScala.toSeq.sorted if kk.startsWith("hunter.") && kk != "hunter.wdMs" && kk != "hunter.delayNanos")
      if (!knobs.exists(_._1 == kk.stripPrefix("hunter."))) argv += s"-D$kk=${System.getProperty(kk)}"
    argv ++= cmd.stripPrefix("java ").trim.split("\\s+")
    argv ++= Seq("optimus.graph.CacheHunter", "--child", mode, seed.toString, cfg)
    val kt = knobs.map { case (a, b) => s"$a=$b" }.mkString("_").replaceAll("[^A-Za-z0-9=_.]", ".")
    val log = logDir.resolve(s"$mode-s$seed-$cfg${if (kt.nonEmpty) "-" + kt.take(80) else ""}${if (tag.nonEmpty) "-" + tag else ""}.log")
    val pb = new ProcessBuilder(argv.asJava).redirectErrorStream(true).redirectOutput(log.toFile)
    val t0 = System.nanoTime()
    val p = pb.start()
    val budget = java.lang.Long.getLong("hunter.budgetMs", 3000L)
    val fin = p.waitFor(budget / 1000 + 3 * wdMs / 1000 + 60, TimeUnit.SECONDS)
    if (!fin) p.destroyForcibly().waitFor()
    val wall = (System.nanoTime() - t0) / 1000000
    runCounter.incrementAndGet()
    val lines = java.nio.file.Files.readAllLines(log, java.nio.charset.StandardCharsets.ISO_8859_1).asScala.toSeq
    classify(mode, seed, cfg, knobs, if (fin) p.exitValue else -1, lines, wall, log.toString)
  }

  def classify(mode: String, seed: Long, cfg: String, knobs: Seq[(String, String)], exit: Int, lines: Seq[String], wall: Long, log: String): Res = {
    val kv = mutable.LinkedHashMap[String, String]()
    val fam = mutable.LinkedHashMap[String, Int]()
    var hang: String = null
    val first = mutable.ArrayBuffer[String]()
    for (l <- lines) {
      if (l.startsWith("KV ")) for (t <- l.substring(3).split(" ")) { val e = t.indexOf('='); if (e > 0) kv.getOrElseUpdate(t.substring(0, e), t.substring(e + 1)) }
      if (l.startsWith("FAMILY ")) { val p = l.split(" "); fam(p(1)) = p(2).toInt }
      if (l.startsWith("HANG-CLASS ")) hang = l.substring(11).trim
      if (l.startsWith("FINDING ") && first.size < 3) first += l.take(700)
      if (l.startsWith("CHILD-HANG") && first.size < 3) first += l
    }
    val txt = lines.mkString("\n")
    val f1b = txt.contains("logAndDie called recursively") || txt.contains("InfoDumper")
    var cls = exit match {
      case 0  => "PASS"
      case 1  => "FAIL"
      case 2  => "H5"
      case 3  => s"HANG:${Option(hang).getOrElse("?")}"
      case -1 => "HANG:unresponsive"
      case x  => s"CRASH(exit=$x)"
    }
    if (f1b) {
      cls += "+F1b"
      if (txt.contains("ClassCastException") && txt.contains("processOnChildCompleted")) cls += "(D16)"
      else if (txt.contains("NullPointerException") && txt.contains("TTrack")) cls += "(F5?)"
    }
    Res(mode, seed, cfg, knobs, exit, cls, fam.toMap, kv.toMap, wall, log, first.toSeq)
  }

  def resultLine(r: Res): String = {
    val k = r.kv
    val fam = if (r.families.isEmpty) "-" else r.families.toSeq.sorted.map { case (a, b) => s"$a:$b" }.mkString(",")
    s"RESULT mode=${r.mode} seed=${r.seed} cfg=${r.cfg}${if (r.knobs.nonEmpty) " knobs=" + r.knobs.map { case (a, b) => s"$a=$b" }.mkString(",") else ""}" +
      s" class=${r.cls} exit=${r.exit} wallMs=${r.wallMs} workMs=${k.getOrElse("workMs", "?")} props=${k.getOrElse("props", "?")}" +
      s" ${if (r.mode == "h2") s"nodes=${k.getOrElse("nodes", "?")} rounds=${k.getOrElse("rounds", "?")}" else s"trackers=${k.getOrElse("trackers", "?")} phases=${k.getOrElse("phases", "?")}"}" +
      s" clients=${k.getOrElse("clients", "?")} families=[$fam] h5=${k.getOrElse("h5", "?")} log=${java.nio.file.Path.of(r.log).getFileName}"
  }

  def par: Int = sys.env.getOrElse("HUNTER_PAR", "8").toInt

  def sweep(mode: String, first: Long, count: Int): Unit = {
    val pool = Executors.newFixedThreadPool(par)
    def submit[A](f: => A) = pool.submit(() => f)
    val t0 = System.nanoTime()
    val seeds = first until first + count
    val all = mutable.ArrayBuffer[Res]()
    val base = seeds.map(s => s -> submit(runChild(mode, s, baseline(mode), Nil, 30000, ""))).toMap
    val wd = mutable.Map[Long, Long]()
    for (s <- seeds) {
      val r = base(s).get()
      all += r
      println(resultLine(r)); System.out.flush()
      val mx = r.kv.get(if (mode == "h2") "maxRoundMs" else "maxPhaseMs").map(_.toLong).getOrElse(1000L)
      wd(s) = math.max(10000L, 10 * mx)
    }
    val fs = for (s <- seeds; c <- cfgs(mode) if c != baseline(mode)) yield submit(runChild(mode, s, c, Nil, wd(s), ""))
    for (f <- fs) { val r = f.get(); all += r; println(resultLine(r)); System.out.flush() }
    println(s"==== SUMMARY mode=$mode runs=${all.size} sweepMs=${(System.nanoTime() - t0) / 1000000}")
    for ((c, rs) <- all.groupBy(_.cfg).toSeq.sortBy(_._1))
      println(s"SUMMARY cfg=$c pass=${rs.count(!_.failed)} fail=${rs.count(_.failed)} classes=${rs.filter(_.failed).groupBy(_.cls).map { case (a, b) => s"$a:${b.size}" }.mkString(",")}")
    val byFam = mutable.LinkedHashMap[String, mutable.ArrayBuffer[Res]]()
    for (r <- all if r.failed; f <- r.famKeys) byFam.getOrElseUpdate(f, mutable.ArrayBuffer()) += r
    for ((f, rs) <- byFam.toSeq.sortBy(-_._2.size))
      println(s"SUMMARY family=$f runs=${rs.size} seeds=${rs.map(_.seed).distinct.size} cfgs=${rs.groupBy(_.cfg).map { case (a, b) => s"$a:${b.size}" }.toSeq.sorted.mkString(",")}")
    coverage(all.toSeq)
    // reruns: up to 3 representatives per family (smallest DAG first), 5x each
    val reps = byFam.toSeq.map { case (f, rs) => f -> rs.sortBy(r => r.kv.getOrElse("props", "999").toInt).take(3) }
    val reruns = for ((f, rs) <- reps; r <- rs.distinct) yield (f, r, submit((0 until 5).map(i => runChild(mode, r.seed, r.cfg, Nil, wd(r.seed), s"rerun$i"))))
    val rerunOk = mutable.Map[String, Res]()
    for ((f, r, fu) <- reruns) {
      val xs = fu.get()
      val n = xs.count(_.famKeys.contains(f))
      println(s"RERUN family=$f seed=${r.seed} cfg=${r.cfg} reproduced=$n/5 classes=${xs.map(_.cls).mkString(";")}")
      if (n > 0 && !rerunOk.contains(f)) rerunOk(f) = r
    }
    // minimise + classify one representative per family (those that reproduced), capped
    val maxMin = sys.env.getOrElse("HUNTER_MAXMIN", "4").toInt
    val todo = rerunOk.toSeq.sortBy { case (f, _) => if (f.contains("UNEXPLAINED") || !f.startsWith("ORACLE")) 0 else 1 }.take(maxMin)
    val mins = todo.map { case (f, r) => submit(minimiseAndClassify(f, r, wd(r.seed))) }
    mins.foreach(f => println(f.get()))
    println(s"==== DONE mode=$mode totalMs=${(System.nanoTime() - t0) / 1000000} childRuns=${runCounter.get}")
    pool.shutdown()
  }

  def coverage(all: Seq[Res]): Unit = {
    val sums = mutable.TreeMap[String, Long]()
    for (r <- all; (kk, v) <- r.kv if kk.contains('.') || kk.startsWith("depth") || Set("req", "nodes", "phases", "checkpoints", "disposed").contains(kk))
      scala.util.Try(v.toLong).foreach(x => sums(kk) = sums.getOrElse(kk, 0L) + x)
    val props = all.flatMap(_.kv.get("props")).map(_.toLong)
    println(s"COVERAGE runs=${all.size} propsTotal=${props.sum} propsMax=${if (props.isEmpty) 0 else props.max} " + sums.map { case (a, b) => s"$a=$b" }.mkString(" "))
  }

  /** Reproduces with any of 3 runs showing the family. */
  /** Number of runs (out of n, run in parallel) that show the family. */
  def hits(f: String, r: Res, knobs: Seq[(String, String)], wdMs: Long, tag: String, cfg: String = null, n: Int = 4): Int = {
    val ts = (0 until n).map(i => new java.util.concurrent.FutureTask[Boolean](() =>
      runChild(r.mode, r.seed, Option(cfg).getOrElse(r.cfg), knobs, wdMs, s"$tag$i").famKeys.contains(f)))
    ts.foreach(t => new Thread(t).start())
    ts.count(_.get())
  }
  /** A reduction is kept when at least 2 of 4 runs still show the family (flaky families otherwise drift). */
  def persists(f: String, r: Res, knobs: Seq[(String, String)], wdMs: Long, tag: String, cfg: String = null): Boolean =
    hits(f, r, knobs, wdMs, tag, cfg) >= 2

  def minimiseAndClassify(f: String, r: Res, wdMs: Long): String = {
    var knobs = Seq.empty[(String, String)]
    def kn(drop: Set[Int], maxProps: Int, extra: Seq[(String, String)] = Nil): Seq[(String, String)] =
      (if (drop.nonEmpty) Seq("drop" -> drop.toSeq.sorted.mkString(",")) else Nil) ++
        (if (maxProps > 0) Seq("maxProps" -> maxProps.toString) else Nil) ++ extra
    val spec = GSpec.generate(r.seed, r.mode == "h3")
    var drop = Set.empty[Int]
    val ids = if (r.mode == "h2") spec.nodes.map(_.id).filter(_ > 0).reverse else spec.trackers.map(_.id).filter(_ > 0).reverse
    val ft = f.replaceAll("[^A-Za-z0-9]", "")
    for (i <- ids) if (persists(f, r, kn(drop + i, 0), wdMs, s"min-$ft-d$i-")) drop += i
    var n = spec.props.size
    var lo = 1
    while (n - lo > 1) {
      val mid = (n + lo) / 2
      if (persists(f, r, kn(drop, mid), wdMs, s"min-$ft-p$mid-")) n = mid else lo = mid
    }
    knobs = kn(drop, if (n < spec.props.size) n else 0)
    val baseHits = hits(f, r, knobs, wdMs, s"cls-$ft-base-", null, 6)
    // classification by feature toggles
    val toggles =
      if (r.mode == "h2") Seq("tags", "modes", "byname", "when", "si", "xs", "xsft", "alias", "pin", "nest", "unordered", "twice", "cancel")
      else Seq("tags", "modes", "byname", "when", "si", "xs", "xsft", "alias", "addsync", "addasync", "batch", "evalsync", "evalasync", "lp", "newchild", "tagupd", "canceleval", "clientasync")
    val tHits = toggles.map(t => t -> hits(f, r, knobs :+ ("off" -> t), wdMs, s"cls-$ft-$t-", null, 6))
    val removedBy = if (baseHits < 3) Nil else tHits.filter(_._2 == 0).map(_._1)
    val cfgVariants = Seq(r.cfg.replaceAll("d1", "d0"), r.cfg.replaceAll("^i\\d", "i1"), r.cfg.replaceAll("a1", "a0")).distinct.filter(_ != r.cfg)
    val cHits = cfgVariants.map(c => c -> hits(f, r, knobs, wdMs, s"cls-$ft-$c-", c, 6))
    val cfgRemoved = if (baseHits < 3) Nil else cHits.filter(_._2 == 0).map(_._1)
    val known = removedBy.collect {
      case "tags" => "F8"; case "modes" => "F1"; case "si" => "D4"; case "xs" => "XS(D1?)"; case "xsft" => "XSFT(D7/D16/D8?)"
      case "alias" | "pin" => "ALIAS(new?)"; case "cancel" => "CANCEL"
    }.distinct
    val m = new Mat(spec, Knobs.fromMap(knobs))
    val dump = logDir.resolve(s"MIN-${r.mode}-s${r.seed}-${ft}.spec.txt")
    java.nio.file.Files.writeString(dump, m.describe(r.mode == "h3"))
    s"MIN family=$f seed=${r.seed} cfg=${r.cfg} knobs=${knobs.map { case (a, b) => s"$a=$b" }.mkString(" ")} baseHits=$baseHits/6 toggleHits=[${tHits.map { case (a, b) => s"$a:$b" }.mkString(",")}] cfgHits=[${cHits.map { case (a, b) => s"$a:$b" }.mkString(",")}] removedBy=[${removedBy.mkString(",")}]" +
      s" cfgRemovedBy=[${cfgRemoved.mkString(",")}] classification=[${if (known.isEmpty) "UNCLASSIFIED" else known.mkString(",")}]" +
      s" spec=${dump.getFileName}\n  repro: probes/cache/run-cache-hunter.sh one ${r.mode} ${r.seed} ${r.cfg} 5 ${knobs.map { case (a, b) => s"$a=$b" }.mkString(" ")}"
  }

  def one(mode: String, seed: Long, cfg: String, reps: Int, knobs: Seq[(String, String)]): Unit = {
    val wd = java.lang.Long.getLong("hunter.wdMs", 15000L)
    val pool = Executors.newFixedThreadPool(math.min(par, reps))
    val fs = (0 until reps).map(i => pool.submit(() => runChild(mode, seed, cfg, knobs, wd, s"one$i")))
    var fails = 0
    for (f <- fs) { val r = f.get(); if (r.failed) fails += 1; println(resultLine(r)); r.firstFindings.foreach(l => println("  " + l)) }
    println(s"ONE mode=$mode seed=$seed cfg=$cfg knobs=${knobs.map { case (a, b) => s"$a=$b" }.mkString(",")} failed=$fails/$reps")
    pool.shutdown()
  }

  /** Memoised oracle vs ProbeDag.SequentialEvaluator on small generated specs: values, ISD and transitive reads. */
  def selftest(count: Int): Unit = {
    var checked = 0; var bad = 0
    for (seed <- 1L to count.toLong) {
      val spec = GSpec.generate(seed, h3 = false)
      val m = new Mat(spec, Knobs.fromMap(Seq("maxProps" -> "40")))
      for (g <- spec.nodes if m.keptNode(g.id)) {
        val levels = m.nodeLevels(g.id)
        val st = new Stack(levels.toSeq.asJava, 0)
        val ho = new HOracle(m, levels, 0, withReads = true)
        for (i <- 0 until math.min(m.nKeys, 60)) {
          val key = m.keyOf(i * 7 % m.nKeys)
          val se = new SequentialEvaluator(st)
          val (v, isd) = try (se.eval(key), false) catch { case _: OracleISD => (0L, true) }
          val r = ho.eval(key)
          val rs = se.reads.get(key.toString + "@" + levels.length)
          val mine = { val b = mutable.Set[Key](); var j = r.reads.nextSetBit(0); while (j >= 0) { b += m.keyOf(j); j = r.reads.nextSetBit(j + 1) }; b.toSet }
          checked += 1
          if (r.isd != isd || (!isd && r.v != v) || (rs != null && rs.asScala.toSet != mine)) {
            bad += 1
            if (bad < 10) println(s"SELFTEST MISMATCH seed=$seed node=${g.id} key=$key seq=${if (isd) "ISD" else v} memo=${if (r.isd) "ISD" else r.v} readsSeq=${Option(rs).map(_.asScala.toSet)} readsMemo=$mine")
          }
        }
      }
    }
    println(s"SELFTEST checked=$checked mismatches=$bad")
  }

  /** Hand-written check of how a tracker's own property / instance tweaks are tracked (TTrack roots) and invalidated. */
  def ttprobe(): Unit = {
    CacheFixtures.init()
    val dag = new ProbeDag
    val a = dag.prop("a").base(1)
    val c = dag.prop("c").notTweakable().reads(a)
    val sys = new ProbeSystem(dag)
    for ((label, mk) <- Seq[(String, Long => Tw)](("property", (v: Long) => Tw.property(a, v)), ("instance", (v: Long) => Tw.instance(ProbeDag.key(a), v)))) {
      val root = T.newRoot()
      val ch = T.consistentChild(root, "ch")
      T.addTweaks(ch, sys.tweak(mk(5)))
      println(s"$label: c@ch = ${T.evaluate(ch, sys.newKey(c))}")
      val n = sys.pp(c).lastRun.get(ProbeDag.key(c))
      for ((nm, t) <- Seq("root" -> root, "ch" -> ch)) T.ttracks(t).forEach((k, r) => println(s"  $nm ttrack key=${k.getClass.getSimpleName} ${k.propertyInfo.name} -> reach(c node)=${CacheHunterTT.reach(r.asInstanceOf[AnyRef]).contains(n)}"))
      T.addTweaks(ch, sys.tweak(mk(7)))
      println(s"  after a:=7 in ch: c node invalid=${n.isInvalidCache} value now=${T.evaluate(ch, sys.newKey(c))}")
      T.addTweaks(root, sys.tweak(mk(9)))
      println(s"  after a:=9 in root: value in ch=${T.evaluate(ch, sys.newKey(c))} root=${T.evaluate(root, sys.newKey(c))}")
      T.disposeAllRoots()
    }
    // plugin tag set twice on a tracker
    {
      val tg = dag.prop("tg").notTweakable().kind(Kind.TAG_READER)
      val sys3 = new ProbeSystem(dag)
      val root = T.newRoot()
      root.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(3))
      val v1 = scala.util.Try(T.evaluate(root, sys3.newKey(tg)))
      root.setPluginTag(ProbeTagKey, java.lang.Long.valueOf(4))
      val raw = scala.util.Try(T.executeEvaluate(root)(CacheFixtures.evalIn(T.evaluationStack(root))(T.rawStack(root).findPluginTag(ProbeTagKey.asInstanceOf[PluginTagKey[AnyRef]]).map(x => s"${x.getClass.getSimpleName}($x)"))))
      val ch = T.consistentChild(root, "tagch")
      val v2 = scala.util.Try(T.evaluate(ch, sys3.newKey(tg)))
      println(s"TAG-TWICE first=$v1 findPluginTag after second set=$raw tagReader in fresh child=$v2")
      T.disposeAllRoots()
    }
    // by-name tweak in a child whose RHS reads a tweakable tweaked in the parent
    val b = dag.prop("b").base(100)
    val d = dag.prop("d").notTweakable().reads(b)
    val sys2 = new ProbeSystem(dag)
    for ((label, pa) <- Seq(("property a", true), ("instance a", false)); mode <- Seq(Mode.BYNAME, Mode.BIND, Mode.BIND_ONCE)) {
      def ta(v: Long) = if (pa) Tw.property(a, v) else Tw.instance(ProbeDag.key(a), v)
      val rhs = new Rhs(java.util.List.of(new Ref(a, SAME, SAME)), (_: Key, xs: Array[Long]) => xs.sum * 10)
      val root = T.newRoot()
      val ch = T.consistentChild(root, "ch")
      T.addTweaks(root, sys2.tweak(ta(5)))
      T.addTweaks(ch, sys2.tweak(Tw.property(b, rhs, mode)))
      val v0 = T.evaluate(ch, sys2.newKey(d))
      val n = sys2.pp(d).lastRun.get(ProbeDag.key(d))
      val roots = Seq("root" -> root, "ch" -> ch).flatMap { case (nm, t) => T.ttracks(t).asScala.toSeq.map { case (k, r) => s"$nm:${k.propertyInfo.name}:${CacheHunterTT.reach(r.asInstanceOf[AnyRef]).contains(n)}" } }
      T.addTweaks(root, sys2.tweak(ta(6)))
      val inv = n.isInvalidCache
      val v1 = T.evaluate(ch, sys2.newKey(d))
      println(s"$label b:=$mode(a*10): d@ch=$v0 roots=[${roots.mkString(" ")}] after a:=6 in root: invalid=$inv d@ch=$v1 (expect 60)")
      T.disposeAllRoots()
    }
  }

  /** Request a running node from an uncancelled scope while its owner's scope is cancelled (CANCEL_LEAK hypothesis). */
  def cancelprobe(): Unit = {
    CacheFixtures.init()
    for (policy <- Seq("basic", "xsft"); target <- Seq("s", "t"); order <- Seq("requestThenCancel", "cancelThenRequest"); childRead <- Seq(false, true)) {
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val started = new java.util.concurrent.CountDownLatch(1)
      val release = new java.util.concurrent.CountDownLatch(1)
      val b = dag.prop("b").notTweakable().reads(a)
      val sl = dag.prop("s").notTweakable().reads(a).combine { (_, xs) => started.countDown(); release.await(5, TimeUnit.SECONDS); xs.sum }
      if (childRead) { sl.reads.clear(); sl.reads(a) } // same shape; childRead variant reads b after the wait below
      val tt = dag.prop("t").notTweakable().reads(sl)
      val sys = new ProbeSystem(dag)
      if (childRead) sl.combine { (_, xs) => started.countDown(); release.await(5, TimeUnit.SECONDS); xs.sum + sys.newKey(b).lookupAndGet.longValue }
      if (policy == "basic") for (p <- Seq(b, sl, tt)) CacheFixtures.setPolicy(sys.info(p), NCPolicy.Basic)
      val root = CacheFixtures.newRoot()
      val ssX = root.withChildCancellationScope()
      val r1 = new java.util.concurrent.FutureTask[String](() => scala.util.Try(sys.eval(ssX, ProbeDag.key(tt))).toString)
      HUtil.thread("r1") { CacheFixtures.init(); r1.run() }.start()
      started.await(5, TimeUnit.SECONDS)
      val key = if (target == "s") ProbeDag.key(sl) else ProbeDag.key(tt)
      val r2 = new java.util.concurrent.FutureTask[String](() => scala.util.Try(sys.eval(root, key)).map(_.toString).recover { case e => HUtil.brief(e) }.get)
      if (order == "cancelThenRequest") ssX.cancelScope.cancel("hunter")
      HUtil.thread("r2") { CacheFixtures.init(); r2.run() }.start()
      Thread.sleep(50)
      if (order == "requestThenCancel") ssX.cancelScope.cancel("hunter")
      Thread.sleep(20)
      release.countDown()
      val res2 = scala.util.Try(r2.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
      val res1 = scala.util.Try(r1.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
      println(f"CANCELPROBE policy=$policy%-5s target=$target order=$order%-17s r1(cancelled scope)=${res1.take(60)}%-60s r2(uncancelled)=$res2 s.runs=${sys.pp(sl).runs.get}")
    }
  }

  /** CANCEL_LEAK hypothesis: a by-name TweakNode completed with cancellation under scope X is reused from another scope. */
  def tweakcancelprobe(): Unit = {
    CacheFixtures.init()
    for (mode <- Seq(Mode.BYNAME, Mode.BIND, Mode.BIND_ONCE); target <- Seq("leaf", "comb"); inst <- Seq(false, true); slowRhs <- Seq(false, true)) {
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val g = dag.prop("g").base(5)
      val c = dag.prop("c").notTweakable().reads(a)
      val sys = new ProbeSystem(dag)
      val started = new java.util.concurrent.CountDownLatch(1)
      val release = new java.util.concurrent.CountDownLatch(1)
      val rhs = new Rhs(java.util.List.of(new Ref(g, SAME, SAME)), (_: Key, xs: Array[Long]) => {
        if (slowRhs) { started.countDown(); release.await(5, TimeUnit.SECONDS) }
        xs.sum * 10
      })
      val tw = if (inst) Tw.instance(ProbeDag.key(a), rhs, mode) else Tw.property(a, rhs, mode)
      val root = CacheFixtures.newRoot()
      val ss = sys.stack(root, Stack.of(Level.of(tw)))
      val ssX = ss.withChildCancellationScope()
      val key = if (target == "leaf") ProbeDag.key(a) else ProbeDag.key(c)
      val r1 = new java.util.concurrent.FutureTask[String](() => scala.util.Try(sys.eval(ssX, key)).map(_.toString).recover { case e => HUtil.brief(e).take(40) }.get)
      if (!slowRhs) ssX.cancelScope.cancel("hunter")
      HUtil.thread("r1") { CacheFixtures.init(); r1.run() }.start()
      if (slowRhs) { started.await(5, TimeUnit.SECONDS); ssX.cancelScope.cancel("hunter"); release.countDown() }
      val res1 = scala.util.Try(r1.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
      val res2 = scala.util.Try(sys.eval(ss, key)).map(_.toString).recover { case e => "LEAK " + HUtil.brief(e).take(60) }.get
      println(f"TWEAKCANCEL mode=$mode%-9s target=$target%-4s inst=$inst%-5s slowRhs=$slowRhs%-5s r1(cancelled)=$res1%-45s r2(uncancelled)=$res2 rhsRuns=${sys.rhsRuns.get}")
    }
  }

  /** CANCEL_LEAK: R1 (scope X) and R2 (no scope) share a by-name tweak whose RHS reads a slow node; X is cancelled mid-RHS. */
  def tweakcancelprobe2(): Unit = {
    CacheFixtures.init()
    for (policy <- Seq("basic", "xsft"); mode <- Seq(Mode.BYNAME, Mode.BIND_ONCE); r2Target <- Seq("a", "c"); twice <- Seq(false, true)) {
      val dag = new ProbeDag
      val a = dag.prop("a").base(1)
      val b = dag.prop("b").base(7)
      val started = new java.util.concurrent.CountDownLatch(1)
      val release = new java.util.concurrent.CountDownLatch(1)
      var sysRef: ProbeSystem = null
      val sl = dag.prop("sl").notTweakable().combine { (_, _) =>
        started.countDown(); release.await(5, TimeUnit.SECONDS); sysRef.newKey(ProbeDag.key(b)).lookupAndGet.longValue
      }
      val c = dag.prop("c").notTweakable().reads(a)
      val sys = new ProbeSystem(dag); sysRef = sys
      if (policy == "basic") for (p <- Seq(a, b, sl, c)) CacheFixtures.setPolicy(sys.info(p), NCPolicy.Basic)
      val rhs = new Rhs(java.util.List.of(new Ref(sl, SAME, SAME)), (_: Key, xs: Array[Long]) => xs.sum * 10)
      val st = Stack.of(Level.of(Tw.property(a, rhs, mode)))
      val root = CacheFixtures.newRoot()
      val ss1 = sys.stack(root, st)
      val ss2 = if (twice) sys.stack(root, st) else ss1
      val ssX = ss1.withChildCancellationScope()
      val r1 = new java.util.concurrent.FutureTask[String](() => scala.util.Try(sys.eval(ssX, ProbeDag.key(c))).map(_.toString).recover { case e => HUtil.brief(e).take(40) }.get)
      HUtil.thread("r1") { CacheFixtures.init(); r1.run() }.start()
      started.await(5, TimeUnit.SECONDS)
      val k2 = if (r2Target == "a") ProbeDag.key(a) else ProbeDag.key(c)
      val r2 = new java.util.concurrent.FutureTask[String](() => scala.util.Try(sys.eval(ss2, k2)).map(_.toString).recover { case e => "LEAK " + HUtil.brief(e).take(50) }.get)
      HUtil.thread("r2") { CacheFixtures.init(); r2.run() }.start()
      Thread.sleep(30)
      ssX.cancelScope.cancel("hunter")
      Thread.sleep(10)
      release.countDown()
      val res1 = scala.util.Try(r1.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
      val res2 = scala.util.Try(r2.get(10, TimeUnit.SECONDS)).getOrElse("TIMEOUT")
      val res3 = scala.util.Try(sys.eval(ss2, k2)).map(_.toString).recover { case e => "LEAK " + HUtil.brief(e).take(50) }.get
      println(f"TWEAKCANCEL2 policy=$policy%-5s mode=$mode%-9s r2=$r2Target twice=$twice%-5s r1(X)=$res1%-42s r2(none, concurrent)=$res2%-40s r3(none, later)=$res3 sl.runs=${sys.pp(sl).runs.get}")
    }
  }

  def parseKnobs(xs: Seq[String]): Seq[(String, String)] = xs.map { s => val i = s.indexOf('='); (s.substring(0, i), s.substring(i + 1)) }

  def main(args: Array[String]): Unit = args.toSeq match {
    case Seq("--child", mode, seed, cfg) =>
      var code = 4
      val t = HUtil.thread("hunter-main") {
        try code = if (mode == "h2") H2Child.run(seed.toLong, cfg) else H3Child.run(seed.toLong, cfg)
        catch { case e: Throwable => println(s"CHILD-CRASH $e"); e.printStackTrace(System.out); code = 5 }
      }
      t.start(); t.join()
      System.out.flush()
      Runtime.getRuntime.halt(code)
    case Seq("sweep", mode, first, count) => sweep(mode, first.toLong, count.toInt); System.exit(0)
    case Seq("one", mode, seed, cfg, rest @ _*) =>
      val (reps, kn) = rest.headOption.filter(_.forall(_.isDigit)).map(r => (r.toInt, rest.tail)).getOrElse((1, rest))
      one(mode, seed.toLong, cfg, reps, parseKnobs(kn)); System.exit(0)
    case Seq("spec", mode, seed, rest @ _*) =>
      println(new Mat(GSpec.generate(seed.toLong, mode == "h3"), Knobs.fromMap(parseKnobs(rest))).describe(mode == "h3")); System.exit(0)
    case Seq("min", mode, seed, cfg, family) =>
      val r = runChild(mode, seed.toLong, cfg, Nil, 15000, "minbase")
      println(resultLine(r)); println(minimiseAndClassify(family, r, 15000)); System.exit(0)
    case Seq("ttprobe") => ttprobe(); System.exit(0)
    case Seq("cancelprobe") => cancelprobe(); System.exit(0)
    case Seq("tweakcancelprobe") => tweakcancelprobe(); System.exit(0)
    case Seq("tweakcancelprobe2") => tweakcancelprobe2(); System.exit(0)
    case Seq("selftest", rest @ _*) => selftest(rest.headOption.map(_.toInt).getOrElse(40)); System.exit(0)
    case _ =>
      System.err.println("usage: sweep h2|h3 first count | one h2|h3 seed cfg [reps] [k=v..] | spec h2|h3 seed [k=v..] | min h2|h3 seed cfg family | selftest [n]")
      System.exit(2)
  }
}
}

package optimus.graph.tracking.ttracks {
  object CacheHunterTT {
    /** Every node in a TTrackRef list of a track reachable from `root` through ptracks (callers). */
    def reach(root: AnyRef): java.util.Set[optimus.graph.NodeTask] = {
      val out = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[optimus.graph.NodeTask, java.lang.Boolean]())
      val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[TTrack, java.lang.Boolean]())
      val st = new java.util.ArrayDeque[TTrack]()
      val r = root.asInstanceOf[TTrack]
      st.push(r); seen.add(r)
      while (!st.isEmpty) {
        val t = st.pop()
        var ref = t.nodes
        while (ref != null && (ref ne TTrackRef.Nil) && (ref ne TTrackRef.Invalid)) {
          val n = ref.get
          if (n != null) out.add(n)
          ref = ref.nref
        }
        val p = t.ptracks
        if (p != null) {
          val sz = p.currentSize
          var i = 0
          while (i < sz) { val c = p.at(i); if (c != null && seen.add(c)) st.push(c); i += 1 }
        }
      }
      out
    }
  }
}
