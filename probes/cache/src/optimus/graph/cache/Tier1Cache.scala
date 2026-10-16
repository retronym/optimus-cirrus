package optimus.graph.cache

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import optimus.graph._
import optimus.graph.ProbeDag._
import optimus.platform._

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Cache/XS-side Tier 1 probes of the cache campaign (CACHE_CAMPAIGN.md): T1 (D1), T2 (D4), T3 (D5), T13 (R11),
 * T15 (F8), T16 (P1), T19 (W3, W4). Every probe runs in child JVMs (`--child <id> <variant>`), started from
 * CACHE_JAVA_CMD with per-variant -D flags; the child prints `V <outcome> <detail>` per variant and the parent
 * folds the variants into one RESULT line, appended to $TIER1_CACHE_RESULTS. Watchdog: 10 s of no progress in a
 * child dumps all threads (with findDeadlockedThreads) and halts.
 *
 * Run: probes/cache/run-tier1-cache.sh [T1 T2 ...]
 */
object Tier1Cache {
  // ---------------------------------------------------------------------------------------------------------------
  // child-side helpers
  // ---------------------------------------------------------------------------------------------------------------
  val progress = new AtomicLong(System.nanoTime())
  def tick(): Unit = progress.set(System.nanoTime())
  @volatile var watchdogExtra: () => String = () => ""

  def kv(kvs: Any*): Unit = {
    val sb = new StringBuilder("KV")
    kvs.grouped(2).foreach { case Seq(k, v) => sb.append(' ').append(k).append('=').append(v); case _ => }
    println(sb); System.out.flush()
  }
  def v(outcome: String, detail: String): Unit = { println(s"V $outcome $detail"); System.out.flush() }

  def dumpThreads(prefixes: String*): Unit = {
    val mx = ManagementFactory.getThreadMXBean
    val dl = mx.findDeadlockedThreads()
    println("---- findDeadlockedThreads: " + (if (dl == null) "none" else s"${dl.length} threads ${dl.mkString(",")}"))
    for (ti <- mx.dumpAllThreads(true, true)) {
      val n = ti.getThreadName
      val want = (dl != null && dl.contains(ti.getThreadId)) || prefixes.exists(n.startsWith) || n == "main"
      if (want) {
        println(s"""--- "$n" id=${ti.getThreadId} ${ti.getThreadState}""" +
          (if (ti.getLockName != null) s""" waiting on ${ti.getLockName} owned by "${ti.getLockOwnerName}"""" else ""))
        val st = ti.getStackTrace
        for (i <- 0 until math.min(45, st.length)) {
          println("    at " + st(i))
          for (m <- ti.getLockedMonitors if m.getLockedStackDepth == i) println("      - locked " + m)
        }
        for (s <- ti.getLockedSynchronizers) println("    holds synchronizer " + s)
      }
    }
    System.out.flush()
  }

  /** 10 s without tick() -> dump and halt(3). */
  def startWatchdog(ms: Long = 10000, prefixes: Seq[String] = Seq("og.", "t13", "t19", "t3-")): Unit = {
    val t = new Thread(() => {
      while (true) {
        Thread.sleep(200)
        if (System.nanoTime() - progress.get() > ms * 1000000L) {
          println(s"CHILD-HANG no progress for $ms ms ${watchdogExtra()}")
          val dl = ManagementFactory.getThreadMXBean.findDeadlockedThreads()
          kv("deadlocked", if (dl == null) 0 else dl.length)
          dumpThreads(prefixes: _*)
          Runtime.getRuntime.halt(3)
        }
      }
    }, "tier1cache-watchdog")
    t.setDaemon(true); t.start()
  }

  def exClass(t: Throwable): String = if (t == null) "null" else t.getClass.getSimpleName
  def rootCause(t: Throwable): Throwable = { var c = t; while (c.getCause != null && (c.getCause ne c)) c = c.getCause; c }
  def tryEval[A](f: => A): Either[Throwable, A] = try Right(f) catch { case t: Throwable => Left(t) }
  def show(e: Either[Throwable, _]): String = e match {
    case Right(x) => x.toString
    case Left(t)  => s"EX(${exClass(t)}${if (rootCause(t) ne t) "<-" + exClass(rootCause(t)) else ""})"
  }

  // ---------------------------------------------------------------------------------------------------------------
  // parent side
  // ---------------------------------------------------------------------------------------------------------------
  final case class Child(exit: Int, lines: Seq[String], timedOut: Boolean, wallMs: Long) {
    def vlines: Seq[String] = lines.filter(_.startsWith("V "))
    def kv(key: String): String = lines.reverseIterator.filter(_.startsWith("KV ")).flatMap { l =>
      l.split(' ').find(_.startsWith(key + "=")).map(_.substring(key.length + 1))
    }.nextOption().orNull
    def grep(max: Int, needles: String*): Seq[String] = lines.filter(l => needles.exists(l.contains)).take(max)
  }

  def runChild(id: String, variant: String, flags: String, timeoutSec: Int = 120): Child = {
    val cmd = System.getenv("CACHE_JAVA_CMD")
    if (cmd == null) throw new IllegalStateException("CACHE_JAVA_CMD not set; use run-tier1-cache.sh")
    val full = "java " + flags + " " + cmd.substring(5) + s" optimus.graph.cache.Tier1Cache --child $id $variant"
    val t0 = System.nanoTime()
    val p = new ProcessBuilder("sh", "-c", full).redirectErrorStream(true).start()
    val lines = java.util.Collections.synchronizedList(new java.util.ArrayList[String]())
    val rd = new Thread(() => {
      val r = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream))
      var l = r.readLine()
      while (l != null) { lines.add(l); l = r.readLine() }
    })
    rd.start()
    val done = p.waitFor(timeoutSec, TimeUnit.SECONDS)
    if (!done) p.destroyForcibly().waitFor()
    rd.join(5000)
    val all = lines.asScala.toSeq
    val logDir = System.getenv("TIER1_CACHE_LOGDIR")
    if (logDir != null)
      java.nio.file.Files.write(java.nio.file.Path.of(logDir, s"$id-$variant-${flags.replaceAll("[^A-Za-z0-9=.]+", "_")}.log"), all.asJava)
    Child(if (done) p.exitValue else -1, all, !done, (System.nanoTime() - t0) / 1000000)
  }

  /** Run variants; fold the V lines into one RESULT. outcome precedence: DEMONSTRATED > ERROR > QUANTIFIED > NOT_REPRODUCED. */
  def runVariants(id: String, variants: Seq[(String, String)], timeoutSec: Int = 120, hangIsDemo: Boolean = false, childId: String = null): String = {
    val sb = new StringBuilder
    var outcomes = List.empty[String]
    for ((variant, flags) <- variants) {
      val ch = runChild(if (childId == null) id else childId, variant, flags, timeoutSec)
      val hang = ch.timedOut || ch.exit == 3
      sb.append(s"\n  [$variant${if (flags.nonEmpty) " " + flags else ""}] exit=${ch.exit} wallMs=${ch.wallMs}")
      if (ch.vlines.isEmpty && !hang) {
        outcomes ::= "ERROR"
        sb.append(" no V line; tail: ").append(ch.lines.takeRight(25).map(_.trim).mkString(" | "))
      }
      for (l <- ch.vlines) {
        val rest = l.substring(2)
        val o = rest.takeWhile(_ != ' ')
        outcomes ::= o
        sb.append("\n    ").append(rest)
      }
      if (hang) {
        val dl = ch.kv("deadlocked")
        outcomes ::= (if (hangIsDemo && dl != null && dl != "0") "DEMONSTRATED" else "ERROR")
        sb.append(s" HANG deadlocked=$dl")
        for (l <- ch.grep(60, "CHILD-HANG", "findDeadlockedThreads", "--- \"", "_visitLock", "OGScheduler.enqueue(", "detectCycles", "runOnStalledCallbacks",
               "fixWaiterChain", "breakProxyChains", "offerValue", "forAllWaiters", "- locked", "holds synchronizer", "waiting on"))
          sb.append("\n    | ").append(l.trim)
      }
    }
    val o =
      if (outcomes.contains("DEMONSTRATED")) "DEMONSTRATED"
      else if (outcomes.contains("ERROR")) "ERROR"
      else if (outcomes.contains("QUANTIFIED")) "QUANTIFIED"
      else "NOT_REPRODUCED"
    s"RESULT $id $o${sb.toString}"
  }

  val NOXSFT = "-Doptimus.profile.enableXSFT=false"

  val probes: Seq[(String, () => String)] = Seq(
    "T1" -> (() => runVariants("T1", Seq("cancel" -> "", "control" -> "", "cancel" -> NOXSFT, "control" -> NOXSFT))),
    "T2" -> (() => runVariants("T2", Seq("main" -> "", "main" -> NOXSFT))),
    "T3" -> (() => runVariants("T3", Seq("main" -> NOXSFT, "main" -> "", "quiet" -> NOXSFT, "control" -> NOXSFT))),
    "T13" -> (() => runVariants("T13", Seq("main" -> "-Doptimus.graph.keepTaskSchedulerAffinity=true", "nodelay" -> "-Doptimus.graph.keepTaskSchedulerAffinity=true", "noaffinity" -> ""), 60, hangIsDemo = true)),
    // repeat of T13 without the 300 ms widening hook: how often does the natural window deadlock?
    "T13-repeat" -> (() => runVariants("T13-repeat", Seq.fill(Integer.getInteger("t13.repeat", 10))("nodelay" -> "-Doptimus.graph.keepTaskSchedulerAffinity=true"), 60, hangIsDemo = true, childId = "T13")),
    "T15" -> (() => runVariants("T15", Seq("main" -> "", "main" -> NOXSFT))),
    "T16" -> (() => runVariants("T16", Seq("main" -> ""))),
    "T19" -> (() => runVariants("T19", Seq("main" -> "", "main" -> NOXSFT), 600))
  )

  def main(args: Array[String]): Unit = {
    if (args.length >= 3 && args(0) == "--child") { child(args(1), args(2)); return }
    val which = if (args.isEmpty) probes.map(_._1).filterNot(_ == "T13-repeat") else args.toSeq
    val out = Option(System.getenv("TIER1_CACHE_RESULTS"))
    out.foreach { f =>
      val hdr = s"# run-tier1-cache.sh ${which.mkString(" ")}  (JDK ${System.getProperty("java.version")}), ${new java.util.Date}\n"
      java.nio.file.Files.writeString(java.nio.file.Path.of(f), hdr, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)
    }
    for (id <- which) {
      val r = probes.find(_._1 == id).map(_._2()).getOrElse(s"RESULT $id ERROR unknown probe")
      println(r)
      out.foreach(f => java.nio.file.Files.writeString(java.nio.file.Path.of(f), r + "\n", java.nio.file.StandardOpenOption.APPEND))
    }
  }

  def child(id: String, variant: String): Unit = {
    startWatchdog()
    CacheFixtures.init()
    try {
      id match {
        case "T1"  => T1cProbe1.run(variant)
        case "T2"  => T1cProbe2.run(variant)
        case "T3"  => T1cProbe3.run(variant)
        case "T13" => T1cProbe13.run(variant)
        case "T15" => T1cProbe15.run(variant)
        case "T16" => T1cProbe16.run(variant)
        case "T19" => T1cProbe19.run(variant)
        case _     => v("ERROR", s"unknown child $id")
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace(System.out)
        v("ERROR", s"$id/$variant threw $t")
    }
    System.out.flush()
    Runtime.getRuntime.halt(0)
  }

  def xsftLabel: String = s"enableXSFT=${CacheFixtures.xsftEnabledByDefault}"
}
