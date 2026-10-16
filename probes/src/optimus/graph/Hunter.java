package optimus.graph;

import java.io.File;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * H3 (lost-wakeup search) and H5 (thread accounting) hunter, TEST_CAMPAIGN.md Tier 2.
 *
 * <p>A seed generates a layered random DAG of hand-written NodeTask subclasses (SYNC, FAN, SEQ,
 * MIXED, ASYNC, LEAF, PROMISE), optional cancellation scopes over subtrees, and a set of client
 * threads that each runAndWait one entry node of a shared graph instance. The child JVM runs the
 * graph repeatedly on the real OGScheduler and checks, after every iteration: per-node result
 * oracles against a sequential evaluation, hang (watchdog + classification), and H5 accounting
 * after quiescence. The parent sweeps seeds x configurations, reruns failures and minimises N.
 *
 * <p>Usage (via run-hunter.sh): {@code sweep <firstSeed> <count>} | {@code one <seed> <cfg> [maxN]
 * [reps]} | {@code --child <seed> <cfg>} (internal). cfg = i{ideal}-a{asserts}-d{delay}-lt{limit}.
 */
public final class Hunter {

  // ================================================================== spec (pure, from the seed)

  enum Kind {
    SYNC, // enqueue kids, runAndWait each from inside run()
    FAN, // enqueue kids, continueWith(this) on all; onChildCompleted counts down and completes
    SEQ, // enqueue kids, continueWith on the first undone; default onChildCompleted re-enqueues
    MIXED, // some kids sync (runAndWait), some via continueWith count-down
    ASYNC, // exclusive kids launched from a non-graph thread via evaluateNodeAsync/enqueueAndTrackCompletion
    LEAF, // completes with its weight; some fail by throwing
    PROMISE // run() hands completion to a non-graph thread after a delay (external callback)
  }

  static final class Spec {
    long seed;
    int n, levels, clients, failEvery;
    Kind[] kind;
    int[] level, primaryParent, scopeOf, promiseDelayUs, clientRoot;
    int[][] kids;
    boolean[][] syncEdge; // MIXED: per kid
    boolean[][] extEdge; // ASYNC: per kid, launched externally
    boolean[] viaEvalNodeAsync, fails;
    long[] weight, expected;
    boolean[] succeeds;
    // scopes
    int[] scopeRoot, scopeParent, cancelDelayUs;
    double pShare;

    String summary() {
      int[] kc = new int[Kind.values().length];
      int edges = 0, ext = 0, shared = 0;
      int[] parents = new int[n];
      for (int i = 0; i < n; i++) {
        kc[kind[i].ordinal()]++;
        edges += kids[i].length;
        for (int j = 0; j < kids[i].length; j++) {
          parents[kids[i][j]]++;
          if (extEdge[i][j]) ext++;
        }
      }
      for (int p : parents) if (p > 1) shared++;
      StringBuilder sb = new StringBuilder();
      sb.append("N=").append(n).append(" levels=").append(levels).append(" edges=").append(edges)
          .append(" sharedNodes=").append(shared).append(" clients=").append(clients)
          .append(" scopes=").append(scopeRoot.length).append(" failEvery=").append(failEvery)
          .append(" extEdges=").append(ext).append(" kinds=");
      for (Kind k : Kind.values()) sb.append(k.name()).append(':').append(kc[k.ordinal()]).append(',');
      sb.setLength(sb.length() - 1);
      sb.append(" roots=").append(Arrays.toString(clientRoot).replace(" ", ""));
      return sb.toString();
    }
  }

  static Spec generate(long seed, int maxN) {
    Random r = new Random(seed * 0x9E3779B97F4A7C15L + 0x5DEECE66DL);
    Spec s = new Spec();
    s.seed = seed;
    int n = (int) Math.round(10 * Math.pow(200, r.nextDouble())); // 10..2000, log-uniform
    int levels = Math.min(3 + r.nextInt(8), Math.max(2, n / 3));
    // level sizes: 1 at the top, at least 1 per level, rest biased to deeper levels
    int[] cnt = new int[levels];
    cnt[0] = 1;
    for (int l = 1; l < levels; l++) cnt[l] = 1;
    for (int k = levels; k < n; k++) cnt[1 + (int) ((levels - 1) * Math.pow(r.nextDouble(), 0.6))]++;
    int[] level = new int[n];
    int[] first = new int[levels + 1];
    for (int l = 0, i = 0; l < levels; l++) {
      first[l] = i;
      for (int k = 0; k < cnt[l]; k++) level[i++] = l;
    }
    first[levels] = n;
    List<List<Integer>> kids = new ArrayList<>();
    for (int i = 0; i < n; i++) kids.add(new ArrayList<>());
    int[] primary = new int[n];
    primary[0] = -1;
    for (int i = 1; i < n; i++) {
      int l = level[i];
      int p = first[l - 1] + r.nextInt(cnt[l - 1]);
      primary[i] = p;
      kids.get(p).add(i);
    }
    s.pShare = r.nextDouble() < 0.25 ? 0 : r.nextDouble() * 0.4;
    for (int i = 0; i < n; i++) {
      int l = level[i];
      if (l >= levels - 1 || r.nextDouble() >= s.pShare) continue;
      int extra = 1 + r.nextInt(3);
      for (int e = 0; e < extra; e++) {
        int tl = l + 1 + r.nextInt(Math.min(2, levels - 1 - l));
        int j = first[tl] + r.nextInt(cnt[tl]);
        if (!kids.get(i).contains(j)) kids.get(i).add(j);
      }
    }
    // kind mix per seed
    double wSync = r.nextDouble(), wFan = r.nextDouble(), wSeq = r.nextDouble(), wMixed = r.nextDouble();
    double wAsync = r.nextDouble() < 0.5 ? 0 : r.nextDouble() * 0.5;
    double pPromise = r.nextDouble() < 0.4 ? 0 : r.nextDouble() * 0.5;
    double pSyncEdge = r.nextDouble();
    s.failEvery = r.nextDouble() < 0.5 ? 0 : 3 + r.nextInt(30);
    Kind[] kind = new Kind[n];
    double tot = wSync + wFan + wSeq + wMixed + wAsync;
    int[] parents = new int[n];
    for (int i = 0; i < n; i++) for (int j : kids.get(i)) parents[j]++;
    boolean[][] syncEdge = new boolean[n][], extEdge = new boolean[n][];
    boolean[] viaENA = new boolean[n];
    int[] promiseDelay = new int[n];
    for (int i = 0; i < n; i++) {
      int nk = kids.get(i).size();
      syncEdge[i] = new boolean[nk];
      extEdge[i] = new boolean[nk];
      double u = r.nextDouble() * tot; // always drawn so truncation does not shift the stream
      double pd = r.nextDouble();
      promiseDelay[i] = r.nextInt(500);
      viaENA[i] = r.nextBoolean();
      if (nk == 0) {
        kind[i] = pd < pPromise ? Kind.PROMISE : Kind.LEAF;
        continue;
      }
      if ((u -= wSync) < 0) kind[i] = Kind.SYNC;
      else if ((u -= wFan) < 0) kind[i] = Kind.FAN;
      else if ((u -= wSeq) < 0) kind[i] = Kind.SEQ;
      else if ((u -= wMixed) < 0) kind[i] = Kind.MIXED;
      else kind[i] = Kind.ASYNC;
      for (int j = 0; j < nk; j++) {
        syncEdge[i][j] = r.nextDouble() < pSyncEdge;
        int k = kids.get(i).get(j);
        extEdge[i][j] = kind[i] == Kind.ASYNC && parents[k] == 1 && primary[k] == i;
      }
    }
    // cancellation scopes over subtrees
    List<Integer> scopeRoots = new ArrayList<>();
    if (r.nextDouble() < 0.5 && n > 3) {
      int ns = 1 + r.nextInt(3);
      for (int k = 0; k < ns; k++) {
        int c = 1 + r.nextInt(n - 1);
        if (!scopeRoots.contains(c)) scopeRoots.add(c);
      }
    }
    scopeRoots.sort(null);
    int[] cancelDelay = new int[scopeRoots.size()];
    for (int k = 0; k < cancelDelay.length; k++) cancelDelay[k] = r.nextInt(4000);
    int clients = 1 + r.nextInt(6);
    int[] clientRoot = new int[clients];
    for (int c = 1; c < clients; c++) {
      if (r.nextBoolean() || levels < 2) clientRoot[c] = 0;
      else {
        int l = 1 + r.nextInt(Math.min(2, levels - 1));
        clientRoot[c] = first[l] + r.nextInt(cnt[l]);
      }
    }

    // ---- truncation (minimisation): keep the first maxN nodes (BFS order), drop edges past it
    int m = maxN > 0 ? Math.min(n, Math.max(2, maxN)) : n;
    s.n = m;
    s.levels = level[m - 1] + 1;
    s.kind = new Kind[m];
    s.level = Arrays.copyOf(level, m);
    s.primaryParent = Arrays.copyOf(primary, m);
    s.kids = new int[m][];
    s.syncEdge = new boolean[m][];
    s.extEdge = new boolean[m][];
    s.viaEvalNodeAsync = Arrays.copyOf(viaENA, m);
    s.promiseDelayUs = Arrays.copyOf(promiseDelay, m);
    for (int i = 0; i < m; i++) {
      List<Integer> ks = kids.get(i);
      int c = 0;
      for (int k : ks) if (k < m) c++;
      s.kids[i] = new int[c];
      s.syncEdge[i] = new boolean[c];
      s.extEdge[i] = new boolean[c];
      for (int j = 0, o = 0; j < ks.size(); j++) {
        if (ks.get(j) >= m) continue;
        s.kids[i][o] = ks.get(j);
        s.syncEdge[i][o] = syncEdge[i][j];
        s.extEdge[i][o] = extEdge[i][j];
        o++;
      }
      s.kind[i] = c > 0 ? kind[i] : (kind[i] == Kind.PROMISE ? Kind.PROMISE : Kind.LEAF);
    }
    s.fails = new boolean[m];
    for (int i = 0; i < m; i++) s.fails[i] = s.kind[i] == Kind.LEAF && s.failEvery > 0 && i % s.failEvery == 0;
    s.weight = new long[m];
    for (int i = 0; i < m; i++) s.weight[i] = 1 + (i * 31L % 7);
    List<Integer> sr = new ArrayList<>();
    List<Integer> sd = new ArrayList<>();
    for (int k = 0; k < scopeRoots.size(); k++)
      if (scopeRoots.get(k) < m) {
        sr.add(scopeRoots.get(k));
        sd.add(cancelDelay[k]);
      }
    s.scopeRoot = sr.stream().mapToInt(Integer::intValue).toArray();
    s.cancelDelayUs = sd.stream().mapToInt(Integer::intValue).toArray();
    s.scopeParent = new int[s.scopeRoot.length];
    s.scopeOf = new int[m];
    s.scopeOf[0] = -1;
    for (int i = 1; i < m; i++) {
      int ps = s.scopeOf[s.primaryParent[i]];
      int idx = sr.indexOf(i);
      if (idx >= 0) {
        s.scopeParent[idx] = ps;
        s.scopeOf[i] = idx;
      } else s.scopeOf[i] = ps;
    }
    s.clients = clients;
    s.clientRoot = new int[clients];
    for (int c = 0; c < clients; c++) s.clientRoot[c] = clientRoot[c] < m ? clientRoot[c] : 0;
    // sequential oracle: failing leaves are excluded from the sum, everything else succeeds
    s.expected = new long[m];
    s.succeeds = new boolean[m];
    for (int i = m - 1; i >= 0; i--) {
      long v = s.weight[i];
      for (int k : s.kids[i]) if (s.succeeds[k]) v += s.expected[k];
      s.expected[i] = v;
      s.succeeds[i] = !s.fails[i];
    }
    return s;
  }

  // ================================================================== runtime graph

  static final ConcurrentLinkedQueue<String> findings = new ConcurrentLinkedQueue<>();
  static final AtomicInteger findingCount = new AtomicInteger();
  static volatile int curIter;

  static void finding(String kind, String detail) {
    if (findingCount.incrementAndGet() <= 200) {
      String l = "FINDING " + kind + " iter=" + curIter + " " + detail;
      findings.add(l);
      System.out.println(l);
    }
  }

  static final AtomicLong statPromises = new AtomicLong(), statExtLaunches = new AtomicLong(),
      statCancelledScopes = new AtomicLong(), statCancelledNodes = new AtomicLong(), statEnqueued = new AtomicLong(),
      statFailedLeaves = new AtomicLong(), statSeqReruns = new AtomicLong();

  static final class G {
    final Spec sp;
    final OGScheduler s;
    final ScheduledThreadPoolExecutor ext;
    final HN[] nodes;
    final ScenarioStack[] scopeSS;
    final AtomicInteger extOutstanding = new AtomicInteger();

    G(Spec sp, OGScheduler s, ScheduledThreadPoolExecutor ext, ScenarioStack base) {
      this.sp = sp;
      this.s = s;
      this.ext = ext;
      scopeSS = new ScenarioStack[sp.scopeRoot.length];
      for (int k = 0; k < scopeSS.length; k++) {
        ScenarioStack parent = sp.scopeParent[k] < 0 ? base : scopeSS[sp.scopeParent[k]];
        scopeSS[k] = parent.withChildCancellationScope();
      }
      nodes = new HN[sp.n];
      for (int i = 0; i < sp.n; i++) {
        ScenarioStack ss = sp.scopeOf[i] < 0 ? base : scopeSS[sp.scopeOf[i]];
        nodes[i] = switch (sp.kind[i]) {
          case SYNC -> new SyncN(this, i, ss);
          case FAN -> new FanN(this, i, ss);
          case SEQ -> new SeqN(this, i, ss);
          case MIXED -> new MixedN(this, i, ss);
          case ASYNC -> new AsyncN(this, i, ss);
          case LEAF -> new LeafN(this, i, ss);
          case PROMISE -> new PromiseN(this, i, ss);
        };
      }
    }

    void extRun(Runnable r, long delayUs) {
      extOutstanding.incrementAndGet();
      Runnable w = () -> {
        try {
          r.run();
        } catch (Throwable t) {
          finding("EXT_THREW", t.toString());
          t.printStackTrace(System.out);
        } finally {
          extOutstanding.decrementAndGet();
        }
      };
      if (delayUs <= 0) ext.execute(w);
      else ext.schedule(w, delayUs, TimeUnit.MICROSECONDS);
    }
  }

  abstract static class HN extends NodeTask {
    final G g;
    final int i;
    volatile long value;
    volatile boolean enqueued;
    final AtomicInteger runs = new AtomicInteger(), completes = new AtomicInteger();
    final java.util.concurrent.atomic.AtomicIntegerArray notified, registered;

    HN(G g, int i, ScenarioStack ss) {
      this.g = g;
      this.i = i;
      notified = new java.util.concurrent.atomic.AtomicIntegerArray(g.sp.kids[i].length);
      registered = new java.util.concurrent.atomic.AtomicIntegerArray(g.sp.kids[i].length);
      attach(ss);
    }

    int pendingValue() {
      return -1;
    }

    final void noteNotified(NodeTask child) {
      for (int j = 0; j < nk(); j++) if (kid(j) == child) notified.incrementAndGet(j);
    }

    final void register(int j, EvaluationQueue eq) {
      registered.incrementAndGet(j);
      kid(j).continueWith(this, eq);
    }

    String kidReport() {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < nk(); j++) {
        HN c = kid(j);
        sb.append(" [").append(c).append(g.sp.syncEdge[i][j] && g.sp.kind[i] == Kind.MIXED ? " sync" : "").append(g.sp.extEdge[i][j] ? " ext" : "")
            .append(" done=").append(c.isDone()).append(" reg=").append(registered.get(j)).append(" notified=").append(notified.get(j))
            .append(" state=").append(c.stateAsString().replace(' ', '_')).append(']');
      }
      return "pending=" + pendingValue() + sb;
    }

    final HN kid(int j) {
      return g.nodes[g.sp.kids[i][j]];
    }

    final int nk() {
      return g.sp.kids[i].length;
    }

    final void enq(OGSchedulerContext ec, HN c) {
      if (!c.enqueued) {
        c.enqueued = true;
        statEnqueued.incrementAndGet();
      }
      ec.enqueue(this, c);
    }

    final long sumKids() {
      long v = g.sp.weight[i];
      for (int j = 0; j < nk(); j++) {
        HN c = kid(j);
        if (c.isDoneWithResult()) v += c.value;
      }
      return v;
    }

    final void finish(EvaluationQueue eq, long v) {
      value = v;
      completes.incrementAndGet();
      complete(eq);
    }

    final void combine(HN c, EvaluationQueue eq) {
      combineInfo(c, eq);
    }

    @Override
    public String toString() {
      return "H" + g.sp.kind[i].name() + "#" + i;
    }
  }

  static final class LeafN extends HN {
    LeafN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      if (g.sp.fails[i]) {
        statFailedLeaves.incrementAndGet();
        throw new RuntimeException("hunter-leaf-fail-" + i);
      }
      finish(ec, g.sp.weight[i]);
    }
  }

  static final class PromiseN extends HN {
    PromiseN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      statPromises.incrementAndGet();
      // completed from a non-graph thread with the scheduler as the EvaluationQueue (external callback)
      g.extRun(() -> finish(g.s, g.sp.weight[i]), g.sp.promiseDelayUs[i]);
    }
  }

  static final class SyncN extends HN {
    SyncN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      for (int j = 0; j < nk(); j++) enq(ec, kid(j));
      for (int j = 0; j < nk(); j++) {
        HN c = kid(j);
        ec.runAndWait(c);
        combine(c, ec);
      }
      finish(ec, sumKids());
    }
  }

  static final class FanN extends HN {
    final AtomicInteger pending = new AtomicInteger();

    FanN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      pending.set(nk());
      for (int j = 0; j < nk(); j++) enq(ec, kid(j));
      for (int j = 0; j < nk(); j++) register(j, ec);
    }

    @Override
    int pendingValue() {
      return pending.get();
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      noteNotified(child);
      combineInfo(child, eq);
      if (pending.decrementAndGet() == 0) finish(eq, sumKids());
    }
  }

  static final class SeqN extends HN {
    boolean started;
    int idx;

    SeqN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      if (runs.incrementAndGet() > 1) statSeqReruns.incrementAndGet();
      if (!started) {
        started = true;
        for (int j = 0; j < nk(); j++) enq(ec, kid(j));
      }
      while (idx < nk()) {
        HN c = kid(idx);
        if (!c.isDone()) {
          register(idx, ec);
          return;
        }
        combine(c, ec);
        idx++;
      }
      finish(ec, sumKids());
    }
  }

  static final class MixedN extends HN {
    final AtomicInteger pending = new AtomicInteger();

    MixedN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      int nAsync = 0;
      for (int j = 0; j < nk(); j++) if (!g.sp.syncEdge[i][j]) nAsync++;
      pending.set(nAsync + 1);
      for (int j = 0; j < nk(); j++) enq(ec, kid(j));
      for (int j = 0; j < nk(); j++) if (!g.sp.syncEdge[i][j]) register(j, ec);
      for (int j = 0; j < nk(); j++)
        if (g.sp.syncEdge[i][j]) {
          ec.runAndWait(kid(j));
          combine(kid(j), ec);
        }
      if (pending.decrementAndGet() == 0) finish(ec, sumKids());
    }

    @Override
    int pendingValue() {
      return pending.get();
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      noteNotified(child);
      if (pending.decrementAndGet() == 0) finish(eq, sumKids());
    }
  }

  /** -Dhunter.asyncMode=mixed (default: per node from the seed) | ena (all evaluateNodeAsync) | eatc (all enqueueAndTrackCompletion) */
  static final String ASYNC_MODE = System.getProperty("hunter.asyncMode", "mixed");

  static final class AsyncN extends HN {
    final AtomicInteger pending = new AtomicInteger();

    AsyncN(G g, int i, ScenarioStack ss) {
      super(g, i, ss);
    }

    @Override
    public void run(OGSchedulerContext ec) {
      runs.incrementAndGet();
      pending.set(nk());
      List<HN> ext = new ArrayList<>();
      for (int j = 0; j < nk(); j++) {
        HN c = kid(j);
        if (g.sp.extEdge[i][j]) ext.add(c);
        else {
          enq(ec, c);
          register(j, ec);
        }
      }
      boolean viaENA = ASYNC_MODE.equals("ena") || (ASYNC_MODE.equals("mixed") && g.sp.viaEvalNodeAsync[i]);
      for (HN c : ext) {
        g.extRun(
            () -> {
              statExtLaunches.incrementAndGet();
              if (!c.enqueued) {
                c.enqueued = true;
                statEnqueued.incrementAndGet();
              }
              if (viaENA) g.s.evaluateNodeAsync(c);
              else g.s.enqueueAndTrackCompletion(c);
              for (int j = 0; j < nk(); j++) if (kid(j) == c) registered.incrementAndGet(j);
              c.continueWith(this, g.s);
            },
            0);
      }
    }

    @Override
    int pendingValue() {
      return pending.get();
    }

    @Override
    protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      noteNotified(child);
      if (pending.decrementAndGet() == 0) finish(eq, sumKids());
    }
  }

  // ================================================================== oracles

  static boolean isCancellation(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      String n = t.getClass().getName();
      if (t instanceof OptimusCancellationException || n.endsWith("ScopeWasCancelledException")) return true;
      if (t.getCause() == t) break;
    }
    return false;
  }

  static void checkGraph(G g) {
    Spec sp = g.sp;
    int n = sp.n;
    boolean[] cancelled = new boolean[n], taint = new boolean[n];
    boolean[] scopeCancelled = new boolean[sp.scopeRoot.length];
    for (int k = 0; k < scopeCancelled.length; k++) {
      scopeCancelled[k] = g.scopeSS[k].cancelScope().isCancelled();
      if (scopeCancelled[k]) statCancelledScopes.incrementAndGet();
    }
    for (int i = n - 1; i >= 0; i--) {
      cancelled[i] = sp.scopeOf[i] >= 0 && scopeCancelled[sp.scopeOf[i]];
      boolean t = cancelled[i];
      for (int k : sp.kids[i]) t |= taint[k];
      taint[i] = t;
    }
    int reported = 0;
    for (int i = 0; i < n; i++) {
      HN x = g.nodes[i];
      String id = x + "(level " + sp.level[i] + ")";
      if (x.completes.get() > 1) finding("DOUBLE_COMPLETE_CALL", id + " completes=" + x.completes.get());
      if (sp.kind[i] != Kind.SEQ && x.runs.get() > 1) finding("DOUBLE_RUN", id + " runs=" + x.runs.get());
      if (!x.enqueued) {
        if (x.runs.get() > 0) finding("RAN_UNENQUEUED", id);
        continue;
      }
      if (!x.isDone()) {
        if (reported++ < 5)
          finding("NOT_DONE", id + " state=" + x.stateAsString().replace(' ', '_') + " runs=" + x.runs.get() + " cancelledScope=" + cancelled[i] + " " + x.kidReport());
        continue;
      }
      if (x.isDoneWithException()) {
        Throwable e = x.exception();
        if (cancelled[i] && isCancellation(e)) {
          statCancelledNodes.incrementAndGet();
          continue;
        }
        if (sp.fails[i] && e != null && String.valueOf(e.getMessage()).startsWith("hunter-leaf-fail-")) continue;
        if (reported++ < 5) finding("UNEXPECTED_EXCEPTION", id + " cancelledScope=" + cancelled[i] + " ex=" + e);
        continue;
      }
      if (sp.fails[i]) finding("FAILING_LEAF_SUCCEEDED", id);
      long local = x.sumKids();
      if (x.value != local && reported++ < 5) finding("LOCAL_SUM_MISMATCH", id + " value=" + x.value + " recomputed=" + local);
      if (!taint[i] && x.value != sp.expected[i] && reported++ < 5)
        finding("VALUE_MISMATCH", id + " value=" + x.value + " expected=" + sp.expected[i]);
    }
    for (int c = 0; c < sp.clients; c++) {
      HN r = g.nodes[sp.clientRoot[c]];
      if (!r.isDone()) finding("ROOT_NOT_DONE", "client " + c + " root " + r);
    }
  }

  // ================================================================== reflection helpers

  static Field field(Class<?> c, String name) {
    try {
      Field f = c.getDeclaredField(name);
      f.setAccessible(true);
      return f;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final Field F_LOCK = field(OGScheduler.class, "_lock"), F_WORKERS = field(OGScheduler.class, "_workers"),
      F_WAITERS = field(OGScheduler.class, "_waiters"), F_IDLE = field(OGScheduler.class, "_idlePool"),
      F_EXTB = field(OGScheduler.class, "_externallyBlocked"), F_SPIN = field(OGScheduler.class, "_spinningThreadID"),
      F_QQ = field(OGScheduler.class, "_qqueue"), F_Q = field(OGScheduler.class, "_queue"),
      F_TRACKED = field(OGScheduler.class, "_evalAsyncTrackedNodes"), F_CB = field(OGScheduler.class, "_onStalledCallbacks"),
      F_IDEAL = field(OGScheduler.class, "_idealCount"), F_SID = field(OGScheduler.class, "schedulerID"),
      F_WQ_Q = field(OGSchedulerContext.WorkQueue.class, "_q"), F_WQ_TOP = field(OGSchedulerContext.WorkQueue.class, "_top"),
      F_WQ_BASE = field(OGSchedulerContext.WorkQueue.class, "_base");

  /** [runnable, total non-null] tasks in a WorkQueue (unsynchronised read). */
  static int[] wqCounts(OGSchedulerContext.WorkQueue q) {
    try {
      NodeTask[] a = (NodeTask[]) F_WQ_Q.get(q);
      int top = F_WQ_TOP.getInt(q), base = F_WQ_BASE.getInt(q);
      int run = 0, tot = 0;
      int mask = a.length - 1;
      for (int i = base; i < top && i - base <= a.length; i++) {
        NodeTask t = a[i & mask];
        if (t == null || t == NodeTask.nullTask) continue;
        tot++;
        if (!t.isDoneOrNotRunnable()) run++;
      }
      return new int[] {run, tot};
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static OGScheduler.Context[] contexts(OGScheduler s, Field f) throws Exception {
    OGScheduler.Contexts cs = (OGScheduler.Contexts) f.get(s);
    OGScheduler.Context[] a = cs.getUnderlyingList();
    int c = Math.min(cs.count, a.length);
    List<OGScheduler.Context> r = new ArrayList<>();
    for (int i = 0; i < c; i++) if (a[i] != null) r.add(a[i]);
    return r.toArray(new OGScheduler.Context[0]);
  }

  static List<Thread> ogWorkers(int sid) {
    List<Thread> r = new ArrayList<>();
    String pfx = OGScheduler.ogThreadNamePrefix + sid + "-";
    for (Thread t : Thread.getAllStackTraces().keySet()) if (t.isAlive() && t.getName().startsWith(pfx)) r.add(t);
    return r;
  }

  /** Thread is an idle og.worker: parked (possibly inside a Delay park) and not RUNNABLE/BLOCKED. */
  static boolean isIdleWorker(Thread t) {
    Thread.State st = t.getState();
    return st == Thread.State.TIMED_WAITING || st == Thread.State.WAITING || st == Thread.State.TERMINATED;
  }

  // ================================================================== H5 accounting

  static final class Acct {
    int workers, waiters, idle, extBlocked, spin, qqRun, qqTot, qRun, qTot, tracked, callbacks, ideal, idleCtxRun,
        idleDeadThreads, liveOg, nonIdleOg;
    boolean bad;
    String nonIdleNames = "";

    boolean quiet() {
      return workers == 0 && waiters == 0 && spin == 0 && nonIdleOg == 0;
    }

    List<String> violations() {
      List<String> v = new ArrayList<>();
      if (workers != 0) v.add("workers=" + workers);
      if (waiters != 0) v.add("waiters=" + waiters);
      if (spin != 0) v.add("spinningThreadID=" + spin + "(no workers)");
      if (qqRun != 0) v.add("qqueueRunnable=" + qqRun);
      if (qRun != 0) v.add("queueRunnable=" + qRun);
      if (idleCtxRun != 0) v.add("idleCtxQueueRunnable=" + idleCtxRun);
      if (tracked != 0) v.add("trackedNodes=" + tracked);
      if (callbacks != 0) v.add("onStalledCallbacks=" + callbacks);
      if (extBlocked != 0) v.add("externallyBlocked=" + extBlocked);
      if (idle > 2 * Math.max(ideal, 1)) v.add("idlePool=" + idle + ">2*ideal");
      if (idleDeadThreads != 0) v.add("idleCtxWithDeadThread=" + idleDeadThreads);
      if (nonIdleOg != 0) v.add("nonIdleOgWorkers=" + nonIdleOg + nonIdleNames);
      if (liveOg > idle + workers) v.add("liveOgWorkers=" + liveOg + ">idle+workers=" + (idle + workers));
      return v;
    }

    @Override
    public String toString() {
      return "workers=" + workers + " waiters=" + waiters + " idle=" + idle + " extBlocked=" + extBlocked + " spin=" + spin
          + " qqRun/Tot=" + qqRun + "/" + qqTot + " qRun/Tot=" + qRun + "/" + qTot + " tracked=" + tracked
          + " callbacks=" + callbacks + " badState=" + bad + " liveOg=" + liveOg + " nonIdleOg=" + nonIdleOg;
    }
  }

  @SuppressWarnings("unchecked")
  static Acct account(OGScheduler s) throws Exception {
    Acct a = new Acct();
    int sid = F_SID.getInt(s);
    synchronized (F_LOCK.get(s)) {
      a.workers = ((OGScheduler.Contexts) F_WORKERS.get(s)).count;
      a.waiters = ((OGScheduler.Contexts) F_WAITERS.get(s)).count;
      a.idle = ((OGScheduler.Contexts) F_IDLE.get(s)).count;
      a.extBlocked = ((OGScheduler.Contexts) F_EXTB.get(s)).count;
      a.spin = F_SPIN.getInt(s);
      int[] qq = wqCounts((OGSchedulerContext.WorkQueue) F_QQ.get(s));
      a.qqRun = qq[0];
      a.qqTot = qq[1];
      java.util.PriorityQueue<NodeTask> q = (java.util.PriorityQueue<NodeTask>) F_Q.get(s);
      for (NodeTask t : q) {
        a.qTot++;
        if (!t.isDoneOrNotRunnable()) a.qRun++;
      }
      Object tr = F_TRACKED.get(s);
      synchronized (tr) {
        a.tracked = ((optimus.core.IndexedArrayList<?>) tr).count;
      }
      a.callbacks = ((java.util.PriorityQueue<?>) F_CB.get(s)).size();
      a.ideal = F_IDEAL.getInt(s);
      for (OGScheduler.Context c : contexts(s, F_IDLE)) {
        a.idleCtxRun += wqCounts(c.queue)[0];
        if (c.thread != null && !c.thread.isAlive()) a.idleDeadThreads++;
      }
      a.bad = s.schedulerInBadState();
    }
    StringBuilder nn = new StringBuilder();
    for (Thread t : ogWorkers(sid)) {
      a.liveOg++;
      if (!isIdleWorker(t)) {
        a.nonIdleOg++;
        nn.append(' ').append(t.getName()).append(':').append(t.getState());
      }
    }
    a.nonIdleNames = nn.toString();
    return a;
  }

  static Acct settle(OGScheduler s, long maxMs) throws Exception {
    long t0 = System.nanoTime();
    Acct a = account(s);
    while (!a.quiet() && (System.nanoTime() - t0) / 1_000_000 < maxMs) {
      Thread.sleep(2);
      a = account(s);
    }
    long ms = (System.nanoTime() - t0) / 1_000_000;
    maxSettleMs = Math.max(maxSettleMs, ms);
    return a;
  }

  static long maxSettleMs;

  // ================================================================== hang dump + classification

  static void hang(G g, OGScheduler s, String why, long waitedMs) {
    snapshot(g, s, why, waitedMs, true);
  }

  static int stallDumps;

  static String snapshot(G g, OGScheduler s, String why, long waitedMs, boolean fatal) {
    System.out.println((fatal ? "CHILD-HANG " : "STALL-DUMP ") + why + " after " + waitedMs + " ms");
    StringBuilder cls = new StringBuilder();
    try {
      // 1. thread stacks first (no locks)
      Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
      int blockedOnLock = 0;
      for (var e : all.entrySet()) {
        Thread th = e.getKey();
        String n = th.getName();
        if (n.startsWith("og.worker") || n.startsWith("hunter-") || n.equals("main")) {
          StackTraceElement[] st = e.getValue();
          System.out.println("--- " + n + " " + th.getState());
          for (int i = 0; i < Math.min(22, st.length); i++) System.out.println("    " + st[i]);
          if (th.getState() == Thread.State.BLOCKED && st.length > 0 && st[0].getClassName().startsWith("optimus.graph.OGScheduler"))
            blockedOnLock++;
        }
      }
      // 2. scheduler status with a time bound (it takes _lock briefly)
      Thread ts = new Thread(() -> {
        try {
          System.out.println("---- OGScheduler.toString()");
          System.out.println(s.toString());
        } catch (Throwable t) {
          System.out.println("scheduler toString threw " + t);
        }
      }, "hunter-dump");
      ts.setDaemon(true);
      ts.start();
      ts.join(5000);
      if (ts.isAlive()) System.out.println("(OGScheduler.toString() did not return in 5 s: _lock held?)");
      // 3. unsynchronised snapshot for classification
      OGScheduler.Context[] workers = contexts(s, F_WORKERS), waiters = contexts(s, F_WAITERS), idle = contexts(s, F_IDLE);
      int[] qq = wqCounts((OGSchedulerContext.WorkQueue) F_QQ.get(s));
      int qRun = 0;
      try {
        for (Object o : ((java.util.PriorityQueue<?>) F_Q.get(s)).toArray()) if (!((NodeTask) o).isDoneOrNotRunnable()) qRun++;
      } catch (Exception ignored) {
      }
      int localRun = 0;
      for (OGScheduler.Context c : waiters) localRun += wqCounts(c.queue)[0];
      for (OGScheduler.Context c : workers) localRun += wqCounts(c.queue)[0];
      for (OGScheduler.Context c : idle) localRun += wqCounts(c.queue)[0];
      System.out.println("HANG-SNAPSHOT workers=" + workers.length + " waiters=" + waiters.length + " idle=" + idle.length
          + " spin=" + F_SPIN.getInt(s) + " qqRunnable=" + qq[0] + "/" + qq[1] + " queueRunnable=" + qRun
          + " localQueuesRunnable=" + localRun + " blockedOnSchedulerMonitor=" + blockedOnLock
          + " extOutstanding=" + g.extOutstanding.get() + " timerLW=" + HunterHooks.timerLW.get() + " timerCS=" + HunterHooks.timerCS.get());
      int lost = 0;
      for (OGScheduler.Context c : waiters) {
        NodeTask at = c.awaitedTask;
        boolean atDone = at != null && at.isDoneEx();
        System.out.println("  waiter " + (c.thread == null ? "?" : c.thread.getName()) + " tid=" + c.threadID + " parked=" + c.parked
            + " awaitedCausality=" + c.awaitedCausalityID + " awaited=" + at + " awaitedDone=" + atDone + HunterH4.postMortem(c)
            + (at != null ? " awaitedState=" + at.stateAsString().replace(' ', '_') + " awaitedCausality=" + at.causalityThreadID() + "/" + at.causalityID() : ""));
        if (atDone && c.parked) lost++;
      }
      // graph-level: enqueued but not done
      int notDone = 0, inversion = 0, lostNotify = 0, runnableStuck = 0, promiseStuck = 0, shown = 0;
      for (HN x : g.nodes) {
        if (!x.enqueued || x.isDone()) continue;
        notDone++;
        boolean kidsDone = true;
        for (int j = 0; j < x.nk(); j++) kidsDone &= x.kid(j).isDone();
        Kind k = g.sp.kind[x.i];
        boolean runnable = !x.isDoneOrNotRunnable();
        if (runnable) runnableStuck++;
        if (k == Kind.PROMISE && x.runs.get() > 0) promiseStuck++;
        if (kidsDone && !runnable && x.nk() > 0) {
          // all kids done but x is not: either a notification was lost (a registered kid never called back),
          // or x's run() is suspended in a sync frame on some stack below a frame that now waits on x (inversion)
          boolean lostCb = false;
          for (int j = 0; j < x.nk(); j++) if (x.registered.get(j) > 0 && x.notified.get(j) == 0 && k != Kind.SEQ) lostCb = true;
          if (k == Kind.SEQ && x.registered.get(Math.min(((SeqN) x).idx, x.nk() - 1)) > 0) lostCb = true;
          if (lostCb) lostNotify++;
          else if (k == Kind.SYNC || k == Kind.MIXED) inversion++;
          System.out.println("  " + (lostCb ? "LOST-NOTIFY " : "SUSPENDED-WITH-KIDS-DONE ") + x + " level=" + g.sp.level[x.i] + " runs=" + x.runs.get() + " " + x.kidReport());
        }
        if (shown++ < 25)
          System.out.println("  notDone " + x + " level=" + g.sp.level[x.i] + " runs=" + x.runs.get() + " state=" + x.stateAsString().replace(' ', '_')
              + " causality=" + x.causalityThreadID() + "/" + x.causalityID() + " kidsDone=" + kidsDone + " waitingOn=" + x.getWaitingOn());
      }
      // runnable tasks on the global queues and every waiter's forward chain
      OGSchedulerContext.WorkQueue gq = (OGSchedulerContext.WorkQueue) F_QQ.get(s);
      NodeTask[] qa = (NodeTask[]) F_WQ_Q.get(gq);
      int qtop = F_WQ_TOP.getInt(gq), qbase = F_WQ_BASE.getInt(gq);
      for (int qi = qbase, shownQ = 0; qi < qtop && shownQ < 15; qi++) {
        NodeTask t = qa[qi & (qa.length - 1)];
        if (t != null && t != NodeTask.nullTask && !t.isDoneOrNotRunnable()) {
          shownQ++;
          System.out.println("  qqueue-runnable " + t + " state=" + t.stateAsString().replace(' ', '_') + " causality=" + t.causalityThreadID() + "/" + t.causalityID()
              + (t instanceof HN h ? " enqueued=" + h.enqueued + " runs=" + h.runs.get() : ""));
        }
      }
      int chainRunnable = 0;
      for (OGScheduler.Context c : waiters) {
        StringBuilder ch = new StringBuilder();
        NodeTask t = c.awaitedTask;
        for (int d = 0; t != null && d < 15; d++, t = t.getWaitingOn()) {
          if (!t.isDoneOrNotRunnable()) chainRunnable++;
          ch.append(" -> ").append(t).append('(').append(t.stateAsString().replace(' ', '_')).append(' ').append(t.causalityThreadID()).append('/').append(t.causalityID()).append(')');
        }
        System.out.println("  chain " + (c.thread == null ? "?" : c.thread.getName()) + ":" + ch);
      }
      System.out.println("HANG-GRAPH notDone=" + notDone + " runnableNotDone=" + runnableStuck + " lostNotify=" + lostNotify + " inversion=" + inversion + " promiseStuck=" + promiseStuck);
      if (blockedOnLock > 0 && !ts.isAlive()) cls.append("other:monitor-blocked ");
      if (ts.isAlive()) cls.append("lock-held ");
      if (lost > 0 && workers.length == 0) cls.append("lost-wakeup ");
      // runnable work that no waiter may take (global queue or on a waiter's forward chain)
      if (waiters.length > 0 && (qq[0] > 0 || qRun > 0 || chainRunnable > 0)) cls.append("causality-stall ");
      if (lostNotify > 0) cls.append("lost-notify ");
      if (inversion > 0) cls.append("sync-stack-inversion ");
      if (promiseStuck > 0) cls.append("promise-stuck ");
      if (cls.length() == 0 && localRun > 0) cls.append("fenced-local-work ");
      if (cls.length() == 0) cls.append(waiters.length > 0 ? "other:waiters-nothing-runnable " : "other ");
    } catch (Throwable t) {
      System.out.println("hang classification threw " + t);
      t.printStackTrace(System.out);
      cls.append("other:classify-error ");
    }
    System.out.println("KV h4 " + HunterH4.summary());
    System.out.println((fatal ? "HANG-CLASS " : "STALL-CLASS ") + cls.toString().trim().replace(' ', '+'));
    System.out.flush();
    if (fatal) Runtime.getRuntime().halt(3);
    return cls.toString().trim().replace(' ', '+');
  }

  // ================================================================== child

  record Job(G g, int root, CountDownLatch done) {}

  static void child(long seed, String cfg) throws Exception {
    int maxN = Integer.getInteger("hunter.maxN", 0);
    long wdMs = Long.getLong("hunter.wdMs", 10_000);
    long budgetMs = Long.getLong("hunter.budgetMs", 2500);
    int maxIters = Integer.getInteger("hunter.iters", 200), minIters = Integer.getInteger("hunter.minIters", 3);
    Spec sp = generate(seed, maxN);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = (OGScheduler) EvaluationContext.scheduler();
    ScenarioStack base = EvaluationContext.scenarioStack();
    int sid = F_SID.getInt(s);
    System.out.println("KV seed=" + seed + " cfg=" + cfg + " ideal=" + F_IDEAL.getInt(s) + " limitThreads=" + Settings.limitThreads
        + " asserts=" + Settings.schedulerAsserts + " delayMaxNanos=" + Long.getLong("probes.delay.maxNanos", 0));
    System.out.println("KV spec " + sp.summary());
    System.out.println("KV hooks " + HunterHooks.instrumentedSummary());
    AtomicInteger extN = new AtomicInteger();
    ScheduledThreadPoolExecutor ext = new ScheduledThreadPoolExecutor(3, r -> {
      Thread t = new Thread(() -> {
        EvaluationContext.initializeWithoutRuntime();
        r.run();
      }, "hunter-ext-" + extN.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
    ext.setRemoveOnCancelPolicy(true);
    // persistent clients (thread ids stay small: no R1 overflow)
    List<ArrayBlockingQueue<Job>> qs = new ArrayList<>();
    for (int c = 0; c < sp.clients; c++) {
      ArrayBlockingQueue<Job> q = new ArrayBlockingQueue<>(1);
      qs.add(q);
      int ci = c;
      Thread t = new Thread(() -> {
        EvaluationContext.initializeWithoutRuntime();
        OGSchedulerContext ec = OGSchedulerContext.current();
        while (true) {
          Job j;
          try {
            j = q.take();
          } catch (InterruptedException e) {
            return;
          }
          try {
            ec.runAndWait(j.g.nodes[j.root]);
          } catch (Throwable e) {
            finding("CLIENT_THREW", "client " + ci + " " + e);
            e.printStackTrace(System.out);
          } finally {
            j.done.countDown();
          }
        }
      }, "hunter-client-" + c);
      t.setDaemon(true);
      t.start();
    }
    Acct before = settle(s, 500);
    int liveBefore = before.liveOg;
    List<Long> iterMs = new ArrayList<>();
    long t0 = System.nanoTime();
    int iter;
    List<String> h5 = new ArrayList<>();
    Acct last = before;
    for (iter = 0; iter < maxIters; iter++) {
      curIter = iter;
      if (iter >= minIters && (System.nanoTime() - t0) / 1_000_000 > budgetMs) break;
      G g = new G(sp, s, ext, base);
      Random ir = new Random(seed ^ (iter * 0x632BE59BD9B4E019L));
      List<ScheduledFuture<?>> cancels = new ArrayList<>();
      for (int k = 0; k < sp.scopeRoot.length; k++) {
        var cs = g.scopeSS[k].cancelScope();
        long d = (long) (sp.cancelDelayUs[k] * (0.25 + 1.5 * ir.nextDouble()));
        cancels.add(ext.schedule(() -> cs.cancel("hunter cancel"), d, TimeUnit.MICROSECONDS));
      }
      CountDownLatch done = new CountDownLatch(sp.clients);
      long ts = System.nanoTime();
      for (int c = 0; c < sp.clients; c++) {
        HN r = g.nodes[sp.clientRoot[c]];
        if (!r.enqueued) {
          r.enqueued = true;
          statEnqueued.incrementAndGet();
        }
        qs.get(c).put(new Job(g, sp.clientRoot[c], done));
      }
      long stallMs = Long.getLong("hunter.stallDumpMs", 600);
      if (stallMs > 0 && stallDumps < 2 && !done.await(stallMs, TimeUnit.MILLISECONDS)) {
        stallDumps++;
        snapshot(g, s, "slow iteration " + iter + " (non-fatal)", stallMs, false);
      }
      if (!done.await(wdMs, TimeUnit.MILLISECONDS)) hang(g, s, "clients not done (iter " + iter + ")", wdMs);
      long ms = (System.nanoTime() - ts) / 1_000_000;
      iterMs.add(ms);
      for (ScheduledFuture<?> f : cancels)
        if (!f.cancel(false)) {
          try {
            f.get(2, TimeUnit.SECONDS);
          } catch (Exception ignored) {
          }
        }
      long tw = System.nanoTime();
      while (g.extOutstanding.get() > 0 && (System.nanoTime() - tw) < 5_000_000_000L) Thread.sleep(1);
      if (g.extOutstanding.get() > 0) finding("EXT_OUTSTANDING", "" + g.extOutstanding.get());
      checkGraph(g);
      last = settle(s, 2000);
      List<String> v = last.violations();
      if (!v.isEmpty()) {
        String key = String.join(",", v.stream().map(x -> x.replaceAll("=.*", "")).toList());
        if (!h5.contains(key)) {
          h5.add(key);
          finding("H5", String.join(" ", v) + " | " + last);
        }
      }
    }
    long total = (System.nanoTime() - t0) / 1_000_000;
    List<Long> sorted = new ArrayList<>(iterMs.size() > 1 ? iterMs.subList(1, iterMs.size()) : iterMs); // iter 0 = JIT warm-up
    sorted.sort(null);
    long med = sorted.isEmpty() ? 0 : sorted.get(sorted.size() / 2), max = sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1);
    // slow iteration: timer-paced recovery (deadlock timer is 1 s)
    List<Integer> slowIdx = new ArrayList<>();
    for (int k = 1; k < iterMs.size(); k++) if (iterMs.get(k) >= 900) slowIdx.add(k);
    if (max >= 900 && max > 10 * Math.max(med, 1))
      finding("SLOW_ITER", "max=" + max + "ms median=" + med + "ms slowIters=" + slowIdx + " (1 s deadlock timer recovery?)");
    if (HunterHooks.timerLW.get() > 0) finding("TIMER_RECOVERED_LOST_WAKEUP", "count=" + HunterHooks.timerLW.get() + " first: " + HunterHooks.firstLW);
    Acct fin = settle(s, 2000);
    int liveAfter = ogWorkers(sid).size();
    System.out.println("KV summary iters=" + iter + " ms=" + total + " medIterMs=" + med + " maxIterMs=" + max + " enq=" + statEnqueued.get()
        + " promises=" + statPromises.get() + " extLaunches=" + statExtLaunches.get() + " seqReruns=" + statSeqReruns.get()
        + " failedLeaves=" + statFailedLeaves.get() + " cancelledScopes=" + statCancelledScopes.get() + " cancelledNodes=" + statCancelledNodes.get()
         + " timerLW=" + HunterHooks.timerLW.get() + " timerLWInFlight=" + HunterHooks.timerLWInFlight.get() + " timerCS=" + HunterHooks.timerCS.get() + " timerCalls=" + HunterHooks.timerCalls.get()
        + " maxSettleMs=" + maxSettleMs + " ogLiveBefore=" + liveBefore + " ogLiveAfter=" + liveAfter + " findings=" + findingCount.get());
    System.out.println("KV h5 " + fin + " violations=" + (fin.violations().isEmpty() ? "none" : String.join(",", fin.violations())));
    System.out.println("KV h4 " + HunterH4.summary());
    System.out.flush();
    int code = 0;
    boolean oracle = false, h5only = false;
    for (String f : findings) {
      if (f.startsWith("FINDING H5 ")) h5only = true;
      else if (!f.startsWith("FINDING SLOW_ITER") && !f.startsWith("FINDING TIMER_RECOVERED")) oracle = true;
    }
    if (oracle) code = 1;
    else if (h5only) code = 2;
    Runtime.getRuntime().halt(code);
  }

  // ================================================================== scenario INV (stale causality stamp)

  /**
   * Deterministic, unprimed reproducer for the stale-causality-stamp inversion found by the sweep
   * (seed 14). Causality ids are sync-stack depths, not frame identities: a node stamped (T, d) by a
   * frame at depth d that has since been popped is accepted by any later, unrelated frame of T at
   * depth <= d. Single client T:
   *
   * <pre>
   *   T: runAndWait(R)                                   frame 1 (causality 0: StartNode)
   *   R.run: runAndWait(A)                               frame 2, causality 1
   *     A.run: runAndWait(Q)                             frame 3, causality 2
   *       Q.run: enqueue Z; complete                     Z drained in that frame: stamped (T,2)
   *       Z.run#1: enqueue P1 (promise, 100 ms); continueWith(P1)   P1 handed to an external thread
   *     A completes; frames 3 and 2 pop
   *   R.run: runAndWait(S)                               frame 2, causality 1
   *     S.run: enqueue P2 (promise, 300 ms); runAndWait(P2)        frame 3, T parks at causality 2
   *   +100 ms  ext: P1.complete(scheduler) -> Z.onChildCompleted -> scheduler.enqueue(Z)
   *            canTake(Z=(T,2), T, 2) is true -> wakeupThread hands Z to T inside S's frame
   *   T: Z.run#2: runAndWait(S)                          frame 4: waits on S, which is suspended in frame 3 below
   *   +300 ms  ext: P2 completes; S's frame is woken only if T's awaitedTask == P2 (it is S): no wake
   * </pre>
   *
   * Permanent hang: no cycle in the graph (cycle detection finds nothing), no runnable work.
   */
  static void inv() throws Exception {
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = (OGScheduler) EvaluationContext.scheduler();
    ScenarioStack ss = EvaluationContext.scenarioStack();
    ScheduledThreadPoolExecutor ext = new ScheduledThreadPoolExecutor(1, r -> {
      Thread t = new Thread(r, "hunter-ext-inv");
      t.setDaemon(true);
      return t;
    });
    long p1ms = Long.getLong("inv.p1ms", 100), p2ms = Long.getLong("inv.p2ms", 300);
    final class P extends NodeTask {
      final String name;
      final long ms;
      P(String name, long ms) {
        this.name = name;
        this.ms = ms;
        attach(ss);
      }
      @Override
      public void run(OGSchedulerContext ec) {
        ext.schedule(() -> complete(s), ms, TimeUnit.MILLISECONDS); // external callback, eq = scheduler (as NodePromise does)
      }
      @Override
      public String toString() {
        return name;
      }
    }
    final class Fn extends NodeTask {
      final String name;
      final java.util.function.BiConsumer<Fn, OGSchedulerContext> body;
      int step;
      Fn(String name, java.util.function.BiConsumer<Fn, OGSchedulerContext> body) {
        this.name = name;
        this.body = body;
        attach(ss);
      }
      @Override
      public void run(OGSchedulerContext ec) {
        System.out.println("INV run " + name + " step=" + step + " on " + Thread.currentThread().getName()
            + " causality=" + causalityThreadID() + "/" + causalityID());
        body.accept(this, ec);
      }
      void done(EvaluationQueue eq) {
        complete(eq);
      }
      @Override
      public String toString() {
        return name;
      }
    }
    P p1 = new P("P1", p1ms), p2 = new P("P2", p2ms);
    Fn[] S = new Fn[1];
    S[0] = new Fn("S", (n, ec) -> {
      ec.enqueue(n, p2);
      ec.runAndWait(p2);
      n.done(ec);
    });
    Fn z0 = new Fn("Z", (n, ec) -> {
      if (n.step++ == 0) {
        ec.enqueue(n, p1);
        p1.continueWith(n, ec);
      } else {
        ec.runAndWait(S[0]); // Z's continuation needs S (shared node)
        n.done(ec);
      }
    });
    Fn q = new Fn("Q", (n, ec) -> {
      ec.enqueue(n, z0);
      n.done(ec);
    });
    Fn a = new Fn("A", (n, ec) -> {
      ec.runAndWait(q);
      n.done(ec);
    });
    Fn r0 = new Fn("R", (n, ec) -> {
      ec.runAndWait(a);
      ec.enqueue(n, S[0]);
      ec.runAndWait(S[0]);
      n.done(ec);
    });
    Fn r = r0, z = z0;
    if (Boolean.getBoolean("inv.n2")) {
      // N2: R (sync) -> runAndWait(A); A.run hands K to a non-graph thread which calls
      // scheduler.evaluateNodeAsync(K) (poisonCausality) and K.continueWith(A). T waits at causality 1,
      // canTake(poisoned K) is false for it, and limitThreads counts T as active: no helper is injected.
      Fn kk = new Fn("K", (n, ec) -> n.done(ec));
      AtomicInteger pend = new AtomicInteger(2);
      // A2 waits on K (launched externally, poisoned) and on L (a slow promise). L is registered last,
      // so A2.waitingOn = L and the forward-chain walk in wait()/fullWait never reaches K.
      final class A2 extends NodeTask {
        A2() {
          attach(ss);
        }
        @Override
        public void run(OGSchedulerContext ec) {
          System.out.println("INV run A2 on " + Thread.currentThread().getName() + " causality=" + causalityThreadID() + "/" + causalityID());
          CountDownLatch l = new CountDownLatch(1);
          NodeTask self = this;
          ext.execute(() -> {
            s.evaluateNodeAsync(kk);
            kk.continueWith(self, s);
            l.countDown();
          });
          try {
            l.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          ec.enqueue(this, p2);
          p2.continueWith(this, ec);
        }
        @Override
        protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
          if (pend.decrementAndGet() == 0) complete(eq);
        }
        @Override
        public String toString() {
          return "A2";
        }
      }
      NodeTask aa = new A2();
      r = new Fn("R2", (n, ec) -> {
        ec.runAndWait(aa);
        n.done(ec);
      });
      z = kk;
      S[0] = new Fn("A2", (n, ec) -> {}); // placeholder for the hang printout

    }
    final Fn rr = r, zz = z;
     System.out.println("KV inv n2=" + Boolean.getBoolean("inv.n2") + " ideal=" + F_IDEAL.getInt(s) + " limitThreads=" + Settings.limitThreads + " asserts=" + Settings.schedulerAsserts);
    CountDownLatch done = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      EvaluationContext.initializeWithoutRuntime();
      OGSchedulerContext.current().runAndWait(rr);
      done.countDown();
    }, "hunter-client-inv");
    t.setDaemon(true);
    t.start();
    long wd = Long.getLong("hunter.wdMs", 10_000);
    if (!done.await(wd, TimeUnit.MILLISECONDS)) {
      System.out.println("INV-HANG " + zz + ".done=" + zz.isDone() + " " + S[0] + ".done=" + S[0].isDone() + " P1.done=" + p1.isDone() + " P2.done=" + p2.isDone()
          + " " + zz + ".causality=" + zz.causalityThreadID() + "/" + zz.causalityID());
      for (StackTraceElement e : t.getStackTrace()) if (e.getClassName().startsWith("optimus.graph")) System.out.println("    " + e);
      System.out.println(s.toString());
      System.out.println("KV h4 " + HunterH4.summary());
      System.out.flush();
      Runtime.getRuntime().halt(3);
    }
    System.out.println("KV h4 " + HunterH4.summary());
    System.out.println("KV inv completed (no hang)");
    System.out.flush();
    Runtime.getRuntime().halt(0);
  }


  // ================================================================== H10 soak (one long-lived JVM, fresh seeds, thread churn)

  static final Field F_TC = field(OGScheduler.class, "threadCounter");

  /** Max WorkQueue._top over every context the hunter knows (clients, workers, waiters, idle) and the global quick queue. */
  static int maxTop(OGScheduler s) throws Exception {
    int m = F_WQ_TOP.getInt(F_QQ.get(s));
    List<OGScheduler.Context> cs = new ArrayList<>();
    for (Field f : new Field[] {F_WORKERS, F_WAITERS, F_IDLE}) cs.addAll(Arrays.asList(contexts(s, f)));
    for (OGSchedulerContext sc : HunterH4.byTid.values()) cs.add(HunterH4.ctxOf(sc));
    for (OGScheduler.Context c : cs) if (c != null) m = Math.max(m, F_WQ_TOP.getInt(c.queue));
    return m;
  }

  static long leakedClients;
  static boolean hungSeed;
  static final java.lang.reflect.Method M_INJECT;

  static {
    try {
      M_INJECT = OGScheduler.class.getDeclaredMethod("injectThread", NodeTask.class);
      M_INJECT.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void soakChild(long firstSeed, String cfg, long durationSec) throws Exception {
    long wdMs = Long.getLong("hunter.wdMs", 10_000);
    long budgetMs = Long.getLong("hunter.budgetMs", 2000);
    int maxIters = Integer.getInteger("hunter.iters", 200), minIters = Integer.getInteger("hunter.minIters", 3);
    EvaluationContext.initializeWithoutRuntime();
    OGScheduler s = (OGScheduler) EvaluationContext.scheduler();
    ScenarioStack base = EvaluationContext.scenarioStack();
    int sid = F_SID.getInt(s);
    System.out.println("KV soak firstSeed=" + firstSeed + " cfg=" + cfg + " ideal=" + F_IDEAL.getInt(s) + " limitThreads=" + Settings.limitThreads
        + " asserts=" + Settings.schedulerAsserts + " delayMaxNanos=" + Long.getLong("probes.delay.maxNanos", 0) + " threadCounter=" + F_TC.get(s));
    AtomicInteger extN = new AtomicInteger();
    ScheduledThreadPoolExecutor ext = new ScheduledThreadPoolExecutor(3, r -> {
      Thread t = new Thread(() -> {
        EvaluationContext.initializeWithoutRuntime();
        r.run();
      }, "hunter-ext-" + extN.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
    ext.setRemoveOnCancelPolicy(true);
    long t0 = System.nanoTime(), lastStat = 0;
    for (long seed = firstSeed; (System.nanoTime() - t0) / 1_000_000_000L < durationSec; seed++) {
      Spec sp = generate(seed, 0);
      findingCount.set(0); // the 200-line cap applies per seed here
      int f0 = 0;
      findings.clear();
      // fresh client threads for every seed: a request pool that churns threads (each one initialises an
      // EvaluationContext and takes a new id from threadCounter, as production threads do)
      List<ArrayBlockingQueue<Job>> qs = new ArrayList<>();
      List<Thread> clients = new ArrayList<>();
      for (int c = 0; c < sp.clients; c++) {
        ArrayBlockingQueue<Job> q = new ArrayBlockingQueue<>(1);
        qs.add(q);
        int ci = c;
        Thread t = new Thread(() -> {
          EvaluationContext.initializeWithoutRuntime();
          OGSchedulerContext ec = OGSchedulerContext.current();
          while (true) {
            Job j;
            try {
              j = q.take();
            } catch (InterruptedException e) {
              return;
            }
            try {
              ec.runAndWait(j.g.nodes[j.root]);
            } catch (Throwable e) {
              finding("CLIENT_THREW", "client " + ci + " " + e);
            } finally {
              j.done.countDown();
            }
          }
        }, "hunter-client-" + c);
        t.setDaemon(true);
        t.start();
        clients.add(t);
      }
      long ts0 = System.nanoTime(), maxMs = 0;
      boolean rescuedSeed = false;
      int iter;
      List<String> h5 = new ArrayList<>();
      for (iter = 0; iter < maxIters; iter++) {
        curIter = iter;
        if (iter >= minIters && (System.nanoTime() - ts0) / 1_000_000 > budgetMs) break;
        G g = new G(sp, s, ext, base);
        Random ir = new Random(seed ^ (iter * 0x632BE59BD9B4E019L));
        List<ScheduledFuture<?>> cancels = new ArrayList<>();
        for (int k = 0; k < sp.scopeRoot.length; k++) {
          var cs = g.scopeSS[k].cancelScope();
          long d = (long) (sp.cancelDelayUs[k] * (0.25 + 1.5 * ir.nextDouble()));
          cancels.add(ext.schedule(() -> cs.cancel("hunter cancel"), d, TimeUnit.MICROSECONDS));
        }
        CountDownLatch done = new CountDownLatch(sp.clients);
        long ts = System.nanoTime();
        for (int c = 0; c < sp.clients; c++) {
          g.nodes[sp.clientRoot[c]].enqueued = true;
          qs.get(c).put(new Job(g, sp.clientRoot[c], done));
        }
        if (!done.await(wdMs, TimeUnit.MILLISECONDS)) {
          String hc = snapshot(g, s, "soak seed " + seed + " clients not done (iter " + iter + ")", wdMs, false);
          // Rescue attempt = R10's fix direction: inject one causality-0 worker above the ideal count. A
          // causality stall recovers; a sync-stack deadlock (R9) does not, its clients are abandoned.
          synchronized (F_LOCK.get(s)) {
            M_INJECT.invoke(s, (Object) null);
          }
          boolean rescued = done.await(5, TimeUnit.SECONDS);
          long left = done.getCount();
          if (!rescued) leakedClients += left;
          System.out.println("SOAK-RESULT seed=" + seed + " class=" + (rescued ? "HANG-RESCUED:" : "HANG:") + hc + " iters=" + iter + " abandonedClients=" + (rescued ? 0 : left)
              + " leakedTotal=" + leakedClients + " findings=" + String.join(",", findingKinds()));
          soakStat(s, sid, t0, seed, true);
          System.out.flush();
          if (leakedClients > Integer.getInteger("hunter.soakMaxLeaked", 4)) {
            System.out.println("SOAK-RESTART leakedClients=" + leakedClients);
            System.out.flush();
            Runtime.getRuntime().halt(3);
          }
          if (!rescued) {
            hungSeed = true;
            break;
          }
          checkGraph(g);
          rescuedSeed = true;
          break; // next seed (the same seed would stall again every iteration)
        }
        maxMs = Math.max(maxMs, (System.nanoTime() - ts) / 1_000_000);
        for (ScheduledFuture<?> f : cancels)
          if (!f.cancel(false)) {
            try {
              f.get(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
          }
        long tw = System.nanoTime();
        while (g.extOutstanding.get() > 0 && (System.nanoTime() - tw) < 5_000_000_000L) Thread.sleep(1);
        if (g.extOutstanding.get() > 0) finding("EXT_OUTSTANDING", "" + g.extOutstanding.get());
        checkGraph(g);
        Acct a = settle(s, 2000);
        List<String> v = a.violations();
        if (!v.isEmpty()) {
          String key = String.join(",", v.stream().map(x -> x.replaceAll("=.*", "")).toList());
          if (!h5.contains(key)) {
            h5.add(key);
            finding("H5", String.join(" ", v) + " | " + a);
          }
        }
      }
      for (Thread t : clients) t.interrupt();
      if (!hungSeed) for (Thread t : clients) t.join(2000);
      HunterH4.purge();
      if (hungSeed) {
        hungSeed = false;
        continue;
      }
      List<String> fk = findingKinds();
      boolean oracle = fk.stream().anyMatch(k -> !k.equals("H5") && !k.equals("SLOW_ITER") && !k.equals("TIMER_RECOVERED_LOST_WAKEUP"));
      String cls = rescuedSeed ? "RESCUED-SEED" : oracle ? "ORACLE" : fk.contains("H5") ? "H5" : maxMs >= 900 ? "PASS-SLOW" : "PASS";
      System.out.println("SOAK-RESULT seed=" + seed + " class=" + cls + " iters=" + iter + " maxIterMs=" + maxMs + " N=" + sp.n + " clients=" + sp.clients
          + " findings=" + String.join(",", fk) + " newFindings=" + (findingCount.get() - f0));
      if ((System.nanoTime() - t0) / 1_000_000_000L - lastStat >= 30) {
        lastStat = (System.nanoTime() - t0) / 1_000_000_000L;
        soakStat(s, sid, t0, seed, false);
      }
    }
    soakStat(s, sid, t0, -1, false);
    System.out.println("SOAK-END");
    System.out.flush();
    Runtime.getRuntime().halt(0);
  }

  static List<String> findingKinds() {
    List<String> fk = new ArrayList<>();
    for (String f : findings) {
      String k = f.split(" ")[1];
      if (!fk.contains(k)) fk.add(k);
    }
    return fk;
  }

  static void soakStat(OGScheduler s, int sid, long t0, long seed, boolean hung) {
    try {
      if (!hung) {
        System.gc();
        System.gc();
      }
      Runtime rt = Runtime.getRuntime();
      long heap = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
      int cb = ((java.util.PriorityQueue<?>) F_CB.get(s)).size();
      int q = ((java.util.PriorityQueue<?>) F_Q.get(s)).size();
      int tracked = ((optimus.core.IndexedArrayList<?>) F_TRACKED.get(s)).count;
      int idle = ((OGScheduler.Contexts) F_IDLE.get(s)).count;
      int live = Thread.getAllStackTraces().size();
      System.out.println("SOAKSTAT upSec=" + (System.nanoTime() - t0) / 1_000_000_000L + " seed=" + seed + " heapMB" + (hung ? "(noGC)" : "AfterGC") + "=" + heap
          + " ogLive=" + ogWorkers(sid).size() + " idle=" + idle + " liveThreads=" + live + " onStalledCallbacks=" + cb + " queue=" + q + " tracked=" + tracked
          + " threadCounter=" + F_TC.get(s) + " maxTop=" + maxTop(s) + " h4ctx=" + HunterH4.byTid.size() + " " + HunterH4.summary().replace(' ', ','));
    } catch (Throwable t) {
      System.out.println("SOAKSTAT error " + t);
    }
  }

  /** Parent: one driver per cfg; restarts the soak JVM at the next seed after a hang. */
  static void soak(long firstSeed, int minutes, List<String> cfgs) throws Exception {
    long deadline = System.nanoTime() + minutes * 60_000_000_000L, t0 = System.nanoTime();
    String cmd = System.getenv("HUNTER_JAVA_CMD");
    record Win(int[] counts, Map<String, Integer> cls, String[] lastStat) {}
    Map<String, List<Win>> wins = new java.util.concurrent.ConcurrentHashMap<>();
    List<Thread> drivers = new ArrayList<>();
    for (String cfg : cfgs) {
      List<Win> w = java.util.Collections.synchronizedList(new ArrayList<>());
      wins.put(cfg, w);
      Thread d = new Thread(() -> {
        long seed = firstSeed;
        int segment = 0;
        try {
          while (System.nanoTime() < deadline) {
            long rem = (deadline - System.nanoTime()) / 1_000_000_000L;
            if (rem < 5) break;
            List<String> argv = new ArrayList<>(List.of("java"));
            for (String f : flags(cfg).split(" ")) if (!f.isEmpty()) argv.add(f);
            for (String k : new String[] {"hunter.budgetMs", "hunter.iters", "hunter.minIters", "hunter.wdMs", "hunter.h4"})
              if (System.getProperty(k) != null) argv.add("-D" + k + "=" + System.getProperty(k));
            for (String a : cmd.substring(5).trim().split("\\s+")) argv.add(a);
            argv.addAll(List.of("--soak", Long.toString(seed), cfg, Long.toString(rem)));
            Path log = logDir().resolve("soak-" + cfg + "-seg" + segment + "-s" + seed + ".log");
            Process p = new ProcessBuilder(argv).redirectErrorStream(true).start();
            long lastSeed = seed - 1;
            try (var out = Files.newBufferedWriter(log); var in = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))) {
              String l;
              while ((l = in.readLine()) != null) {
                out.write(l);
                out.newLine();
                int wi = (int) ((System.nanoTime() - t0) / 600_000_000_000L);
                while (w.size() <= wi) w.add(new Win(new int[4], new TreeMap<>(), new String[1]));
                Win cur = w.get(wi);
                if (l.startsWith("SOAK-RESULT ")) {
                  Map<String, String> kv = kvs(List.of("KV " + l.substring(12)));
                  lastSeed = Long.parseLong(kv.get("seed"));
                  String c = kv.get("class");
                  synchronized (cur) {
                    cur.counts[c.startsWith("PASS") ? 0 : c.startsWith("HANG:") ? 1 : c.startsWith("HANG-RESCUED") ? 3 : 2]++;
                    cur.cls.merge(c + (c.startsWith("PASS") || kv.get("findings") == null || kv.get("findings").isEmpty() ? "" : "{" + kv.get("findings") + "}"), 1, Integer::sum);
                  }
                  synchronized (System.out) {
                    System.out.println("RESULT soak cfg=" + cfg + " seg=" + segment + " " + l.substring(12));
                  }
                } else if (l.startsWith("SOAKSTAT ")) {
                  cur.lastStat[0] = "seg=" + segment + " " + l.substring(9);
                  synchronized (System.out) {
                    System.out.println("STAT soak cfg=" + cfg + " seg=" + segment + " " + l.substring(9));
                  }
                } else if (l.startsWith("HANG-CLASS") || l.startsWith("STALL-CLASS") || l.startsWith("H4 SELF_WAIT where") || l.startsWith("H4 WAIT_CYCLE")
                    || l.startsWith("FINDING ") || l.startsWith("CHILD-ERROR")) {
                  synchronized (System.out) {
                    System.out.println("EVENT soak cfg=" + cfg + " seg=" + segment + " seed=" + (lastSeed + 1) + " " + (l.length() > 400 ? l.substring(0, 400) : l));
                  }
                }
              }
            }
            p.waitFor();
            if (p.exitValue() != 0 && p.exitValue() != 3) synchronized (System.out) {
              System.out.println("EVENT soak cfg=" + cfg + " seg=" + segment + " exit=" + p.exitValue());
            }
            seed = Math.max(seed, lastSeed + 1);
            segment++;
          }
        } catch (Exception e) {
          e.printStackTrace(System.out);
        }
      }, "soak-" + cfg);
      d.start();
      drivers.add(d);
    }
    // per-10-minute window summary, printed as windows close
    int printed = 0;
    while (drivers.stream().anyMatch(Thread::isAlive)) {
      Thread.sleep(5000);
      int closed = (int) ((System.nanoTime() - t0) / 600_000_000_000L);
      for (; printed < closed; printed++) printWindow(wins, printed);
    }
    int total = (int) ((System.nanoTime() - t0) / 600_000_000_000L) + 1;
    for (; printed < total; printed++) printWindow(wins, printed);
  }

  static void printWindow(Map<String, ? extends List<?>> wins, int wi) {
    for (var e : new TreeMap<>(wins).entrySet()) {
      List<?> w = e.getValue();
      if (w.size() <= wi) continue;
      Object o = w.get(wi);
      try {
        int[] c = (int[]) o.getClass().getMethod("counts").invoke(o);
        Object cls = o.getClass().getMethod("cls").invoke(o);
        String[] st = (String[]) o.getClass().getMethod("lastStat").invoke(o);
        System.out.println("SOAK-WINDOW cfg=" + e.getKey() + " minutes=" + (wi * 10) + "-" + (wi * 10 + 10) + " pass=" + c[0] + " hang=" + c[1] + " hangRescued=" + c[3] + " other=" + c[2]
            + " classes=" + cls + " lastStat=[" + st[0] + "]");
      } catch (Exception x) {
        System.out.println("SOAK-WINDOW error " + x);
      }
    }
    System.out.flush();
  }

  // ================================================================== parent

  static String flags(String cfg) {
    // i{ideal}-a{0|1}-d{0|1}-lt{0|1}
    String[] p = cfg.split("-");
    String ideal = p[0].substring(1), a = p[1].substring(1), d = p[2].substring(1), lt = p[3].substring(2);
    return "-Doptimus.gthread.ideal=" + ideal + " -Doptimus.graph.limitThreads=" + (lt.equals("1")) + " -Doptimus.scheduler.asserts="
        + a.equals("1") + (d.equals("1") ? " -Dprobes.delay.maxNanos=" + System.getProperty("hunter.delayNanos", "50000") : "")
        + (d.equals("1") && System.getProperty("hunter.delayPoints") != null ? " -Dprobes.delay.points=" + System.getProperty("hunter.delayPoints") : "");
  }

  static List<String> configs(long seed) {
    List<String> r = new ArrayList<>();
    for (int ideal : new int[] {1, 2, 4}) for (int a = 0; a <= 1; a++) for (int d = 0; d <= 1; d++) r.add("i" + ideal + "-a" + a + "-d" + d + "-lt1");
    if (seed % 3 == 0) {
      r.add("i2-a1-d1-lt0");
      r.add("i4-a0-d1-lt0");
    }
    return r;
  }

  static final String BASELINE = "i4-a0-d0-lt1";

  record Res(long seed, String cfg, int maxN, int exit, String cls, String detail, long wallMs, String log, Map<String, String> kv) {}

  static Path logDir() throws Exception {
    Path p = Path.of(System.getenv().getOrDefault("HUNTER_LOGDIR", "/tmp/hunter"));
    Files.createDirectories(p);
    return p;
  }

  static final AtomicInteger runCounter = new AtomicInteger();

  static Res runChild(long seed, String cfg, int maxN, long wdMs, String tag) throws Exception {
    String cmd = System.getenv("HUNTER_JAVA_CMD");
    if (cmd == null || !cmd.startsWith("java ")) throw new IllegalStateException("HUNTER_JAVA_CMD not set; use run-hunter.sh");
    List<String> argv = new ArrayList<>();
    argv.add("java");
    for (String f : flags(cfg).split(" ")) if (!f.isEmpty()) argv.add(f);
    argv.add("-Dhunter.wdMs=" + wdMs);
    if (maxN > 0) argv.add("-Dhunter.maxN=" + maxN);
    for (String k : new String[] {"hunter.budgetMs", "hunter.iters", "hunter.minIters", "hunter.asyncMode", "hunter.stallDumpMs"})
      if (System.getProperty(k) != null) argv.add("-D" + k + "=" + System.getProperty(k));
    for (String a : cmd.substring(5).trim().split("\\s+")) argv.add(a);
    argv.add("--child");
    argv.add(Long.toString(seed));
    argv.add(cfg);
    Path log = logDir().resolve("s" + seed + "-" + cfg + (maxN > 0 ? "-n" + maxN : "") + (tag.isEmpty() ? "" : "-" + tag) + ".log");
    ProcessBuilder pb = new ProcessBuilder(argv).redirectErrorStream(true).redirectOutput(log.toFile());
    long t0 = System.nanoTime();
    Process p = pb.start();
    boolean fin = p.waitFor(wdMs / 1000 + 60, TimeUnit.SECONDS);
    if (!fin) {
      p.destroyForcibly().waitFor();
    }
    long wall = (System.nanoTime() - t0) / 1_000_000;
    runCounter.incrementAndGet();
    List<String> lines = Files.readAllLines(log, java.nio.charset.StandardCharsets.ISO_8859_1);
    return classify(seed, cfg, maxN, fin ? p.exitValue() : -1, lines, wall, log.toString());
  }

  static Map<String, String> kvs(List<String> lines) {
    Map<String, String> m = new LinkedHashMap<>();
    for (String l : lines)
      if (l.startsWith("KV ")) for (String t : l.substring(3).split(" ")) {
        int e = t.indexOf('=');
        if (e > 0 && t.startsWith("h4")) m.put(t.substring(0, e), t.substring(e + 1)); // last snapshot wins
        else if (e > 0) m.putIfAbsent(t.substring(0, e), t.substring(e + 1));
      }
    return m;
  }

  static Res classify(long seed, String cfg, int maxN, int exit, List<String> lines, long wall, String log) {
    Map<String, String> kv = kvs(lines);
    boolean infoDumper = false, invalid = false, twice = false, panic = false;
    String invalidMsg = null;
    List<String> fk = new ArrayList<>();
    String hangCls = null, stallCls = null;
    for (String l : lines) {
      if (l.contains("ExceptionInInitializerError") || (l.contains("InfoDumper") && l.contains("Exception"))) infoDumper = true;
      if (l.contains("GraphInInvalidState") && !l.startsWith("\tat") && !l.startsWith("    at")) {
        invalid = true;
        if (invalidMsg == null) invalidMsg = l.trim();
      }
      if (l.contains("Cannot complete node twice")) twice = true;
      if (l.contains("panic called") || l.contains("exiting (121)")) panic = true;
      if (l.startsWith("FINDING ")) {
        String k = l.split(" ")[1];
        if (!fk.contains(k)) fk.add(k);
      }
      if (l.startsWith("HANG-CLASS ")) hangCls = l.substring(11).trim();
      if (l.startsWith("STALL-CLASS ") && stallCls == null) stallCls = l.substring(12).trim();
    }
    String cls;
    StringBuilder det = new StringBuilder();
    if (exit == 3) cls = "HANG:" + (hangCls == null ? "?" : hangCls);
    else if (exit == -1) cls = "HANG:unresponsive";
    else if (exit == 0) cls = "PASS";
    else if (exit == 1) cls = "ORACLE";
    else if (exit == 2) cls = "H5";
    else cls = "CRASH(exit=" + exit + ")";
    if (!fk.isEmpty()) det.append(" findings=").append(String.join(",", fk));
    if (invalid) {
      cls = cls.equals("PASS") ? "ASSERT" : cls + "+ASSERT";
      det.append(" invalidState=\"").append(invalidMsg.length() > 160 ? invalidMsg.substring(0, 160) : invalidMsg).append('"');
    }
    if (twice) cls += "+DOUBLE_COMPLETE";
    if (panic) cls += "+PANIC";
    if (infoDumper && !cls.equals("PASS")) cls += "+F1b/F2b(InfoDumper)";
    if (cls.equals("PASS") && (fk.contains("SLOW_ITER") || fk.contains("TIMER_RECOVERED_LOST_WAKEUP"))) cls = "PASS-SLOW" + (stallCls != null ? "(" + stallCls + ")" : "");
    return new Res(seed, cfg, maxN, exit, cls, det.toString(), wall, log, kv);
  }

  static String h4brief(Map<String, String> k) {
    if (k.get("h4on") == null) return "h4=[n/a]";
    long take = 0, stale = 0, same = 0, popped = 0, r9 = 0, unk = 0, alias = 0;
    StringBuilder per = new StringBuilder();
    for (String p : new String[] {"wakeup", "findWork", "run"}) {
      take += Long.parseLong(k.getOrDefault("h4take." + p, "0"));
      long st = Long.parseLong(k.getOrDefault("h4stale." + p, "0"));
      stale += st;
      same += Long.parseLong(k.getOrDefault("h4sameDepth." + p, "0"));
      popped += Long.parseLong(k.getOrDefault("h4popped." + p, "0"));
      r9 += Long.parseLong(k.getOrDefault("h4r9AtTake." + p, "0"));
      unk += Long.parseLong(k.getOrDefault("h4unknown." + p, "0"));
      alias += Long.parseLong(k.getOrDefault("h4aliased." + p, "0"));
      per.append(p).append(':').append(st).append('/').append(k.getOrDefault("h4take." + p, "0")).append(',');
    }
    return "h4=[staleTake=" + stale + " sameDepth=" + same + " popped=" + popped + " r9AtTake=" + r9 + " selfWait=" + k.get("h4selfWait.wait") + "+" + k.get("h4selfWait.timer") + " waitCycle=" + k.get("h4waitCycle.timer")
        + " aliased=" + alias + " unknown=" + unk + " stale/take=" + per.substring(0, per.length() - 1) + "]";
  }

  static String resultLine(Res r) {
    Map<String, String> k = r.kv;
    return String.format("RESULT seed=%d cfg=%s N=%s class=%s exit=%d wallMs=%d iters=%s maxIterMs=%s medIterMs=%s timerLW=%s timerCS=%s cancelledNodes=%s promises=%s extLaunches=%s h5=[%s] " + h4brief(k).replace("%", "%%") + "%s spec=[%s] log=%s",
        r.seed, r.cfg, k.getOrDefault("N", "?"), r.cls, r.exit, r.wallMs, k.get("iters"), k.get("maxIterMs"), k.get("medIterMs"),
        k.get("timerLW"), k.get("timerCS"), k.get("cancelledNodes"), k.get("promises"), k.get("extLaunches"),
        "workers=" + k.get("workers") + ",waiters=" + k.get("waiters") + ",idle=" + k.get("idle") + ",spin=" + k.get("spin") + ",qq=" + k.get("qqRun/Tot")
            + ",q=" + k.get("qRun/Tot") + ",tracked=" + k.get("tracked") + ",bad=" + k.get("badState") + ",og=" + k.get("ogLiveBefore") + "->" + k.get("ogLiveAfter")
            + ",viol=" + k.get("violations"),
        r.detail, "levels=" + k.get("levels") + " shared=" + k.get("sharedNodes") + " clients=" + k.get("clients") + " scopes=" + k.get("scopes")
            + " failEvery=" + k.get("failEvery") + " kinds=" + k.get("kinds"),
        Path.of(r.log).getFileName());
  }

  static boolean failed(Res r) {
    return !r.cls.equals("PASS");
  }

  static String family(String cls) {
    return cls.replaceAll("\\+F1b/F2b\\(InfoDumper\\)", "");
  }

  static void sweep(long first, int count) throws Exception {
    int par = Integer.parseInt(System.getenv().getOrDefault("HUNTER_PAR", "4"));
    ExecutorService pool = Executors.newFixedThreadPool(par);
    long t0 = System.nanoTime();
    // 1. baselines (unperturbed) to set the watchdog per seed
    Map<Long, Future<Res>> base = new TreeMap<>();
    for (long sd = first; sd < first + count; sd++) {
      long seed = sd;
      base.put(seed, pool.submit(() -> runChild(seed, BASELINE, 0, 30_000, "")));
    }
    Map<Long, Long> wd = new TreeMap<>();
    List<Res> all = new ArrayList<>();
    for (var e : base.entrySet()) {
      Res r = e.getValue().get();
      all.add(r);
      System.out.println(resultLine(r));
      long mx = r.kv.get("maxIterMs") == null ? 1000 : Long.parseLong(r.kv.get("maxIterMs"));
      wd.put(e.getKey(), Math.max(10_000, 10 * mx));
    }
    // 2. the rest of the matrix
    List<Future<Res>> fs = new ArrayList<>();
    for (long sd = first; sd < first + count; sd++) {
      long seed = sd;
      for (String cfg : configs(seed)) {
        if (cfg.equals(BASELINE)) continue;
        fs.add(pool.submit(() -> runChild(seed, cfg, 0, wd.get(seed), "")));
      }
    }
    for (Future<Res> f : fs) {
      Res r = f.get();
      all.add(r);
      System.out.println(resultLine(r));
      System.out.flush();
    }
    long sweepMs = (System.nanoTime() - t0) / 1_000_000;
    // 3. summary by configuration
    System.out.println("==== SUMMARY runs=" + all.size() + " sweepMs=" + sweepMs);
    Map<String, int[]> byCfg = new TreeMap<>();
    Map<String, Integer> byCls = new TreeMap<>();
    for (Res r : all) {
      int[] c = byCfg.computeIfAbsent(r.cfg, k -> new int[2]);
      c[failed(r) ? 1 : 0]++;
      byCls.merge(r.cls, 1, Integer::sum);
    }
    byCfg.forEach((k, v) -> System.out.println("SUMMARY cfg=" + k + " pass=" + v[0] + " fail=" + v[1]));
    byCls.forEach((k, v) -> System.out.println("SUMMARY class=" + k + " runs=" + v));
    // 4. rerun failures 5x (distinct seed+cfg), then minimise one representative per (class family)
    List<Res> fails = all.stream().filter(Hunter::failed).toList();
    Map<String, Res> reps = new LinkedHashMap<>();
    List<Future<String>> reruns = new ArrayList<>();
    for (Res r : fails) {
      reruns.add(pool.submit(() -> {
        int nf = 0;
        List<String> cl = new ArrayList<>();
        for (int k = 0; k < 5; k++) {
          Res x = runChild(r.seed, r.cfg, 0, Math.max(r.cls.startsWith("HANG") ? 10_000 : 0, wd.get(r.seed)), "rerun" + k);
          if (failed(x)) nf++;
          cl.add(x.cls);
        }
        return "RERUN seed=" + r.seed + " cfg=" + r.cfg + " orig=" + r.cls + " failed=" + nf + "/5 classes=" + String.join(";", cl);
      }));
      reps.putIfAbsent(family(r.cls), r);
    }
    for (Future<String> f : reruns) System.out.println(f.get());
    List<Future<String>> mins = new ArrayList<>();
    for (Res r : reps.values()) mins.add(pool.submit(() -> minimise(r, wd.get(r.seed))));
    for (Future<String> f : mins) System.out.println(f.get());
    System.out.println("==== DONE totalMs=" + (System.nanoTime() - t0) / 1_000_000 + " childRuns=" + runCounter.get());
    pool.shutdown();
  }

  static String minimise(Res r, long wdMs) throws Exception {
    int n = Integer.parseInt(r.kv.getOrDefault("N", "2000"));
    int best = n;
    String fam = family(r.cls);
    String bestCls = r.cls;
    int cand = n / 2;
    while (cand >= 2) {
      boolean hit = false;
      for (int k = 0; k < 3 && !hit; k++) {
        Res x = runChild(r.seed, r.cfg, cand, wdMs, "min" + k);
        if (failed(x) && family(x.cls).split(":")[0].equals(fam.split(":")[0])) {
          hit = true;
          bestCls = x.cls;
        }
      }
      if (!hit) break;
      best = cand;
      cand = cand / 2;
    }
    // refine between best/2 and best linearly-ish (a few steps)
    return "MIN seed=" + r.seed + " cfg=" + r.cfg + " origClass=" + r.cls + " origN=" + n + " minN=" + best + " minClass=" + bestCls
        + " repro: probes/run-hunter.sh one " + r.seed + " " + r.cfg + " " + best + " 5";
  }

  static void one(long seed, String cfg, int maxN, int reps) throws Exception {
    int fails = 0;
    for (int k = 0; k < reps; k++) {
      Res r = runChild(seed, cfg, maxN, Long.getLong("hunter.wdMs", 10_000), "one" + k);
      if (failed(r)) fails++;
      System.out.println(resultLine(r));
    }
    System.out.println("ONE seed=" + seed + " cfg=" + cfg + " maxN=" + maxN + " failed=" + fails + "/" + reps);
  }

  public static void main(String[] args) throws Exception {
    if (args.length >= 4 && args[0].equals("--soak")) {
      try {
        soakChild(Long.parseLong(args[1]), args[2], Long.parseLong(args[3]));
      } catch (Throwable t) {
        System.out.println("CHILD-ERROR " + t);
        t.printStackTrace(System.out);
        System.out.flush();
        Runtime.getRuntime().halt(4);
      }
      return;
    }
    if (args.length >= 1 && args[0].equals("--inv")) {
      inv();
      return;
    }
    if (args.length >= 3 && args[0].equals("--child")) {
      try {
        child(Long.parseLong(args[1]), args[2]);
      } catch (Throwable t) {
        System.out.println("CHILD-ERROR " + t);
        t.printStackTrace(System.out);
        System.out.flush();
        Runtime.getRuntime().halt(4);
      }
      return;
    }
    if (args.length >= 3 && args[0].equals("sweep")) sweep(Long.parseLong(args[1]), Integer.parseInt(args[2]));
    else if (args.length >= 3 && args[0].equals("one"))
      one(Long.parseLong(args[1]), args[2], args.length > 3 ? Integer.parseInt(args[3]) : 0, args.length > 4 ? Integer.parseInt(args[4]) : 1);
    else if (args.length >= 4 && args[0].equals("soak"))
      soak(Long.parseLong(args[1]), Integer.parseInt(args[2]), Arrays.asList(args).subList(3, args.length));
    else if (args.length >= 2 && args[0].equals("inv")) {
      // parent side of the INV scenario: run it in a child JVM per configuration
      String cmd = System.getenv("HUNTER_JAVA_CMD");
      for (int i = 1; i < args.length; i++) {
        List<String> argv = new ArrayList<>(List.of("java"));
        boolean n2 = args[i].startsWith("n2:");
        String cfg = args[i].substring(args[i].indexOf(':') + 1);
        if (n2) argv.add("-Dinv.n2=true");
        for (String f : flags(cfg).split(" ")) if (!f.isEmpty()) argv.add(f);
        for (String a : cmd.substring(5).trim().split("\\s+")) argv.add(a);
        argv.add("--inv");
        Path log = logDir().resolve("inv-" + args[i].replace(':', '-') + ".log");
        Process p = new ProcessBuilder(argv).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        boolean fin = p.waitFor(60, TimeUnit.SECONDS);
        if (!fin) p.destroyForcibly().waitFor();
        List<String> lines = Files.readAllLines(log, java.nio.charset.StandardCharsets.ISO_8859_1);
        String hang = lines.stream().filter(l -> l.startsWith("INV-HANG")).findFirst().orElse("");
        boolean invalid = lines.stream().anyMatch(l -> l.contains("GraphInInvalidState"));
        System.out.println("RESULT scenario=" + (n2 ? "N2" : "INV") + " cfg=" + cfg + " class=" + (!fin ? "HANG:unresponsive" : p.exitValue() == 3 ? (n2 ? "HANG:causality-stall(N2)" : "HANG:sync-stack-inversion(N3)") : p.exitValue() == 0 ? "PASS" : "CRASH(" + p.exitValue() + ")")
            + (invalid ? "+ASSERT" : "") + " " + hang + " " + h4brief(kvs(lines)) + " log=" + log.getFileName());
      }
    } else if (args.length >= 2 && args[0].equals("spec")) {
      for (int i = 1; i < args.length; i++) System.out.println("seed " + args[i] + ": " + generate(Long.parseLong(args[i]), 0).summary());
    } else {
      System.out.println("usage: sweep <firstSeed> <count> | one <seed> <cfg> [maxN] [reps] | inv <cfg>... | spec <seed>...");
      System.exit(2);
    }
    System.exit(0);
  }
}

/** Oracle hooks called from code inserted by {@link HunterAgent}. Kept tiny: loaded from OGScheduler. */
final class HunterHooks {
  static final AtomicLong timerLW = new AtomicLong(), timerLWInFlight = new AtomicLong(), timerCS = new AtomicLong(), timerCalls = new AtomicLong();
  static final java.lang.reflect.Field CTX = Hunter.field(OGSchedulerContext.class, "_context");
  static volatile String firstLW;

  static final long spinMax = Long.getLong("probes.delay.maxNanos", 0);
  static final java.util.Set<String> spinIds;

  static {
    String p = System.getProperty("probes.delay.points", "");
    spinIds = p.isEmpty() ? null : java.util.Set.of(p.split(","));
  }

  /** Permit-neutral random delay (busy spin) for points immediately before a LockSupport.park. */
  public static void spinPoint(String id) {
    if (spinMax <= 0 || (spinIds != null && !spinIds.contains(id))) return;
    long end = System.nanoTime() + java.util.concurrent.ThreadLocalRandom.current().nextLong(spinMax);
    while (System.nanoTime() < end) Thread.onSpinWait();
  }

  /** fullWait's deadlock-timer branch: what findWorkOnForwardChain(awaitTask) found after >= 1 s parked. */
  public static void timerFound(NodeTask found, NodeTask awaitTask) {
    timerCalls.incrementAndGet();
    HunterH4.beforeWait(OGSchedulerContext.current(), awaitTask, "timer");
    if (found == null) return;
    if (found == awaitTask && awaitTask.isDoneEx()) {
      // Distinguish a lost wakeup from one in flight: if nobody has cleared ctx.parked 20 ms later
      // (Thread.sleep does not touch the LockSupport permit), the completion's wakeup was lost.
      boolean stillParked = false;
      try {
        Thread.sleep(20);
        OGSchedulerContext ec = OGSchedulerContext.current();
        Object ctx = CTX.get(ec);
        stillParked = ((OGScheduler.Context) ctx).parked;
      } catch (Throwable ignored) {
      }
      if (stillParked) {
        if (timerLW.incrementAndGet() == 1)
          firstLW = Thread.currentThread().getName() + " awaited " + awaitTask + " (done, still parked 20 ms later) recovered by deadlock timer";
      } else timerLWInFlight.incrementAndGet();
    } else timerCS.incrementAndGet();
  }

  static String instrumentedSummary() {
    StringBuilder sb = new StringBuilder();
    for (String c : new String[] {"OGScheduler", "NodeTask", "OGSchedulerContext"}) {
      String v = System.getProperty("hunter.agent.optimus.graph." + c);
      java.util.TreeMap<String, Integer> m = new java.util.TreeMap<>();
      if (v != null) for (String x : v.split(",")) m.merge(x.substring(x.indexOf(':') + 1), 1, Integer::sum);
      sb.append(c).append('=').append(v == null ? "NOT-INSTRUMENTED" : m.toString().replace(" ", "")).append(';');
    }
    return sb.toString();
  }
}

/**
 * H4 causality oracle, fed by {@link HunterAgent} hooks. canTake claims: a thread at causality c may
 * take a task stamped (T, d) iff T is itself and d >= c, "because it's caused by work we're
 * currently processing". The stamp is a depth, not a frame identity. This oracle keeps the identity:
 *
 * <ul>
 *   <li>Every OGSchedulerContext gets an epoch vector (agent-added field hunterEpochs): ep[k] is a
 *       globally unique id of the frame that raised currentCausalityID to k (ep[0] = last seen c).
 *   <li>Every NodeTask.setCausality(c, tid) records (tid, c, ep[1..c] of thread tid) in the
 *       agent-added field NodeTask.hunterStamp.
 *   <li>Take events (wakeup = tryWakeupThread handing a task to a waiter; findWork = findWork's
 *       return under causality > 0; run = OGSchedulerContext.run before it re-stamps, under
 *       causality > 0, which covers local-queue pops too) check that the frame at the taker's level
 *       c is the frame that was at level c when the task was stamped. If not, canTake accepted a
 *       stale stamp (STALE_TAKE; sameDepth when d == c, which is the R9 shape). popped counts stamps
 *       whose depth exceeds the stamping thread's current depth (the stamping frame has popped; the
 *       take is still sound if the frame at level c is unchanged).
 *   <li>SELF_WAIT: before runAndWait parks (and on each deadlock-timer tick) the awaited node's
 *       forward chain (waitingOn) is walked; reaching a node that is suspended on this thread's own
 *       sync stack is a certain deadlock (R9's end state), reported before the watchdog fires.
 * </ul>
 */
final class HunterH4 {
  static final boolean ON = !"false".equals(System.getProperty("hunter.h4"));
  static final java.lang.invoke.VarHandle STAMP, EPOCHS, CUR, CTX;
  static final AtomicLong SEQ = new AtomicLong();
  static final java.util.concurrent.ConcurrentHashMap<Integer, OGSchedulerContext> byTid = new java.util.concurrent.ConcurrentHashMap<>();
  static final java.util.concurrent.ConcurrentHashMap<String, AtomicLong> counts = new java.util.concurrent.ConcurrentHashMap<>();
  static final java.util.concurrent.ConcurrentHashMap<OGSchedulerContext, NodeTask> lastSelfWait = new java.util.concurrent.ConcurrentHashMap<>();
  static final AtomicInteger printed = new AtomicInteger();
  static volatile String firstStale, firstSelfWait;
  static final boolean ok;

  static final class Stamp {
    final int tid, c;
    final long[] ep;
    // last take checked against this stamp: path, taker tid@causality, taker frame at that level, verdict
    volatile String takePath, verdict;
    volatile int takeTid, takeC;
    volatile long takeFrame;
    Stamp(int tid, int c, long[] ep) {
      this.tid = tid;
      this.c = c;
      this.ep = ep;
    }
  }

  static {
    java.lang.invoke.VarHandle a = null, b = null, c = null, d = null;
    boolean k = false;
    try {
      var l = java.lang.invoke.MethodHandles.lookup();
      a = java.lang.invoke.MethodHandles.privateLookupIn(NodeTask.class, l).findVarHandle(NodeTask.class, "hunterStamp", Object.class);
      var lc = java.lang.invoke.MethodHandles.privateLookupIn(OGSchedulerContext.class, l);
      b = lc.findVarHandle(OGSchedulerContext.class, "hunterEpochs", long[].class);
      c = lc.findVarHandle(OGSchedulerContext.class, "currentCausalityID", int.class);
      d = lc.findVarHandle(OGSchedulerContext.class, "_context", OGScheduler.Context.class);
      k = true;
    } catch (Throwable t) {
      System.out.println("HunterH4: disabled, agent fields missing: " + t);
    }
    STAMP = a;
    EPOCHS = b;
    CUR = c;
    CTX = d;
    ok = k && ON;
  }

  static void inc(String k) {
    counts.computeIfAbsent(k, x -> new AtomicLong()).incrementAndGet();
  }

  static long get(String k) {
    AtomicLong v = counts.get(k);
    return v == null ? 0 : v.get();
  }

  static void reset() {
    counts.clear();
    lastSelfWait.clear();
  }

  /** Drop contexts of threads that have ended (soak: client threads churn). */
  static void purge() {
    byTid.values().removeIf(sc -> {
      OGScheduler.Context c = ctxOf(sc);
      return c == null || c.thread == null || !c.thread.isAlive();
    });
    lastSelfWait.keySet().removeIf(sc -> !byTid.containsValue(sc));
  }

  static OGScheduler.Context ctxOf(OGSchedulerContext sc) {
    return (OGScheduler.Context) CTX.get(sc);
  }

  static long[] epochs(OGSchedulerContext sc) {
    long[] ep = (long[]) EPOCHS.get(sc);
    if (ep == null) {
      ep = new long[64];
      EPOCHS.set(sc, ep);
      byTid.put(ctxOf(sc).threadID, sc);
    }
    return ep;
  }

  /** After every write of OGSchedulerContext.currentCausalityID (owner thread). */
  public static void causality(OGSchedulerContext sc) {
    if (!ok) return;
    int c = (int) CUR.get(sc);
    long[] ep = epochs(sc);
    if (c >= ep.length) {
      ep = java.util.Arrays.copyOf(ep, Math.max(c + 1, ep.length * 2));
      EPOCHS.set(sc, ep);
    }
    if (c > ep[0]) for (int k = (int) ep[0] + 1; k <= c; k++) ep[k] = SEQ.incrementAndGet();
    ep[0] = c;
  }

  /** NodeTask.setCausality entry. */
  public static void stamp(NodeTask t, int c, int tid) {
    if (!ok) return;
    if (c == 0 || tid == 0 || tid == Integer.MAX_VALUE) {
      STAMP.set(t, null);
      return;
    }
    OGSchedulerContext sc = byTid.get(tid);
    if (sc == null) {
      STAMP.set(t, new Stamp(tid, c, null));
      return;
    }
    long[] ep = epochs(sc);
    if (ep[0] < c) inc("stampAboveDepth"); // stamped with a causality the thread is not at (chain upgrade by another thread?)
    Stamp ns = new Stamp(tid, c, java.util.Arrays.copyOf(ep, Math.min(ep.length, c + 1)));
    if (STAMP.get(t) instanceof Stamp old && old.takePath != null && old.takePath.indexOf('(') < 0) { // keep how this node got here (the take that preceded this run)
      ns.takePath = old.takePath + "(prev stamp " + old.tid + "/" + old.c + " frame=" + (old.ep != null && old.ep.length > old.c ? old.ep[old.c] : -1) + ")";
      ns.takeTid = old.takeTid;
      ns.takeC = old.takeC;
      ns.takeFrame = old.takeFrame;
      ns.verdict = old.verdict;
    }
    STAMP.set(t, ns);
  }

  /** tryWakeupThread, after awaitedTask is set (hand-out succeeded). */
  public static void wakeup(NodeTask t, OGScheduler.Context ctx) {
    if (!ok || ctx == null || t == null) return;
    if (t.isDoneEx()) return; // awakeOnComplete / deadlock timer handing back the awaited node
    check(t, ctx.schedulerContext, ctx.threadID, ctx.awaitedCausalityID, "wakeup");
  }

  /** findWork's return value. */
  public static void found(NodeTask t, OGScheduler.Context ctx, NodeTask awaitTask) {
    if (!ok || ctx == null || t == null || t == awaitTask || t.isDoneEx()) return;
    check(t, ctx.schedulerContext, ctx.threadID, ctx.awaitedCausalityID, "findWork");
  }

  /** OGSchedulerContext.run after tryRun succeeded, before setCausality re-stamps. */
  public static void beforeRun(OGSchedulerContext sc, NodeTask t) {
    if (!ok) return;
    int c = (int) CUR.get(sc);
    inc("runs");
    if (c == 0) return;
    check(t, sc, ctxOf(sc).threadID, c, "run");
  }

  static void check(NodeTask t, OGSchedulerContext sc, int tid, int c, String path) {
    if (c == 0 || sc == null) return;
    inc("take." + path);
    int ttid = t.causalityThreadID(), tc = t.causalityID();
    if (ttid == 0 && tc == 0) {
      // canTake: "task is compatible with every scheduler if it has no causality information". Record the take: an
      // unstamped node run inside an unrelated sync frame is the other way into the R9 end state (seed 14).
      inc("unstamped." + path);
      Stamp u = new Stamp(0, 0, null);
      u.takePath = keepPath(STAMP.get(t), path, tid);
      u.takeTid = tid;
      u.takeC = c;
      u.takeFrame = epochs(sc)[c];
      u.verdict = "UNSTAMPED";
      STAMP.set(t, u);
      return;
    }
    Object o = STAMP.get(t);
    if (!(o instanceof Stamp s) || s.c != tc || (s.tid & 0xFFFF) != (ttid & 0xFFFF) || s.ep == null) {
      inc("unknownStamp." + path);
      return;
    }
    if (path.equals("run") && onAwaitedChain(t, sc)) {
      inc("direct." + path); // the frame runs its own awaited node (or its chain): canTake not consulted, legitimate
      return;
    }
    if (s.tid != tid) {
      inc("aliased." + path); // canTake accepted another thread's stamp (R1 aliasing)
      report("ALIASED_TAKE", t, sc, tid, c, s, path);
      return;
    }
    long[] cur = epochs(sc);
    s.takePath = keepPath(s, path, tid);
    s.takeTid = tid;
    s.takeC = c;
    s.takeFrame = cur[c];
    s.verdict = "ok";
    if (s.c > cur[0]) inc("popped." + path);
    if (s.ep.length <= c || s.ep[c] != cur[c]) {
      s.verdict = "STALE";
      inc("staleTake." + path);
      if (s.c == c) inc("staleSameDepth." + path);
      // R9 shape visible at take time: the task, or something on its forward chain, is suspended on the taker's stack
      var stack = sc.dbgHashOfSyncStack();
      boolean r9 = false;
      NodeTask x = t;
      for (int d = 0; x != null && d < 64; d++, x = x.getWaitingOn()) if (stack.containsKey(x)) r9 = true;
      if (r9) inc("r9AtTake." + path);
      report(r9 ? "STALE_TAKE+R9_SHAPE" : "STALE_TAKE", t, sc, tid, c, s, path);
    }
  }

  /** The run check follows the hand-out check of the same take: keep the hand-out path ("wakeup+run"). */
  static String keepPath(Object old, String path, int tid) {
    if (path.equals("run") && old instanceof Stamp o && o.takePath != null && o.takeTid == tid && !o.takePath.startsWith("run") && o.takePath.indexOf('(') < 0
        && o.takePath.indexOf('+') < 0) return o.takePath + "+run";
    return path;
  }

  /** t is the node the top sync frame awaits, or on its waitingOn chain (drainQueue / findWorkOnForwardChain run these directly). */
  static boolean onAwaitedChain(NodeTask t, OGSchedulerContext sc) {
    var stack = sc.dbgHashOfSyncStack();
    NodeTask top = null;
    int best = -1;
    for (var e : stack.entrySet())
      if (e.getValue() > best) {
        best = e.getValue();
        top = e.getKey();
      }
    NodeTask x = top == null ? null : top.getWaitingOn();
    for (int d = 0; x != null && d < 64; d++, x = x.getWaitingOn()) if (x == t) return true;
    return false;
  }

  static String stampInfo(NodeTask n) {
    Object o = STAMP == null ? null : STAMP.get(n);
    if (!(o instanceof Stamp s)) return n + "[stamp " + n.causalityThreadID() + "/" + n.causalityID() + " unrecorded]";
    return n + "[stamp " + s.tid + "/" + s.c + " frame=" + (s.ep != null && s.ep.length > s.c ? s.ep[s.c] : -1) + " lastTake=" + s.takePath + " by " + s.takeTid + "@" + s.takeC
        + " takerFrame=" + s.takeFrame + " " + s.verdict + "]";
  }

  static void report(String what, NodeTask t, OGSchedulerContext sc, int tid, int c, Stamp s, String path) {
    String l = "H4 " + what + " path=" + path + " task=" + t + " stamp=" + s.tid + "/" + s.c + " stampFrame=" + (s.ep.length > c ? s.ep[c] : -1)
        + " taker=" + tid + "@" + c + " takerFrame=" + epochs(sc)[c] + " takerDepth=" + epochs(sc)[0] + " thread=" + Thread.currentThread().getName();
    if (firstStale == null) firstStale = l;
    if (printed.incrementAndGet() <= 20) System.out.println(l);
  }

  /**
   * Dependencies of a node that is not done: its waitingOn, and for hunter nodes that have started
   * (so cannot be cancelled-instead-of-run any more) every kid that is not done. Every hunter kind
   * completes only after all its kids, so these are hard dependencies even where continueWith
   * leaves no waitingOn edge (FAN/MIXED/ASYNC count-downs).
   */
  static void deps(NodeTask n, java.util.function.Consumer<NodeTask> out) {
    NodeTask w = n.getWaitingOn();
    if (w != null && !w.isDoneEx()) out.accept(w);
    if (n instanceof Hunter.HN h && h.runs.get() > 0)
      for (int j = 0; j < h.nk(); j++) {
        Hunter.HN k = h.kid(j);
        if (!k.isDoneEx()) out.accept(k);
      }
  }

  /** BFS from start over deps; returns the path to the first node accepted by hit, or null. */
  static List<NodeTask> reach(NodeTask start, java.util.function.Predicate<NodeTask> hit) {
    if (start == null || start.isDoneEx()) return null;
    java.util.IdentityHashMap<NodeTask, NodeTask> parent = new java.util.IdentityHashMap<>();
    java.util.ArrayDeque<NodeTask> q = new java.util.ArrayDeque<>();
    parent.put(start, start);
    q.add(start);
    while (!q.isEmpty() && parent.size() < 20000) {
      NodeTask x = q.poll();
      if (hit.test(x)) {
        List<NodeTask> path = new ArrayList<>();
        for (NodeTask y = x; ; y = parent.get(y)) {
          path.add(0, y);
          if (y == start) break;
        }
        return path;
      }
      if (x != start && (x instanceof Hunter.HN h) && h.runs.get() == 0) continue; // not started: could still be cancelled instead of run
      deps(x, y -> {
        if (!parent.containsKey(y)) {
          parent.put(y, x);
          q.add(y);
        }
      });
    }
    return null;
  }

  static String path(List<NodeTask> p) {
    StringBuilder sb = new StringBuilder();
    for (NodeTask n : p) sb.append(sb.length() == 0 ? "" : " -> ").append(n);
    return sb.toString();
  }

  /** runAndWait before _scheduler.wait (owner thread), and the fullWait deadlock-timer tick. */
  public static void beforeWait(OGSchedulerContext sc, NodeTask awaited, String where) {
    if (!ok || sc == null || awaited == null) return;
    inc("waits." + where);
    var stack = sc.dbgHashOfSyncStack();
    List<NodeTask> p = reach(awaited, stack::containsKey);
    if (p != null) {
      if (lastSelfWait.put(sc, awaited) == awaited) return; // already reported for this wait
      NodeTask x = p.get(p.size() - 1);
      Integer at = stack.get(x);
      inc("selfWait." + where);
      String l = "H4 SELF_WAIT where=" + where + " thread=" + Thread.currentThread().getName() + " awaits=" + awaited + " path=" + path(p)
          + " reaches " + x + " suspended in sync frame " + at + " of " + stack.size() + " on this thread; causality=" + CUR.get(sc);
      if (firstSelfWait == null) firstSelfWait = l;
      System.out.println(l);
      // how the frames above the reached one got onto this thread (stamp and last take of each frame's node)
      long[] ep = epochs(sc);
      StringBuilder fr = new StringBuilder("H4 SELF_WAIT frames (index: node, causality-level epochs " + java.util.Arrays.toString(java.util.Arrays.copyOf(ep, (int) Math.min(ep.length, ep[0] + 1))) + "):");
      for (var e : new java.util.TreeMap<Integer, NodeTask>(invert(stack)).entrySet()) fr.append("\n   ").append(e.getKey()).append(": ").append(stampInfo(e.getValue()));
      fr.append("\n   awaited path:");
      for (NodeTask y : p) fr.append(" -> ").append(stampInfo(y));
      System.out.println(fr);
      Hunter.finding("CAUSALITY_SELF_WAIT", l.substring(3));
      return;
    }
    if (where.equals("timer")) {
      String cyc = waitCycle(sc, awaited);
      if (cyc != null && lastSelfWait.put(sc, awaited) != awaited) {
        inc("waitCycle.timer");
        String l = "H4 WAIT_CYCLE thread=" + Thread.currentThread().getName() + " " + cyc;
        if (firstSelfWait == null) firstSelfWait = l;
        System.out.println(l);
        Hunter.finding("CAUSALITY_WAIT_CYCLE", l.substring(3));
      }
    }
  }

  /**
   * Cross-thread form: thread T awaits a; a depends on X suspended on thread U's sync stack; U
   * awaits b; ... back to T. Every thread on the cycle is blocked for good (no graph cycle: the
   * cycle goes through "suspended below on the same stack" edges that canTake was meant to prevent).
   */
  static String waitCycle(OGSchedulerContext me, NodeTask myAwaited) {
    java.util.IdentityHashMap<NodeTask, OGSchedulerContext> onStack = new java.util.IdentityHashMap<>();
    for (OGSchedulerContext c : byTid.values())
      for (var e : c.dbgHashOfSyncStack().entrySet()) if (e.getValue() > 0 && e.getKey() != null) onStack.putIfAbsent(e.getKey(), c);
    StringBuilder sb = new StringBuilder();
    java.util.Set<OGSchedulerContext> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    OGSchedulerContext c = me;
    NodeTask a = myAwaited;
    for (int hops = 0; hops < 16; hops++) {
      seen.add(c);
      final OGSchedulerContext cc = c;
      List<NodeTask> p = reach(a, n -> onStack.containsKey(n) && (onStack.get(n) != cc || cc == me));
      if (p == null) return null;
      NodeTask x = p.get(p.size() - 1);
      OGSchedulerContext u = onStack.get(x);
      OGScheduler.Context uc = ctxOf(u);
      sb.append(" [tid ").append(ctxOf(c).threadID).append(" awaits ").append(path(p)).append(" suspended on tid ").append(uc.threadID).append(']');
      if (u == me) return "cycle" + sb;
      if (seen.contains(u)) return null; // a cycle that does not include me; its members report it
      if (uc.awaitedTask == null || uc.awaitedCausalityID == 0 && !onStack.containsValue(u)) return null;
      c = u;
      a = uc.awaitedTask;
    }
    return null;
  }

  static java.util.Map<Integer, NodeTask> invert(java.util.Map<NodeTask, Integer> m) {
    java.util.Map<Integer, NodeTask> r = new java.util.HashMap<>();
    for (var e : m.entrySet()) r.put(e.getValue(), e.getKey());
    return r;
  }

  /** Post-mortem for the watchdog: same checks for a parked waiter, from another thread. */
  static String postMortem(OGScheduler.Context c) {
    if (!ok || c.schedulerContext == null || c.awaitedTask == null) return "";
    var stack = c.schedulerContext.dbgHashOfSyncStack();
    List<NodeTask> p = reach(c.awaitedTask, stack::containsKey);
    if (p != null) return " selfWait(" + path(p) + "@frame" + stack.get(p.get(p.size() - 1)) + ")";
    String cyc = waitCycle(c.schedulerContext, c.awaitedTask);
    return cyc == null ? "" : " waitCycle(" + cyc + ")";
  }

  static String summary() {
    StringBuilder sb = new StringBuilder();
    for (String p : new String[] {"wakeup", "findWork", "run"})
      sb.append(" h4take.").append(p).append('=').append(get("take." + p)).append(" h4stale.").append(p).append('=').append(get("staleTake." + p))
          .append(" h4sameDepth.").append(p).append('=').append(get("staleSameDepth." + p)).append(" h4popped.").append(p).append('=').append(get("popped." + p))
          .append(" h4unstamped.").append(p).append('=').append(get("unstamped." + p))
          .append(" h4r9AtTake.").append(p).append('=').append(get("r9AtTake." + p)).append(" h4unknown.").append(p).append('=').append(get("unknownStamp." + p))
          .append(" h4aliased.").append(p).append('=').append(get("aliased." + p)).append(" h4direct.").append(p).append('=').append(get("direct." + p));
    sb.append(" h4selfWait.wait=").append(get("selfWait.wait")).append(" h4selfWait.timer=").append(get("selfWait.timer")).append(" h4waitCycle.timer=").append(get("waitCycle.timer"))
        .append(" h4waits=").append(get("waits.wait")).append(" h4runs=").append(get("runs")).append(" h4stampAboveDepth=").append(get("stampAboveDepth"))
        .append(" h4on=").append(ok);
    return sb.toString().trim();
  }
}

/**
 * Second instrumentation agent for the hunter (complements probes.DelayAgent, which only covers
 * WorkQueue). Inserts probes.Delay.point(id) at the lost-wakeup-relevant points of OGScheduler,
 * NodeTask and OGSchedulerContext, plus the fullWait timer-recovery oracle hook:
 *
 * <pre>
 *   s.parked    OGScheduler.*: after every write of Context.parked
 *   s.awaited   OGScheduler.*: after every write of Context.awaitedTask
 *   s.spin      OGScheduler.*: after every write of _spinningThreadID
 *   s.park      OGScheduler.*: before LockSupport.park/parkNanos (busy spin via HunterHooks.spinPoint, permit-neutral)
 *   s.unpark    OGScheduler.*: before LockSupport.unpark
 *   n.waiters   NodeTask.tryAddToWaiterList*, notifyWaiters: after the read of _waiters (before the CAS)
 *   n.notify    NodeTask.notifyWaiters: before each waiter.onChildCompleted
 *   c.register  OGSchedulerContext.runAndWait: before tryAddToWaiterList(frame)
 *   c.wait      OGSchedulerContext.runAndWait: before _scheduler.wait(...)
 *   c.tryRun    OGSchedulerContext.run: after ntsk.tryRun()
 *   (oracle)    OGScheduler.fullWait: after findWorkOnForwardChain -> HunterHooks.timerFound(found, awaitTask)
 * </pre>
 */
final class HunterAgent {
  static final String HOOK = "probes/Delay";
  static final String HOOKS = "optimus/graph/HunterHooks";
  static final String H4 = "optimus/graph/HunterH4";

  public static void premain(String args, Instrumentation inst) {
    inst.addTransformer(
        new ClassFileTransformer() {
          @Override
          public byte[] transform(ClassLoader loader, String name, Class<?> cls, ProtectionDomain pd, byte[] bytes) {
            if (!"optimus/graph/OGScheduler".equals(name) && !"optimus/graph/NodeTask".equals(name) && !"optimus/graph/OGSchedulerContext".equals(name))
              return null;
            if (cls != null) return null; // retransform: cannot add the H4 fields again
            try {
              return instrument(name, bytes);
            } catch (Throwable t) {
              System.err.println("HunterAgent: failed to instrument " + name + ": " + t);
              t.printStackTrace();
              return null;
            }
          }
        });
  }

  static byte[] instrument(String cname, byte[] bytes) {
    ClassReader cr = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
    StringBuilder log = new StringBuilder();
    cr.accept(
        new ClassVisitor(Opcodes.ASM9, cw) {
          @Override
          public void visitEnd() {
            // H4: side fields for the causality oracle (HunterH4)
            if (cname.equals("optimus/graph/NodeTask")) super.visitField(Opcodes.ACC_PUBLIC, "hunterStamp", "Ljava/lang/Object;", null, null).visitEnd();
            if (cname.equals("optimus/graph/OGSchedulerContext")) super.visitField(Opcodes.ACC_PUBLIC, "hunterEpochs", "[J", null, null).visitEnd();
            super.visitEnd();
          }

          @Override
          public MethodVisitor visitMethod(int access, String m, String desc, String sig, String[] exc) {
            MethodVisitor mv = super.visitMethod(access, m, desc, sig, exc);
            return new MethodVisitor(Opcodes.ASM9, mv) {
              void h4(String hname, String hdesc) {
                super.visitMethodInsn(Opcodes.INVOKESTATIC, H4, hname, hdesc, false);
                log.append(' ').append(m).append(":h4.").append(hname);
              }

              @Override
              public void visitCode() {
                super.visitCode();
                if (cname.equals("optimus/graph/NodeTask") && m.equals("setCausality") && desc.equals("(II)V")) {
                  super.visitVarInsn(Opcodes.ALOAD, 0);
                  super.visitVarInsn(Opcodes.ILOAD, 1);
                  super.visitVarInsn(Opcodes.ILOAD, 2);
                  h4("stamp", "(Loptimus/graph/NodeTask;II)V");
                }
              }

              @Override
              public void visitInsn(int op) {
                if (op == Opcodes.ARETURN && cname.equals("optimus/graph/OGScheduler") && m.equals("findWork")) {
                  super.visitInsn(Opcodes.DUP);
                  super.visitVarInsn(Opcodes.ALOAD, 1);
                  super.visitVarInsn(Opcodes.ALOAD, 2);
                  h4("found", "(Loptimus/graph/NodeTask;Loptimus/graph/OGScheduler$Context;Loptimus/graph/NodeTask;)V");
                }
                super.visitInsn(op);
              }
              void hook(String id) {
                super.visitLdcInsn(id);
                super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOK, "point", "(Ljava/lang/String;)V", false);
                log.append(' ').append(m).append(':').append(id);
              }

              @Override
              public void visitFieldInsn(int op, String owner, String name, String d) {
                super.visitFieldInsn(op, owner, name, d);
                if (cname.equals("optimus/graph/OGScheduler") && op == Opcodes.PUTFIELD) {
                  if (owner.equals("optimus/graph/OGScheduler$Context") && name.equals("parked")) hook("s.parked");
                  if (owner.equals("optimus/graph/OGScheduler$Context") && name.equals("awaitedTask")) hook("s.awaited");
                  if (owner.equals("optimus/graph/OGScheduler") && name.equals("_spinningThreadID")) hook("s.spin");
                }
                if (cname.equals("optimus/graph/OGScheduler") && op == Opcodes.PUTFIELD && m.equals("tryWakeupThread")
                    && owner.equals("optimus/graph/OGScheduler$Context") && name.equals("awaitedTask")) {
                  super.visitVarInsn(Opcodes.ALOAD, 1);
                  super.visitVarInsn(Opcodes.ALOAD, 2);
                  h4("wakeup", "(Loptimus/graph/NodeTask;Loptimus/graph/OGScheduler$Context;)V");
                }
                if (cname.equals("optimus/graph/OGSchedulerContext") && op == Opcodes.PUTFIELD && !m.equals("<init>")
                    && owner.equals("optimus/graph/OGSchedulerContext") && name.equals("currentCausalityID")) {
                  super.visitVarInsn(Opcodes.ALOAD, 0);
                  h4("causality", "(Loptimus/graph/OGSchedulerContext;)V");
                }
                if (cname.equals("optimus/graph/NodeTask") && op == Opcodes.GETFIELD && name.equals("_waiters")
                    && (m.equals("tryAddToWaiterList") || m.equals("tryAddToWaiterListLast") || m.equals("notifyWaiters"))) hook("n.waiters");
              }

              @Override
              public void visitMethodInsn(int op, String owner, String name, String d, boolean itf) {
                if (cname.equals("optimus/graph/OGScheduler") && owner.equals("java/util/concurrent/locks/LockSupport")) {
                  // NOT probes.Delay: its parkNanos would consume the LockSupport permit that the waker's
                  // unpark left for exactly this park, turning every such wakeup into a 1 s timed wait.
                  if (name.startsWith("park")) {
                    super.visitLdcInsn("s.park");
                    super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOKS, "spinPoint", "(Ljava/lang/String;)V", false);
                    log.append(' ').append(m).append(":s.park");
                  }
                  if (name.equals("unpark")) hook("s.unpark");
                }
                if (cname.equals("optimus/graph/NodeTask") && m.equals("notifyWaiters") && name.equals("onChildCompleted")) hook("n.notify");
                if (cname.equals("optimus/graph/OGSchedulerContext") && m.equals("runAndWait")) {
                  if (name.equals("tryAddToWaiterList")) hook("c.register");
                  if (name.equals("wait") && owner.equals("optimus/graph/OGScheduler")) {
                    super.visitVarInsn(Opcodes.ALOAD, 0);
                    super.visitVarInsn(Opcodes.ALOAD, 1);
                    super.visitLdcInsn("wait");
                    h4("beforeWait", "(Loptimus/graph/OGSchedulerContext;Loptimus/graph/NodeTask;Ljava/lang/String;)V");
                    hook("c.wait");
                  }
                }
                if (cname.equals("optimus/graph/OGSchedulerContext") && m.equals("run") && name.equals("setCausality")) {
                  super.visitVarInsn(Opcodes.ALOAD, 0);
                  super.visitVarInsn(Opcodes.ALOAD, 1);
                  h4("beforeRun", "(Loptimus/graph/OGSchedulerContext;Loptimus/graph/NodeTask;)V");
                }
                super.visitMethodInsn(op, owner, name, d, itf);
                if (cname.equals("optimus/graph/OGSchedulerContext") && m.equals("run") && name.equals("tryRun")) hook("c.tryRun");
                if (cname.equals("optimus/graph/OGScheduler") && m.equals("fullWait") && name.equals("findWorkOnForwardChain")) {
                  super.visitInsn(Opcodes.DUP);
                  super.visitVarInsn(Opcodes.ALOAD, 1);
                  super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOKS, "timerFound", "(Loptimus/graph/NodeTask;Loptimus/graph/NodeTask;)V", false);
                  log.append(' ').append(m).append(":timerFound");
                }
              }
            };
          }
        },
        0);
    System.err.println("HunterAgent: instrumented " + cname + ":" + log);
    System.setProperty("hunter.agent." + cname.replace('/', '.'), log.toString().trim().replace(' ', ','));
    return cw.toByteArray();
  }
}
