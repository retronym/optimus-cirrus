package optimus.graph.tracking.ttracks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import optimus.graph.ExtractorTweakableKey;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.TweakableKey;
import optimus.graph.tracking.CleanupScheduler;
import optimus.graph.tracking.TraversalIdSource;

/**
 * W14: exhaustive small-graph model check of CleanupVisitor. Lives in the ttracks package to reach
 * the package-private TTrack/TTrackRoot/CleanupVisitor members.
 *
 * <p>Graph model: vertices 0..n-1 in topological order; an edge i->j (i<j) means "j is a caller of
 * i" (j is in i.ptracks). Sources (no incoming edge) are TTrackRoots (nodes = Nil) registered in
 * the tracker map. Every other vertex gets a node state: LIVE (one ref to a strongly held node),
 * NIL, NULL (in-flight owner, nodes == null), DEAD (one ref whose referent is cleared), INVALID.
 * Edges are added with TTrack.addCaller, so the Ptracks representations (bare TTrack, Ptracks2,
 * PtracksArray) arise naturally. Variants: caller insertion order (asc, desc, shuffled), root
 * iteration order (asc, desc), initial lastUpdateTraversalID (all 0, or random stale ids below the
 * start id), plus one duplicate parallel edge where a callee has >= 3 callers.
 *
 * <p>Oracle, after each of two consecutive cleanups: for every root, every live node and every NULL
 * track reachable before the first cleanup by a path that does not pass through an INVALID track is
 * still reachable. (Live nodes reachable only through an INVALID track are counted separately:
 * cleanup drops edges to Invalid callers by design, ring4b S5.)
 */
public final class TTrackProbeSupport {
  static final int LIVE = 0, NIL = 1, NULL = 2, DEAD = 3, INVALID = 4;
  static final String[] STATE_NAMES = {"LIVE", "NIL", "NULL", "DEAD", "INVALID"};

  /** NodeTask's constructor is package-private; PropertyNode's is public. */
  static final class Stub extends optimus.graph.PropertyNode<Object> {
    final int id;

    Stub(int id) {
      this.id = id;
    }

    @Override
    public optimus.platform.storable.Entity entity() {
      return null;
    }

    @Override
    public String toString() {
      return "N" + id;
    }
  }

  static final class Ids implements TraversalIdSource {
    int id;

    @Override
    public void withFreshTraversalId(scala.Function1<Integer, Integer> func) {
      int start = id + 1;
      int end = func.apply(start);
      if (end < start) throw new IllegalStateException("end < start");
      id = end;
    }
  }

  public static final class Report {
    public String label;
    public long graphs; // distinct edge sets
    public long instances; // graphs x states x variants
    public long liveViolations, nullViolations, orphanedViaInvalid;
    public String firstLive, firstNull;
    public long edgesRemoved, middleMenRemoved;

    @Override
    public String toString() {
      return label
          + ": graphs="
          + graphs
          + " instances="
          + instances
          + " liveViolations="
          + liveViolations
          + " nullTrackViolations="
          + nullViolations
          + (orphanedViaInvalid > 0 ? " liveLostOnlyViaInvalidPath=" + orphanedViaInvalid : "")
          + " edgesRemoved="
          + edgesRemoved
          + " middleMenBypassed="
          + middleMenRemoved
          + (firstLive != null ? " firstLiveCounterexample={" + firstLive + "}" : "")
          + (firstNull != null ? " firstNullCounterexample={" + firstNull + "}" : "");
    }
  }

  static final NodeTaskInfo INFO = new NodeTaskInfo("w14key", NodeTaskInfo.TWEAKABLE);

  /** states: subset of {LIVE,NIL,NULL,DEAD,INVALID} for non-root tracks. */
  public static Report run(String label, int maxN, int maxRedundant, int[] states) {
    return run(label, maxN, maxRedundant, states, false);
  }

  /**
   * consistentInvalidation: skip assignments where an INVALID track has a non-INVALID caller
   * (invalidation walks callers transitively, so such a state needs a caller attached after
   * invalidation, ring4b S5 / D6).
   */
  public static Report run(String label, int maxN, int maxRedundant, int[] states, boolean consistentInvalidation) {
    Report rep = new Report();
    rep.label = label;
    int nStates = states.length;
    Random rnd = new Random(42);
    for (int n = 2; n <= maxN; n++) {
      int[][] pairs = new int[n * (n - 1) / 2][];
      int p = 0;
      for (int i = 0; i < n; i++) for (int j = i + 1; j < n; j++) pairs[p++] = new int[] {i, j};
      for (long mask = 1; mask < (1L << pairs.length); mask++) {
        boolean[][] adj = new boolean[n][n];
        int[] deg = new int[n];
        boolean[] hasIn = new boolean[n];
        for (int k = 0; k < pairs.length; k++)
          if ((mask & (1L << k)) != 0) {
            int i = pairs[k][0], j = pairs[k][1];
            adj[i][j] = true;
            deg[i]++;
            deg[j]++;
            hasIn[j] = true;
          }
        boolean isolated = false;
        for (int v = 0; v < n; v++) if (deg[v] == 0) isolated = true;
        if (isolated) continue;
        if (redundantEdges(adj, n) > maxRedundant) continue;
        rep.graphs++;
        List<Integer> inner = new ArrayList<>();
        for (int v = 0; v < n; v++) if (hasIn[v]) inner.add(v);
        int combos = 1;
        for (int k = 0; k < inner.size(); k++) combos *= nStates;
        int dupFrom = -1;
        for (int v = 0; v < n && dupFrom < 0; v++) {
          int cnt = 0;
          for (int j = 0; j < n; j++) if (adj[v][j]) cnt++;
          if (cnt >= 3) dupFrom = v;
        }
        for (int c = 0; c < combos; c++) {
          int[] state = new int[n];
          java.util.Arrays.fill(state, NIL); // roots: Nil
          int cc = c;
          for (int v : inner) {
            state[v] = states[cc % nStates];
            cc /= nStates;
          }
          if (consistentInvalidation) {
            boolean bad = false;
            for (int i = 0; i < n; i++)
              for (int j = 0; j < n; j++)
                if (adj[i][j] && hasIn[i] && state[i] == INVALID && state[j] != INVALID) bad = true;
            if (bad) continue;
          }
          for (int callerOrder = 0; callerOrder < 3; callerOrder++)
            for (int rootsDesc = 0; rootsDesc < 2; rootsDesc++)
              for (int staleIds = 0; staleIds < 2; staleIds++) {
                long seed = rnd.nextLong();
                check(rep, n, adj, hasIn, state, callerOrder, rootsDesc != 0, staleIds != 0, seed, -1, -1);
                if (dupFrom >= 0)
                  for (int j = 0; j < n; j++)
                    if (adj[dupFrom][j])
                      check(rep, n, adj, hasIn, state, callerOrder, rootsDesc != 0, staleIds != 0, seed, dupFrom, j);
              }
        }
      }
    }
    return rep;
  }

  static int redundantEdges(boolean[][] adj, int n) {
    boolean[][] reach = new boolean[n][n]; // path of length >= 1
    for (int i = n - 1; i >= 0; i--)
      for (int j = i + 1; j < n; j++)
        if (adj[i][j]) {
          reach[i][j] = true;
          for (int k = 0; k < n; k++) if (reach[j][k]) reach[i][k] = true;
        }
    int red = 0;
    for (int i = 0; i < n; i++)
      for (int j = i + 1; j < n; j++)
        if (adj[i][j])
          for (int k = i + 1; k < j; k++)
            if (adj[i][k] && reach[k][j]) {
              red++;
              break;
            }
    return red;
  }

  static TTrack[] build(int n, boolean[][] adj, boolean[] hasIn, int[] state, Stub[] live, int callerOrder, long seed, int dupFrom, int dupTo) {
    TTrack[] t = new TTrack[n];
    for (int v = 0; v < n; v++) {
      if (!hasIn[v]) t[v] = new TTrackRoot();
      else
        switch (state[v]) {
          case LIVE -> {
            live[v] = new Stub(v);
            t[v] = new TTrack(new TTrackRef(live[v], null, null));
          }
          case NIL -> t[v] = new TTrack(TTrackRef.Nil);
          case NULL -> t[v] = new TTrack((TTrackRef) null);
          case DEAD -> {
            TTrackRef r = new TTrackRef(new Stub(-v), null, null);
            r.unsafeForceClear();
            t[v] = new TTrack(r);
          }
          case INVALID -> t[v] = new TTrack(TTrackRef.Invalid);
          default -> throw new IllegalStateException();
        }
    }
    Random rnd = new Random(seed);
    for (int i = 0; i < n; i++) {
      List<Integer> callers = new ArrayList<>();
      for (int j = 0; j < n; j++) if (adj[i][j]) callers.add(j);
      if (callerOrder == 1) Collections.reverse(callers);
      else if (callerOrder == 2) Collections.shuffle(callers, rnd);
      for (int j : callers) t[i].addCaller(t[j], Integer.MAX_VALUE);
      if (i == dupFrom) t[i].addCaller(t[dupTo], 0); // duplicate parallel edge (PtracksArray only)
    }
    return t;
  }

  static void check(
      Report rep,
      int n,
      boolean[][] adj,
      boolean[] hasIn,
      int[] state,
      int callerOrder,
      boolean rootsDesc,
      boolean staleIds,
      long seed,
      int dupFrom,
      int dupTo) {
    rep.instances++;
    Stub[] live = new Stub[n]; // strong refs
    TTrack[] t = build(n, adj, hasIn, state, live, callerOrder, seed, dupFrom, dupTo);
    Ids ids = new Ids();
    if (staleIds) {
      Random r = new Random(~seed);
      ids.id = 64;
      for (TTrack x : t) x.lastUpdateTraversalID = r.nextInt(65); // all < start id 65
    }
    Map<TweakableKey, TTrackRoot> map = new LinkedHashMap<>();
    List<Integer> roots = new ArrayList<>();
    for (int v = 0; v < n; v++) if (!hasIn[v]) roots.add(v);
    if (rootsDesc) Collections.reverse(roots);
    for (int r : roots) map.put(new ExtractorTweakableKey(r, INFO), (TTrackRoot) t[r]);

    Map<Integer, Set<Object>> beforeLive = new LinkedHashMap<>(), beforeNull = new LinkedHashMap<>(), beforeAll = new LinkedHashMap<>();
    for (int r : roots) {
      Set<Object> l = new HashSet<>(), nl = new HashSet<>(), all = new HashSet<>();
      reach(t[r], l, nl, t, true);
      reach(t[r], all, new HashSet<>(), t, false);
      beforeLive.put(r, l);
      beforeNull.put(r, nl);
      beforeAll.put(r, all);
    }
    for (int pass = 1; pass <= 2; pass++) {
      CleanupVisitor cv = new CleanupVisitor();
      cv.clean(map.entrySet().iterator(), ids, CleanupScheduler.NoInterruption$.MODULE$);
      rep.edgesRemoved += cv.edgesRemoved;
      rep.middleMenRemoved += cv.redundantTTracksRemoved;
      for (int r : roots) {
        Set<Object> l = new HashSet<>(), nl = new HashSet<>();
        boolean inMap = map.containsValue(t[r]);
        if (inMap) reach(t[r], l, nl, t, false);
        String where = null;
        if (!l.containsAll(beforeLive.get(r))) {
          rep.liveViolations++;
          if (rep.firstLive == null) where = "live";
        } else if (!nl.containsAll(beforeNull.get(r))) {
          rep.nullViolations++;
          if (rep.firstNull == null) where = "null";
        } else if (pass == 1 && !l.containsAll(beforeAll.get(r))) rep.orphanedViaInvalid++;
        if (where != null) {
          String s =
              describe(n, adj, hasIn, state, callerOrder, rootsDesc, staleIds, dupFrom, dupTo)
                  + " pass="
                  + pass
                  + " root="
                  + r
                  + (inMap ? "" : "(removed from map)")
                  + " before="
                  + (where.equals("live") ? beforeLive.get(r) : beforeNull.get(r))
                  + " after="
                  + (where.equals("live") ? l : nl)
                  + " graphAfter="
                  + dump(t);
          if (where.equals("live")) rep.firstLive = s;
          else rep.firstNull = s;
        }
        if (where != null) break;
      }
    }
    java.lang.ref.Reference.reachabilityFence(live);
  }

  static int indexOf(TTrack[] t, TTrack x) {
    for (int i = 0; i < t.length; i++) if (t[i] == x) return i;
    return -1;
  }

  static void reach(TTrack start, Set<Object> liveOut, Set<Object> nullOut, TTrack[] all, boolean stopAtInvalid) {
    IdentityHashMap<TTrack, Boolean> seen = new IdentityHashMap<>();
    ArrayList<TTrack> work = new ArrayList<>();
    work.add(start);
    seen.put(start, true);
    while (!work.isEmpty()) {
      TTrack x = work.remove(work.size() - 1);
      TTrackRef ref = x.nodes;
      if (ref == TTrackRef.Invalid && stopAtInvalid) continue;
      if (ref == null) nullOut.add("T" + indexOf(all, x));
      else if (ref != TTrackRef.Nil && ref != TTrackRef.Invalid)
        for (TTrackRef r = ref; r != null; r = r.nref) {
          NodeTask nt = r.get();
          if (nt != null) liveOut.add(nt);
        }
      Ptracks p = x.ptracks;
      for (int k = 0; k < p.currentSize(); k++) {
        TTrack c = p.at(k);
        if (c != null && seen.put(c, true) == null) work.add(c);
      }
    }
  }

  static String describe(
      int n, boolean[][] adj, boolean[] hasIn, int[] state, int callerOrder, boolean rootsDesc, boolean staleIds, int dupFrom, int dupTo) {
    StringBuilder sb = new StringBuilder("n=" + n + " edges(callee->caller)=[");
    for (int i = 0; i < n; i++) for (int j = 0; j < n; j++) if (adj[i][j]) sb.append(i).append("->").append(j).append(' ');
    sb.append("] states=[");
    for (int v = 0; v < n; v++) sb.append(v).append(':').append(hasIn[v] ? STATE_NAMES[state[v]] : "ROOT").append(' ');
    sb.append("] callerOrder=").append(new String[] {"asc", "desc", "shuffled"}[callerOrder]);
    sb.append(" rootOrder=").append(rootsDesc ? "desc" : "asc");
    sb.append(" staleIds=").append(staleIds);
    if (dupFrom >= 0) sb.append(" dupEdge=").append(dupFrom).append("->").append(dupTo);
    return sb.toString();
  }

  static String dump(TTrack[] t) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < t.length; i++) {
      sb.append(i).append(":callers{");
      Ptracks p = t[i].ptracks;
      for (int k = 0; k < p.currentSize(); k++) {
        TTrack c = p.at(k);
        sb.append(c == null ? "null" : String.valueOf(indexOf(t, c))).append(' ');
      }
      TTrackRef r = t[i].nodes;
      sb.append("} nodes=")
          .append(r == null ? "null" : r == TTrackRef.Nil ? "Nil" : r == TTrackRef.Invalid ? "Invalid" : "refs")
          .append("; ");
    }
    return sb.append("]").toString();
  }

  /** Oracle self-test: removing an edge by hand must be reported. Returns true if detected. */
  public static boolean oracleSelfTest() {
    Stub n1 = new Stub(1);
    TTrack r = new TTrackRoot();
    TTrack a = new TTrack(new TTrackRef(n1, null, null));
    r.addCaller(a, Integer.MAX_VALUE);
    Set<Object> before = new HashSet<>(), after = new HashSet<>();
    reach(r, before, new HashSet<>(), new TTrack[] {r, a}, true);
    r.ptracks.drop(0, r);
    reach(r, after, new HashSet<>(), new TTrack[] {r, a}, true);
    java.lang.ref.Reference.reachabilityFence(n1);
    return before.contains(n1) && !after.containsAll(before);
  }
}
