package optimus.graph.tracking.ttracks;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import optimus.graph.NodeTask;

/**
 * Tier 1 tracker probes (Tier1Tracker.scala): read-only walks of a tracker's TTrack graph, in the ttracks package so
 * the package-private TTrack.nodes/ptracks, TTrackRef.nref and Ptracks are reachable.
 */
public final class Tier1TrackerTT {
  private Tier1TrackerTT() {}

  /** Where is `target` in the graph? One entry per root key (selected by keyFilter) from which it is reachable. */
  public static String locate(Map<?, ?> ttracks, NodeTask target, Predicate<Object> keyFilter) {
    StringBuilder sb = new StringBuilder();
    int roots = 0, hits = 0;
    for (Map.Entry<?, ?> e : ttracks.entrySet()) {
      if (keyFilter != null && !keyFilter.test(e.getKey())) continue;
      roots++;
      String r = search((TTrack) e.getValue(), target);
      if (r != null) {
        hits++;
        sb.append("root(").append(shortKey(e.getKey())).append(")->").append(r).append(' ');
      }
    }
    return "roots=" + roots + " reachableFrom=" + hits + (hits > 0 ? " [" + sb.toString().trim() + "]" : "");
  }

  /** Is target reachable from any root whose key passes keyFilter? */
  public static boolean reachable(Map<?, ?> ttracks, NodeTask target, Predicate<Object> keyFilter) {
    for (Map.Entry<?, ?> e : ttracks.entrySet()) {
      if (keyFilter != null && !keyFilter.test(e.getKey())) continue;
      if (search((TTrack) e.getValue(), target) != null) return true;
    }
    return false;
  }

  public static int countRoots(Map<?, ?> ttracks, Predicate<Object> keyFilter) {
    int n = 0;
    for (Object k : ttracks.keySet()) if (keyFilter == null || keyFilter.test(k)) n++;
    return n;
  }

  static String shortKey(Object k) {
    String s = String.valueOf(k);
    return s.length() > 40 ? s.substring(0, 40) : s;
  }

  /** BFS over callers from root; returns "depth=d track=<state>" for the first track listing target, else null. */
  static String search(TTrack root, NodeTask target) {
    Set<TTrack> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    ArrayDeque<TTrack> q = new ArrayDeque<>();
    ArrayDeque<Integer> d = new ArrayDeque<>();
    q.add(root);
    d.add(0);
    seen.add(root);
    while (!q.isEmpty()) {
      TTrack t = q.poll();
      int depth = d.poll();
      String st = scan(t, target);
      if (st != null) return "depth=" + depth + " " + st;
      Ptracks p = t.ptracks;
      if (p == null) continue;
      int n = p.currentSize();
      for (int i = 0; i < n; i++) {
        TTrack c = p.at(i);
        if (c != null && seen.add(c)) {
          q.add(c);
          d.add(depth + 1);
        }
      }
    }
    return null;
  }

  /** If t's node list holds target: describe the list (length, terminator). */
  static String scan(TTrack t, NodeTask target) {
    TTrackRef r = t.nodes;
    boolean found = false;
    int len = 0;
    while (r != null && r != TTrackRef.Nil && r != TTrackRef.Invalid) {
      if (r.get() == target) found = true;
      len++;
      r = r.nref;
    }
    if (!found) return null;
    String term = r == null ? "null" : r == TTrackRef.Nil ? "Nil" : "Invalid";
    return "refs=" + len + " terminator=" + term;
  }

  /** State of the TTrack held as a node's info, if any (nodes head / ptracks size). */
  public static String describe(Object info) {
    if (!(info instanceof TTrack t)) return info == null ? "null" : info.getClass().getSimpleName();
    TTrackRef r = t.nodes;
    String head = r == null ? "null(in-flight)" : r == TTrackRef.Nil ? "Nil" : r == TTrackRef.Invalid ? "Invalid" : "refs";
    int callers = t.ptracks == null ? -1 : t.ptracks.currentSize();
    return "TTrack(nodes=" + head + ", callers=" + callers + ")";
  }
}
