package optimus.graph;

import static optimus.graph.Probes.Outcome.DEMONSTRATED;
import static optimus.graph.Probes.Outcome.ERROR;
import static optimus.graph.Probes.Outcome.NOT_REPRODUCED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import optimus.graph.Probes.Result;
import optimus.graph.Probes.T;
import probes.Delay;
import probes.Steps;

/**
 * Tier 0 probes that need P3 (delay injection inside WorkQueue methods, see probes.DelayAgent).
 * Each probe runs every interleaving of its participants' steps (Steps.permutations) against the
 * real, agent-instrumented OGSchedulerContext$WorkQueue.
 *
 * <p>Step convention per participant: step 1 = start the call; step 2 = proceed past the armed
 * Delay point (i.e. do the CAS/write that follows the read). A participant that returns before
 * reaching its armed point consumes its remaining steps via padTo, so "step 2" then means "after
 * return".
 */
final class Probes3 {
  private Probes3() {}

  static Result requireAgent(String id, String... points) {
    for (String p : points)
      if (!Delay.instrumented.contains(p))
        return new Result(id, ERROR, "delay agent not active (missing point " + p + "; instrumented=" + Delay.instrumented + "); run via probes/run.sh");
    return null;
  }

  static NodeTask[] rawArray(OGSchedulerContext.WorkQueue q) throws Exception {
    return (NodeTask[]) Probes.field(Probes.wq(), "_q").get(q);
  }

  static List<NodeTask> live(OGSchedulerContext.WorkQueue q) {
    List<NodeTask> l = new ArrayList<>();
    q.foreach(l::add);
    return l;
  }

  /** Null slot in [base, top) with a non-null slot above it. */
  static String hole(OGSchedulerContext.WorkQueue q) throws Exception {
    NodeTask[] a = rawArray(q);
    int base = Probes.base(q), top = Probes.top(q), mask = a.length - 1;
    for (int i = base; i < top; i++)
      if (a[i & mask] == null)
        for (int j = i + 1; j < top; j++) if (a[j & mask] != null) return "null at " + i + " below live at " + j;
    return null;
  }

  static Set<NodeTask> idSet() {
    return Collections.newSetFromMap(new IdentityHashMap<>());
  }

  // ---------- W1c: localPop vs tryToSteal on a single element ----------
  static Result w1c() throws Exception {
    Result agent = requireAgent("W1c", "localPop:pop.read", "tryToSteal:steal.read");
    if (agent != null) return agent;
    StringBuilder sb = new StringBuilder();
    boolean stale = false, broken = false;
    String staleDetail = null;
    for (String sched : Steps.permutations(2, 2)) {
      var q = Probes.newQueue(10, 10);
      T t = new T(1);
      q.localPush(t);
      NodeTask[] res = new NodeTask[2];
      Steps.schedule(sched)
          .thread(
              ctl -> {
                Delay.arm(ctl, "pop.read");
                try {
                  ctl.step();
                  res[0] = q.localPop(0);
                  ctl.padTo(2);
                } finally {
                  Delay.disarm();
                }
              })
          .thread(
              ctl -> {
                Delay.arm(ctl, "steal.read");
                try {
                  ctl.step();
                  res[1] = q.tryToSteal();
                  ctl.padTo(2);
                } finally {
                  Delay.disarm();
                }
              })
          .run();
      int top = Probes.top(q), base = Probes.base(q);
      boolean inQueue = live(q).contains(t);
      boolean lost = res[0] != t && res[1] != t && !inQueue;
      boolean consistent = top >= base && q.size() == (inQueue ? 1 : 0);
      boolean both = res[0] == t && res[1] == t;
      if (both && staleDetail == null)
        staleDetail = "[" + sched + "] owner localPop -> " + res[0] + " (its CAS failed, _top not decremented), thief tryToSteal -> " + res[1] + "; after: " + Probes.state(q);
      stale |= both;
      broken |= lost || !consistent;
      sb.append(" [").append(sched).append("] pop=").append(res[0]).append(" steal=").append(res[1])
          .append(" top=").append(top).append(" base=").append(base)
          .append(lost ? " LOST" : "").append(consistent ? "" : " INCONSISTENT").append(';');
    }
    String head =
        broken
            ? "INVARIANT BROKEN (lost task or top/base inconsistent)"
            : stale ? "stale return reproduced, invariants held: " + staleDetail : "no stale return in any schedule";
    return new Result("W1c", stale || broken ? DEMONSTRATED : NOT_REPRODUCED, head + "; all schedules:" + sb);
  }

  // ---------- W1d: growArray during steal ----------
  static Result w1d() throws Exception {
    Result agent = requireAgent("W1d", "growArray:grow.beforeSwap", "tryToSteal:steal.read");
    if (agent != null) return agent;
    StringBuilder sb = new StringBuilder();
    boolean dropped = false, baseBack = false, resurrect = false;
    String resurrectDetail = null;
    for (String sched : Steps.permutations(2, 2)) {
      var q = Probes.newQueue(0, 0); // schedulerContext == null: okToDropDoneTasks, all tasks live anyway
      List<T> all = new ArrayList<>();
      for (int i = 0; i < 63; i++) {
        T t = new T(i);
        all.add(t);
        q.localPush(t);
      }
      NodeTask[] oq = rawArray(q);
      int thiefBase0 = Probes.base(q);
      T last = new T(63);
      all.add(last);
      NodeTask[] stolen = new NodeTask[1];
      int[] thiefBaseSeen = {-1};
      Steps.schedule(sched)
          .thread(
              ctl -> { // owner: push #64 hits (top - base) >= mask -> growArray
                Delay.arm(ctl, "grow.beforeSwap");
                try {
                  ctl.step();
                  q.localPush(last);
                  ctl.padTo(2);
                } finally {
                  Delay.disarm();
                }
              })
          .thread(
              ctl -> {
                Delay.arm(ctl, "steal.read");
                try {
                  ctl.step();
                  try {
                    thiefBaseSeen[0] = Probes.base(q); // base the thief is about to read (no one else moves it)
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                  stolen[0] = q.tryToSteal();
                  ctl.padTo(2);
                } finally {
                  Delay.disarm();
                }
              })
          .run();
      NodeTask[] nq = rawArray(q);
      List<NodeTask> liveNow = live(q);
      Set<NodeTask> seen = idSet();
      seen.addAll(liveNow);
      if (stolen[0] != null) seen.add(stolen[0]);
      int missing = 0;
      List<String> missingInOld = new ArrayList<>();
      for (T t : all)
        if (!seen.contains(t)) {
          missing++;
          for (NodeTask o : oq) if (o == t) missingInOld.add(t.toString());
        }
      int base = Probes.base(q);
      boolean grew = nq.length > oq.length;
      boolean dup = stolen[0] != null && liveNow.contains(stolen[0]);
      boolean back = base < thiefBaseSeen[0];
      if (dup && resurrectDetail == null)
        resurrectDetail = "[" + sched + "] thief stole " + stolen[0] + " from the old array after the copy; the swap set _base=" + base
            + " (thief read base " + thiefBaseSeen[0] + " and set it to " + (thiefBaseSeen[0] + 1) + "), so " + stolen[0] + " is live again at the bottom of the new array";
      dropped |= missing > 0;
      baseBack |= back;
      resurrect |= dup;
      sb.append(" [").append(sched).append("] grew=").append(grew).append(" stolen=").append(stolen[0])
          .append(" base=").append(base).append(" top=").append(Probes.top(q)).append(" live=").append(liveNow.size())
          .append(missing > 0 ? " DROPPED " + missing + " (still in old array: " + missingInOld + ")" : "")
          .append(dup ? " RESURRECTED" : "").append(back ? " BASE<THIEF_BASE" : "").append(';');
    }
    String head =
        dropped || baseBack
            ? "hazard reproduced: dropped=" + dropped + " baseBelowThief=" + baseBack
            : "no task dropped and _base after swap >= thief's base in every schedule"
                + (resurrect ? "; but a stolen task is resurrected (double dispatch, allowed): " + resurrectDetail : "");
    return new Result("W1d", dropped || baseBack ? DEMONSTRATED : NOT_REPRODUCED, head + "; all schedules:" + sb);
  }

  // ---------- W1e: scanToFindWork/removeAt vs tryToSteal ----------
  static Result w1e() throws Exception {
    Result agent = requireAgent("W1e", "scanToFindWork:scan.beforeRemove", "tryToSteal:steal.read");
    if (agent != null) return agent;
    final int tid = 7, cid = 2;
    StringBuilder sb = new StringBuilder();
    boolean lostAny = false, holeAny = false, twice = false;
    String twiceDetail = null;
    for (String layout : new String[] {"mid", "base"}) {
      for (String sched : Steps.permutations(2, 2)) {
        var q = Probes.newQueue(10, 10);
        T m = new T(100); // matches: causality 2 >= 2, same thread
        m.setCausality(cid, tid);
        List<T> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
          T t;
          if (layout.equals("mid") ? i == 1 : i == 0) t = m;
          else {
            t = new T(i);
            t.setCausality(1, tid); // causality 1 < 2: not takeable
          }
          if (OGScheduler.canTake(t, tid, cid) != (t == m)) return new Result("W1e", ERROR, "canTake setup wrong for " + t);
          tasks.add(t);
          q.localPush(t);
        }
        NodeTask[] res = new NodeTask[2];
        Steps.schedule(sched)
            .thread(
                ctl -> {
                  Delay.arm(ctl, "scan.beforeRemove");
                  try {
                    ctl.step();
                    res[0] = q.scanToFindWork(tid, cid);
                    ctl.padTo(2);
                  } finally {
                    Delay.disarm();
                  }
                })
            .thread(
                ctl -> {
                  Delay.arm(ctl, "steal.read");
                  try {
                    ctl.step();
                    res[1] = q.tryToSteal();
                    ctl.padTo(2);
                  } finally {
                    Delay.disarm();
                  }
                })
            .run();
        List<NodeTask> liveNow = live(q);
        Set<NodeTask> seen = idSet();
        seen.addAll(liveNow);
        if (res[0] != null) seen.add(res[0]);
        if (res[1] != null) seen.add(res[1]);
        int lost = 0;
        for (T t : tasks) if (!seen.contains(t)) lost++;
        String h = hole(q);
        int mCount = (res[0] == m ? 1 : 0) + (res[1] == m ? 1 : 0) + (liveNow.contains(m) ? 1 : 0);
        boolean dup = mCount > 1;
        if (dup && twiceDetail == null)
          twiceDetail = "[" + layout + " " + sched + "] scanToFindWork -> " + res[0] + ", tryToSteal -> " + res[1] + ", left in queue " + liveNow
              + " (" + Probes.state(q) + "): removeAt saw index < _base and did nothing, scanToFindWork returned its stale read anyway";
        lostAny |= lost > 0;
        holeAny |= h != null;
        twice |= dup;
        sb.append(" [").append(layout).append(' ').append(sched).append("] scan=").append(res[0]).append(" steal=").append(res[1])
            .append(" left=").append(liveNow).append(" base=").append(Probes.base(q)).append(" top=").append(Probes.top(q))
            .append(lost > 0 ? " LOST " + lost : "").append(h != null ? " HOLE " + h : "").append(dup ? " M_TWICE" : "").append(';');
      }
    }
    String head =
        lostAny || holeAny
            ? "INVARIANT BROKEN: lost=" + lostAny + " hole=" + holeAny
            : twice ? "matched task handed out twice (no loss, no hole): " + twiceDetail : "matched task returned once, nothing lost, no hole, in every schedule";
    return new Result("W1e", lostAny || holeAny || twice ? DEMONSTRATED : NOT_REPRODUCED, head + "; all schedules:" + sb);
  }
}
