package optimus.graph;

import static org.openjdk.jcstress.annotations.Expect.*;

import optimus.graph.MC.T;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/** H1: jcstress tests for OGSchedulerContext.WorkQueue. */
public final class H1WorkQueue {
  private H1WorkQueue() {}

  /**
   * (a) One-element queue: owner localPop(0) vs thief tryToSteal. r1 = owner got it, r2 = thief got
   * it, r3 = final top-base, r4 = final isEmpty().
   */
  @JCStressTest
  @Outcome(id = "1, 0, 0, 1", expect = ACCEPTABLE, desc = "owner only")
  @Outcome(id = "0, 1, 0, 1", expect = ACCEPTABLE, desc = "thief only")
  @Outcome(id = "1, 1, 0, 1", expect = ACCEPTABLE_INTERESTING, desc = "both (R7 stale localPop return; tryRun CAS must arbitrate)")
  @Outcome(id = "1, 1, .*", expect = ACCEPTABLE_INTERESTING, desc = "both, with an unexpected final size")
  @Outcome(id = "0, 0, .*", expect = FORBIDDEN, desc = "task lost: neither got it")
  @Outcome(expect = FORBIDDEN, desc = "other: final state not empty/consistent")
  @State
  public static class PopVsSteal {
    final T t = new T(1);
    final OGSchedulerContext.WorkQueue q = MC.newQueue(10, 10);
    { q.localPush(t); }

    @Actor public void owner(IIII_Result r) { r.r1 = q.localPop(0) == t ? 1 : 0; }
    @Actor public void thief(IIII_Result r) { r.r2 = q.tryToSteal() == t ? 1 : 0; }
    @Arbiter public void after(IIII_Result r) {
      r.r3 = MC.top(q) - MC.base(q);
      r.r4 = q.isEmpty() ? 1 : 0;
    }
  }

  /**
   * (b) 63 tasks queued; owner pushes the 64th (growArray) while a thief steals. Arbiter counts, for
   * every task, thief-taken + occurrences in the live region of the current array. r1 = thief got a
   * task (index of it, -1 none), r2 = tasks lost (count 0), r3 = tasks present twice (count 2:
   * resurrected after steal, W1d), r4 = final size().
   */
  @JCStressTest
  @Outcome(id = "-1, 0, 0, 64", expect = ACCEPTABLE, desc = "no steal")
  @Outcome(id = "0, 0, 0, 63", expect = ACCEPTABLE, desc = "steal of T0, clean")
  @Outcome(id = "0, 0, 1, 64", expect = ACCEPTABLE_INTERESTING, desc = "T0 stolen from old array after copy, resurrected in new array (W1d)")
  @Outcome(id = "[-0-9]+, 0, .*", expect = ACCEPTABLE_INTERESTING, desc = "no loss, other shape")
  @Outcome(expect = FORBIDDEN, desc = "task lost")
  @State
  public static class GrowVsSteal {
    static final int N = 64;
    final T[] ts = new T[N];
    final OGSchedulerContext.WorkQueue q = MC.newQueue(0, 0);
    {
      for (int i = 0; i < N; i++) ts[i] = new T(i);
      for (int i = 0; i < N - 1; i++) q.localPush(ts[i]);
    }
    NodeTask stolen;

    @Actor public void owner() { q.localPush(ts[N - 1]); }
    @Actor public void thief() { stolen = q.tryToSteal(); }
    @Arbiter public void after(IIII_Result r) {
      int idx = -1;
      for (int i = 0; i < N; i++) if (ts[i] == stolen) idx = i;
      if (stolen != null && idx < 0) idx = -2;
      int lost = 0, dup = 0;
      for (int i = 0; i < N; i++) {
        int c = MC.countLive(q, ts[i]) + (ts[i] == stolen ? 1 : 0);
        if (c == 0) lost++;
        if (c > 1) dup++;
      }
      r.r1 = idx; r.r2 = lost; r.r3 = dup; r.r4 = q.size();
    }
  }

  // (c) scanToFindWork(7, 2) vs tryToSteal. Tasks T0..T2 pushed bottom to top; M is the only task
  // with a causality scanToFindWork may take. Result: r1 = scan got M, r2 = steal result (0 none, 1
  // M, 2 other), r3 = live tasks lost, r4 = null holes in [base, top).
  static final int TID = 7, CID = 2;

  abstract static class ScanBase {
    final T[] ts = new T[3];
    final T m;
    final OGSchedulerContext.WorkQueue q = MC.newQueue(10, 10);
    NodeTask scanned, stolen;

    ScanBase(int mAt) {
      for (int i = 0; i < 3; i++) {
        ts[i] = new T(i);
        ts[i].setCausality(i == mAt ? CID : 1, TID);
        q.localPush(ts[i]);
      }
      m = ts[mAt];
    }

    void result(IIII_Result r) {
      r.r1 = scanned == m ? 1 : scanned == null ? 0 : 2;
      r.r2 = stolen == m ? 1 : stolen == null ? 0 : 2;
      int lost = 0;
      for (T t : ts) if (MC.countLive(q, t) == 0 && t != scanned && t != stolen) lost++;
      r.r3 = lost;
      r.r4 = MC.nullsLive(q);
    }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "M handed out twice (W1e/R7)")
  @Outcome(id = "1, 0, 0, 0", expect = ACCEPTABLE, desc = "scan got M, thief got nothing")
  @Outcome(id = "0, 1, 0, 0", expect = ACCEPTABLE, desc = "thief got M, scan found nothing")
  @Outcome(id = "1, 2, 0, 0", expect = ACCEPTABLE, desc = "scan got M, thief got another task")
  @Outcome(id = "[0-2], [0-2], 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "no loss, other combination")
  @Outcome(id = "[0-2], [0-2], 0, [1-9]", expect = FORBIDDEN, desc = "null hole in live region")
  @Outcome(expect = FORBIDDEN, desc = "task lost")
  @State
  public static class ScanVsStealBase extends ScanBase {
    public ScanVsStealBase() { super(0); }
    @Actor public void scan() { scanned = q.scanToFindWork(TID, CID); }
    @Actor public void thief() { stolen = q.tryToSteal(); }
    @Arbiter public void after(IIII_Result r) { result(r); }
  }

  @JCStressTest
  @Outcome(id = "1, 2, 0, 0", expect = ACCEPTABLE, desc = "scan got M, thief got another task")
  @Outcome(id = "1, 1, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "M handed out twice")
  @Outcome(id = "0, 1, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "thief got M (after removeAt moved it to base?)")
  @Outcome(id = "[0-2], [0-2], 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "no loss, other combination")
  @Outcome(id = "[0-2], [0-2], 0, [1-9]", expect = FORBIDDEN, desc = "null hole in live region")
  @Outcome(expect = FORBIDDEN, desc = "task lost")
  @State
  public static class ScanVsStealMid extends ScanBase {
    public ScanVsStealMid() { super(1); }
    @Actor public void scan() { scanned = q.scanToFindWork(TID, CID); }
    @Actor public void thief() { stolen = q.tryToSteal(); }
    @Arbiter public void after(IIII_Result r) { result(r); }
  }

  /**
   * (d) Near Integer.MAX_VALUE: base = MAX-3 with T1, T2 queued (top = MAX-1). Owner pushes T3 (top
   * reaches MAX, resetBounds) then pops; two thieves steal. Owner push and pop share one actor
   * because _top is owner-only (a plain field): running them on separate threads would violate the
   * single-owner contract and any failure would be an artefact of the test. r1 = tasks lost, r2 =
   * tasks present more than once (handed out and/or still queued), r3 = final size(), r4 = isEmpty
   * check: 0 consistent, 1 isEmpty() true with live tasks queued (lie), 2 isEmpty() false with no live
   * task (phantom).
   */
  @JCStressTest
  @Outcome(id = "0, 0, [0-9]+, 0", expect = ACCEPTABLE, desc = "every task delivered or queued exactly once")
  @Outcome(id = "0, [1-9], [0-9]+, [02]", expect = ACCEPTABLE_INTERESTING, desc = "double hand-out, no loss")
  @Outcome(id = "0, 0, [0-9]+, 2", expect = ACCEPTABLE_INTERESTING, desc = "phantom: isEmpty() false with nothing live")
  @Outcome(expect = FORBIDDEN, desc = "loss, negative size or isEmpty() lying")
  @State
  public static class WrapPushPopVsTwoThieves {
    static final int MAX = Integer.MAX_VALUE;
    final T t1 = new T(1), t2 = new T(2), t3 = new T(3);
    final OGSchedulerContext.WorkQueue q = MC.newQueue(MAX - 3, MAX - 3);
    { q.localPush(t1); q.localPush(t2); }
    NodeTask popped, s1, s2;

    @Actor public void owner() { q.localPush(t3); popped = q.localPop(0); }
    @Actor public void thief1() { s1 = q.tryToSteal(); }
    @Actor public void thief2() { s2 = q.tryToSteal(); }
    @Arbiter public void after(IIII_Result r) {
      int lost = 0, dup = 0, live = 0;
      for (T t : new T[] {t1, t2, t3}) {
        int inQ = MC.countLive(q, t);
        live += inQ;
        int c = inQ + (popped == t ? 1 : 0) + (s1 == t ? 1 : 0) + (s2 == t ? 1 : 0);
        if (c == 0) lost++;
        if (c > 1) dup++;
      }
      r.r1 = lost; r.r2 = dup; r.r3 = q.size();
      boolean e = q.isEmpty();
      r.r4 = (e && live > 0) ? 1 : (!e && live == 0) ? 2 : 0;
    }
  }

  /**
   * (d') Tighter variant of (d): base = MAX-2 with only T1 queued (top = MAX-1). The owner's push of
   * T2 fires resetBounds with two tasks queued, and its pop then contends with both thieves for the
   * last remaining task. Same result encoding as (d).
   */
  @JCStressTest
  @Outcome(id = "0, 0, [0-9]+, 0", expect = ACCEPTABLE, desc = "every task delivered or queued exactly once")
  @Outcome(id = "0, [1-9], [0-9]+, [02]", expect = ACCEPTABLE_INTERESTING, desc = "double hand-out, no loss")
  @Outcome(id = "0, 0, [0-9]+, 2", expect = ACCEPTABLE_INTERESTING, desc = "phantom: isEmpty() false with nothing live")
  @Outcome(expect = FORBIDDEN, desc = "loss, negative size or isEmpty() lying")
  @State
  public static class WrapTightPushPopVsTwoThieves {
    static final int MAX = Integer.MAX_VALUE;
    final T t1 = new T(1), t2 = new T(2);
    final OGSchedulerContext.WorkQueue q = MC.newQueue(MAX - 2, MAX - 2);
    { q.localPush(t1); }
    NodeTask popped, s1, s2;

    @Actor public void owner() { q.localPush(t2); popped = q.localPop(0); }
    @Actor public void thief1() { s1 = q.tryToSteal(); }
    @Actor public void thief2() { s2 = q.tryToSteal(); }
    @Arbiter public void after(IIII_Result r) {
      int lost = 0, dup = 0, live = 0;
      for (T t : new T[] {t1, t2}) {
        int inQ = MC.countLive(q, t);
        live += inQ;
        int c = inQ + (popped == t ? 1 : 0) + (s1 == t ? 1 : 0) + (s2 == t ? 1 : 0);
        if (c == 0) lost++;
        if (c > 1) dup++;
      }
      r.r1 = lost; r.r2 = dup; r.r3 = q.size();
      boolean e = q.isEmpty();
      r.r4 = (e && live > 0) ? 1 : (!e && live == 0) ? 2 : 0;
    }
  }
}
