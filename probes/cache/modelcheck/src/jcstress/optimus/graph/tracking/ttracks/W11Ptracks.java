package optimus.graph.tracking.ttracks;

import static org.openjdk.jcstress.annotations.Expect.*;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * W11 (D12, ring 4b S9): PtracksArray.addCaller on the last slot vs the resize it forces. The array has capacity 8
 * with slots 0..6 filled (nextFree 7). Two actors add distinct callers A and B: one lands in slot 7, the other must
 * grow the array (Arrays.copyOf of a copy whose slot 7 is only ordered by the other thread's slot CAS and a plain
 * nextFree store). r1 = nextFree; r2 = capacity; r3 = null slots below nextFree; r4 = A present (1) + B present (2).
 */
public final class W11Ptracks {
  private W11Ptracks() {}

  static final TTrack[] POOL = new TTrack[9];
  static { for (int i = 0; i < POOL.length; i++) POOL[i] = new TTrack(); }

  @JCStressTest
  @Outcome(id = "9, 16, 0, 3", expect = ACCEPTABLE, desc = "both callers present, no gap")
  @Outcome(id = "9, 16, 1, [12]", expect = FORBIDDEN, desc = "D12: resize copied a null last slot; one caller lost behind a gap")
  @Outcome(expect = FORBIDDEN, desc = "other: gap, lost caller or bad size")
  @State
  public static class LastSlotVsResize {
    final TTrack owner = new TTrack();
    final PtracksArray arr = new PtracksArray(POOL[0], POOL[1], POOL[2]);
    {
      for (int i = 3; i < 7; i++) arr.addCaller(owner, POOL[i], 0);
      owner.ptracks = arr;
    }

    @Actor public void a() { arr.addCaller(owner, POOL[7], 0); }
    @Actor public void b() { arr.addCaller(owner, POOL[8], 0); }
    @Arbiter public void after(IIII_Result r) {
      int n = arr.currentSize();
      r.r1 = n;
      r.r2 = arr.currentCapacity();
      int nulls = 0, present = 0;
      for (int i = 0; i < n; i++) {
        TTrack t = arr.at(i);
        if (t == null) nulls++;
        else if (t == POOL[7]) present |= 1;
        else if (t == POOL[8]) present |= 2;
      }
      r.r3 = nulls;
      r.r4 = present;
    }
  }
}
