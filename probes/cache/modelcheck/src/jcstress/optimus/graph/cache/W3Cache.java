package optimus.graph.cache;

import static optimus.graph.cache.CacheMC.*;
import static org.openjdk.jcstress.annotations.Expect.*;

import optimus.graph.PropertyNode;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * W3 (W1, W2, R16, S4/OPTIMUS-57931): jcstress on NodeCacheWithLRU. Keys are fixture PropertyNodes (CacheMC.key)
 * on one root stack under the Basic policy. "C1" tests use UNodeCache(name, 16) (concurrency 1: no ledgers, every
 * touch takes updateLock); "L2" tests use concurrency 2, batch 1, padding 1 (ledgers that catch up on the second
 * record). Presence in arbiters is read with CacheMC.liveFor (walks the current table, no touch) unless stated.
 */
public final class W3Cache {
  private W3Cache() {}

  static Long dummy;

  /**
   * (a) Two actors putIfAbsent fresh, equal keys (serials 1 and 2). r1/r2 = serial of the node each got; r3 = live
   * non-proxy entries for the key in the table; r4 = serial getIfPresent returns afterwards (-1 miss).
   */
  @JCStressTest
  @Outcome(id = {"1, 1, 1, 1", "2, 2, 1, 2"}, expect = ACCEPTABLE, desc = "one insert; the loser found the winner")
  @Outcome(id = {"1, 2, .*", "2, 1, .*"}, expect = FORBIDDEN, desc = "each kept a different node (W1 broken)")
  @Outcome(expect = FORBIDDEN, desc = "two live nodes, or lookup miss / wrong node")
  @State
  public static class A_SameKey {
    final UNodeCache c = cache(16);
    final PropertyNode<Long> ka = key(5, 1), kb = key(5, 2);

    @Actor public void a(IIII_Result r) { r.r1 = serial(put(c, ka)); }
    @Actor public void b(IIII_Result r) { r.r2 = serial(put(c, kb)); }
    @Arbiter public void after(IIII_Result r) {
      r.r3 = liveFor(c, ka);
      r.r4 = serial(get(c, key(5)));
    }
  }

  static final int ARG_A = 5, ARG_B = collidingArg(ARG_A, 16, 6), ARG_C = collidingArg(ARG_A, 16, ARG_B + 1);

  /**
   * (b) Two actors putIfAbsent different keys that share a bucket (args chosen at class init so hashOf & 15 match).
   * r1/r2 = serial each put returned (own: 1 and 2); r3/r4 = getIfPresent of each afterwards hits (1) or misses (0).
   */
  @JCStressTest
  @Outcome(id = "1, 2, 1, 1", expect = ACCEPTABLE, desc = "both inserted, both found")
  @Outcome(expect = FORBIDDEN, desc = "an insert lost or a put returned a foreign node")
  @State
  public static class B_SameBucket {
    final UNodeCache c = cache(16);
    final PropertyNode<Long> ka = key(ARG_A, 1), kb = key(ARG_B, 2);

    @Actor public void a(IIII_Result r) { r.r1 = serial(put(c, ka)); }
    @Actor public void b(IIII_Result r) { r.r2 = serial(put(c, kb)); }
    @Arbiter public void after(IIII_Result r) {
      r.r3 = get(c, key(ARG_A)) == ka ? 1 : 0;
      r.r4 = get(c, key(ARG_B)) == kb ? 1 : 0;
    }
  }

  /** (b') As (b), with a third colliding key already at the head of the bucket. */
  @JCStressTest
  @Outcome(id = "1, 2, 1, 1", expect = ACCEPTABLE, desc = "both inserted, both found")
  @Outcome(expect = FORBIDDEN, desc = "an insert lost or a put returned a foreign node")
  @State
  public static class B_SameBucketNonEmpty {
    final UNodeCache c = cache(16);
    final PropertyNode<Long> ka = key(ARG_A, 1), kb = key(ARG_B, 2);
    { put(c, key(ARG_C, 3)); }

    @Actor public void a(IIII_Result r) { r.r1 = serial(put(c, ka)); }
    @Actor public void b(IIII_Result r) { r.r2 = serial(put(c, kb)); }
    @Arbiter public void after(IIII_Result r) {
      r.r3 = get(c, key(ARG_A)) == ka ? 1 : 0;
      r.r4 = get(c, key(ARG_B)) == kb ? 1 : 0;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // (c) putIfAbsent vs clear(), four keys prefilled. r1 = put returned its own key; r2 = new key live in the table;
  // r3 = prefilled keys still live; r4 = entries in the LRU but not the table; r5 = in the table but not the LRU;
  // r6 = size field minus live LRU length (after flushing ledgers).

  static void prefill(UNodeCache c, int n) {
    for (int i = 0; i < n; i++) put(c, key(i, 100 + i));
    c.getSize(); // flush ledgers
  }

  static int live(UNodeCache c, int from, int to) {
    int n = 0;
    for (int i = from; i < to; i++) n += liveFor(c, key(i)) > 0 ? 1 : 0;
    return n;
  }

  static void desyncInto(UNodeCache c, IIIIII_Result r) {
    int[] d = desync(c);
    r.r4 = d[0];
    r.r5 = d[1];
    r.r6 = d[2];
  }

  public abstract static class PutVsClear {
    final UNodeCache c = make();
    final PropertyNode<Long> k = key(10, 1);
    { prefill(c, 4); }

    abstract UNodeCache make();

    void put1(IIIIII_Result r) { r.r1 = serial(put(c, k)); }
    void clear1() { clear(c); }
    void after(IIIIII_Result r) {
      desyncInto(c, r); // flushes the ledgers first
      r.r2 = liveFor(c, k);
      r.r3 = live(c, 0, 4);
    }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "put after clear: only the new key")
  @Outcome(id = "1, 0, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "put before clear: empty")
  @Outcome(expect = FORBIDDEN, desc = "LRU/table desync, size drift, or a prefilled key survived clear")
  @State
  public static class C_PutVsClear_C1 extends PutVsClear {
    UNodeCache make() { return cache(16); }
    @Actor public void a(IIIIII_Result r) { put1(r); }
    @Actor public void b() { clear1(); }
    @Arbiter public void arb(IIIIII_Result r) { after(r); }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "put after clear: only the new key")
  @Outcome(id = "1, 0, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "put before clear: empty")
  @Outcome(expect = FORBIDDEN, desc = "LRU/table desync, size drift, or a prefilled key survived clear")
  @State
  public static class C_PutVsClear_L2 extends PutVsClear {
    UNodeCache make() { return cache(16, 2, 1, 1); }
    @Actor public void a(IIIIII_Result r) { put1(r); }
    @Actor public void b() { clear1(); }
    @Arbiter public void arb(IIIIII_Result r) { after(r); }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // (d) putIfAbsent vs setMaxSize(32) vs setMaxSize(16), eight keys prefilled. r1 = put returned its own key;
  // r2 = new key live in the current table; r3 = prefilled keys missing from the table; r4/r5/r6 as (c).

  public abstract static class PutVsResize {
    final UNodeCache c = make();
    final PropertyNode<Long> k = key(10, 1);
    { prefill(c, 8); }

    abstract UNodeCache make();

    void put1(IIIIII_Result r) { r.r1 = serial(put(c, k)); }
    void grow() { c.setMaxSize(32); }
    void shrink() { c.setMaxSize(16); }
    void after(IIIIII_Result r) {
      desyncInto(c, r);
      r.r2 = liveFor(c, k);
      r.r3 = 8 - live(c, 0, 8);
    }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "consistent")
  @Outcome(id = "1, 0, 0, 1, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "OPTIMUS-57931: insert landed in a retired table; in LRU, not in table")
  @Outcome(expect = FORBIDDEN, desc = "other desync, loss of a prefilled key, or size drift")
  @State
  public static class D_PutVsResize_C1 extends PutVsResize {
    UNodeCache make() { return cache(16); }
    @Actor public void a(IIIIII_Result r) { put1(r); }
    @Actor public void b() { grow(); }
    @Actor public void c() { shrink(); }
    @Arbiter public void arb(IIIIII_Result r) { after(r); }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "consistent")
  @Outcome(id = "1, 0, 0, 1, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "OPTIMUS-57931: insert landed in a retired table; in LRU, not in table")
  @Outcome(expect = FORBIDDEN, desc = "other desync, loss of a prefilled key, or size drift")
  @State
  public static class D_PutVsResize_L2 extends PutVsResize {
    UNodeCache make() { return cache(16, 2, 1, 1); }
    @Actor public void a(IIIIII_Result r) { put1(r); }
    @Actor public void b() { grow(); }
    @Actor public void c() { shrink(); }
    @Arbiter public void arb(IIIIII_Result r) { after(r); }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // (e) touch vs evict: 16 keys (0 oldest .. 15) fill a 16-entry cache. Actor 1 looks up key 0 (getIfPresent, a hit
  // moves it to the LRU head); actor 2 inserts key 16, which evicts the LRU tail. r1 = lookup hit; r2 = key 0 live
  // afterwards; r3 = keys 1..16 missing; r4/r5/r6 as (c) (r6 = size field minus live LRU length).

  public abstract static class TouchVsEvict {
    final UNodeCache c = make();
    final PropertyNode<Long> k0 = key(0), k16 = key(16, 116);
    { prefill(c, 16); }

    abstract UNodeCache make();

    void lookup(IIIIII_Result r) { r.r1 = get(c, k0) != null ? 1 : 0; }
    void insert() { put(c, k16); }
    void after(IIIIII_Result r) {
      desyncInto(c, r);
      r.r2 = liveFor(c, k0);
      r.r3 = 16 - live(c, 1, 17);
    }
  }

  @JCStressTest
  @Outcome(id = "1, 1, 1, 0, 0, 0", expect = ACCEPTABLE, desc = "touch first: key 0 kept, key 1 evicted")
  @Outcome(id = "0, 0, 0, 0, 0, 0", expect = ACCEPTABLE, desc = "evict first: lookup missed")
  @Outcome(id = "1, 0, 0, 0, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "stale hit then evicted (touch lost to the eviction; R16 indistinguishable)")
  @Outcome(expect = FORBIDDEN, desc = "miss on a live key, double eviction, desync or size drift")
  @State
  public static class E_TouchVsEvict_C1 extends TouchVsEvict {
    UNodeCache make() { return cache(16); }
    @Actor public void a(IIIIII_Result r) { lookup(r); }
    @Actor public void b() { insert(); }
    @Arbiter public void arb(IIIIII_Result r) { after(r); }
  }

  /**
   * (e) with ledgers. One insert only lands in a ledger (no catch-up, so no eviction, observed 100% in a first run),
   * so actor 2 inserts keys 16 and 17: its second record triggers catchupCore + removeOverflow. r3 = keys 1..17
   * missing. The lookup's touch may sit in a ledger past the eviction, which is the approximate-LRU case.
   */
  @JCStressTest
  @Outcome(id = "1, 1, [0-2], 0, 0, 0", expect = ACCEPTABLE, desc = "hit; key 0 kept")
  @Outcome(id = "0, 0, [0-2], 0, 0, 0", expect = ACCEPTABLE, desc = "evicted first: lookup missed")
  @Outcome(id = "1, 0, [0-2], 0, 0, 0", expect = ACCEPTABLE_INTERESTING, desc = "stale hit then evicted (touch lost to the eviction; R16 indistinguishable)")
  @Outcome(expect = FORBIDDEN, desc = "miss on a live key, over-eviction, desync or size drift")
  @State
  public static class E_TouchVsEvict_L2 {
    final UNodeCache c = cache(16, 2, 1, 1);
    final PropertyNode<Long> k0 = key(0), k16 = key(16, 116), k17 = key(17, 117);
    { prefill(c, 16); }

    @Actor public void a(IIIIII_Result r) { r.r1 = get(c, k0) != null ? 1 : 0; }
    @Actor public void b() { put(c, k16); put(c, k17); }
    @Arbiter public void arb(IIIIII_Result r) {
      desyncInto(c, r);
      r.r2 = liveFor(c, k0);
      r.r3 = 17 - live(c, 1, 18);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // (f) Found by H1 (Lincheck, lax=clear,resize): the inLedger shortcut lets an entry escape clear(). Actor A inserts
  // k, then clears, then reads whether k is still live in the table (B cannot insert, so a live k means it survived
  // a clear that started after A's own put returned). Actor B looks k up; a hit's touchEntry sets inLedger = true
  // before appending to its ledger. If that lands between A's CAS and A's own touchEntry, A's touch returns early
  // (entry.inLedger), and if A's clear then runs while B's record is not yet appended, catchupCore skips B's ledger
  // (count == 0 peek) and the LRU walk never sees k. r1 = k live after A's clear; r2 = B hit; r3..r5 = desync
  // after flushing (LRU-not-table, table-not-LRU, size drift).

  public static final class PutClearVsHit {
    final UNodeCache c;
    final PropertyNode<Long> k = key(7, 1);
    PutClearVsHit(UNodeCache c) { this.c = c; }
    void a(IIIII_Result r) { put(c, k); clear(c); r.r1 = liveFor(c, k); }
    void b(IIIII_Result r) { r.r2 = get(c, key(7)) != null ? 1 : 0; }
    void arb(IIIII_Result r) { int[] d = desync(c); r.r3 = d[0]; r.r4 = d[1]; r.r5 = d[2]; }
  }

  @JCStressTest
  @Outcome(id = "0, [01], 0, 0, 0", expect = ACCEPTABLE, desc = "cleared")
  @Outcome(id = "1, 1, 0, 0, 0", expect = FORBIDDEN, desc = "k survived a clear that started after its put returned (inLedger race)")
  @Outcome(expect = FORBIDDEN, desc = "other")
  @State
  public static class F_PutClearVsHit_L2 {
    final PutClearVsHit t = new PutClearVsHit(cache(16, 2, 1, 1));
    @Actor public void a(IIIII_Result r) { t.a(r); }
    @Actor public void b(IIIII_Result r) { t.b(r); }
    @Arbiter public void arb(IIIII_Result r) { t.arb(r); }
  }

  @JCStressTest
  @Outcome(id = "0, [01], 0, 0, 0", expect = ACCEPTABLE, desc = "cleared")
  @Outcome(id = "1, 1, 0, 0, 0", expect = FORBIDDEN, desc = "k survived a clear that started after its put returned")
  @Outcome(expect = FORBIDDEN, desc = "other")
  @State
  public static class F_PutClearVsHit_C1 {
    final PutClearVsHit t = new PutClearVsHit(cache(16));
    @Actor public void a(IIIII_Result r) { t.a(r); }
    @Actor public void b(IIIII_Result r) { t.b(r); }
    @Arbiter public void arb(IIIII_Result r) { t.arb(r); }
  }
}
