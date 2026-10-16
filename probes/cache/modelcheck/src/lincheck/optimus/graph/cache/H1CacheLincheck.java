package optimus.graph.cache;

import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import optimus.graph.PropertyNode;
import org.jetbrains.kotlinx.lincheck.*;
import org.jetbrains.kotlinx.lincheck.annotations.*;
import org.jetbrains.kotlinx.lincheck.execution.*;
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen;
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions;
import org.jetbrains.kotlinx.lincheck.verifier.Verifier;

/**
 * H1: Lincheck model check of NodeCacheWithLRU (a UNodeCache, maxSize 16, by default concurrency 2, batch 1,
 * padding 1, so the ledgers catch up on every second record) against a memo-table spec.
 *
 * <p>Operations: putIfAbsent(key 0..7), getIfPresent(key 0..7), clear(), setMaxSize(16 | 32). Keys are fixture
 * PropertyNodes from a static pool (pool[k][j], serial k*16+j; each putIfAbsent takes the next fresh key for k, so
 * equal keys are distinct objects); putIfAbsent returns OWN+serial when it inserted its own key, else the serial of
 * the node it got; getIfPresent returns the serial or -1.
 *
 * <p>The cache is at-least-once by design (setMaxSize swaps the table before moving entries, NodeCacheWithLRU
 * "false negative cache hits ... ok to execute a node twice"), so a deterministic sequential spec cannot describe
 * it. {@link MemoVerifier} searches for a linearization (respecting Lincheck's happens-before clocks) against a
 * nondeterministic spec: per key, the set of live nodes; putIfAbsent returns a live node or, if none is live,
 * inserts its own; getIfPresent returns a live node, or misses only if none is live; clear empties every key. A
 * setMaxSize is an interval [begin, end] (two spec events, other threads' events may fall in between); inside it a
 * lookup may miss and a put may insert a second live node for a key (the documented at-least-once window). Once
 * more entries than maxSize have been inserted since the last clear, any key may have been evicted (miss allowed,
 * key dropped) and a put may then insert a second node; with 8 keys this needs 9+ duplicates, so in practice the
 * spec never allows eviction. After the post part, the real cache's LRU and table must agree (desync via
 * @Validate: LRU-not-table, table-not-LRU, size drift all 0).
 *
 * <p>Relaxations (system properties), each documenting a known deviation so the search can go past it:
 * <ul>
 *   <li>mc.lax=clear: a put is two events, insert (visible) then commit (in the LRU); clear only removes committed
 *       entries, so an insert in flight survives a concurrent clear.
 *   <li>mc.lax=resize: a put inside a setMaxSize interval may be lost (lands in the retired table: OPTIMUS-57931),
 *       and the final check allows LRU-not-table entries.
 *   <li>mc.lax=ledger: a hit is two events, hit then unpin; while pinned, clear spares the entry. Models the
 *       touchEntry race found by this check: a hitter sets inLedger = true before its ledger append, the inserter's
 *       own touch then returns early ("if (entry.inLedger) return"), and clear's catchupCore skips the hitter's ledger
 *       (count still 0), so the entry is in neither the LRU nor a ledger and survives clear.
 *   <li>mc.lax=sweep: clear is an interval [begin, end]; at begin it fixes the entries it will remove (in the LRU,
 *       not pinned), and until end a lookup may already miss them (clear walks the LRU removing one entry at a time,
 *       so readers see a partially cleared cache).
 *   <li>Lost inserts (lax=resize) are "ghosts": a thread still on the retired table may hit them until the resize ends.
 *   <li>Relaxations combine: mc.lax=clear,resize,ledger.
 *   <li>mc.noAtLeastOnce=true: control; no miss/duplicate window inside setMaxSize (expected to fail).
 * </ul>
 * Weakness: the at-least-once window is the whole setMaxSize call, so a miss or duplicate that overlaps it is
 * accepted even if the implementation could only produce it in a narrower window; eviction is modelled as "anything
 * goes" once over capacity.
 */
@Param(name = "key", gen = IntGen.class, conf = "0:7")
@Param(name = "size", gen = IntGen.class, conf = "0:1")
public class H1CacheLincheck {
  static final int KEYS = 8, PER_KEY = 16, OWN = 1000;
  /**
   * Keys live in a holder class rather than in statics of the test class: in lazy mode Lincheck walks the object
   * graph reachable from the test instance and its class's statics, and that walk dies on record classes.
   */
  static final class Keys {
    static final PropertyNode<Long>[][] POOL = pool();
    static final PropertyNode<Long>[] LOOKUP = lookups();
  }

  @SuppressWarnings("unchecked")
  static PropertyNode<Long>[][] pool() {
    var p = new PropertyNode[KEYS][PER_KEY];
    for (int k = 0; k < KEYS; k++) for (int j = 0; j < PER_KEY; j++) p[k][j] = new MCKey(k, k * PER_KEY + j);
    return p;
  }

  @SuppressWarnings("unchecked")
  static PropertyNode<Long>[] lookups() {
    var p = new PropertyNode[KEYS];
    for (int k = 0; k < KEYS; k++) p[k] = new MCKey(k, -1);
    return p;
  }

  final UNodeCache c;
  final AtomicIntegerArray next = new AtomicIntegerArray(KEYS);

  public H1CacheLincheck() {
    c = CacheMC.cache(16, Integer.getInteger("mc.concurrency", 2), Integer.getInteger("mc.batch", 1), Integer.getInteger("mc.padding", 1));
  }

  @Operation
  public int putIfAbsent(@Param(name = "key") int k) {
    int j = next.getAndIncrement(k);
    if (j >= PER_KEY) throw new IllegalStateException("key pool exhausted");
    var key = Keys.POOL[k][j];
    var r = CacheMC.put(c, key);
    return r == key ? OWN + CacheMC.serial(r) : CacheMC.serial(r);
  }

  @Operation
  public int getIfPresent(@Param(name = "key") int k) { return CacheMC.serial(CacheMC.get(c, Keys.LOOKUP[k])); }

  @Operation
  public void clear() { CacheMC.clear(c); }

  @Operation
  public void setMaxSize(@Param(name = "size") int which) { c.setMaxSize(which == 0 ? 16 : 32); }

  /**
   * Final-state check, run by Lincheck after the execution: flushes the
   * ledgers, then the LRU and the table must agree and the size field must equal the live LRU length. With
   * mc.lax=resize, LRU-not-table entries (OPTIMUS-57931) are tolerated.
   */
  @Validate
  public void validate() {
    int[] d = CacheMC.desync(c);
    if ((d[0] != 0 && !MemoVerifier.LAX_RESIZE) || d[1] != 0 || d[2] != 0) {
      List<String> es = new ArrayList<>();
      for (NCEntry e : CacheMC.tableEntries(c)) es.add(String.valueOf(e.value));
      Collections.sort(es);
      throw new IllegalStateException("desync: LRU-not-table=" + d[0] + " table-not-LRU=" + d[1] + " sizeDrift=" + d[2]
          + " table=" + CacheMC.table(c).length + " entries=" + es);
    }
  }

  // ------------------------------------------------------------------------------------------
  public static final class MemoVerifier implements Verifier {
    static long verified, usedWindow, usedLax;
    static final Set<String> LAX = new HashSet<>(Arrays.asList(System.getProperty("mc.lax", "").split(",")));
    static final boolean LAX_CLEAR = LAX.contains("clear"), LAX_RESIZE = LAX.contains("resize"), LAX_LEDGER = LAX.contains("ledger"), LAX_SWEEP = LAX.contains("sweep");
    static final boolean NO_ALO = Boolean.getBoolean("mc.noAtLeastOnce");

    public MemoVerifier(Class<?> seqSpec) {}

    /** Spec events. kind: I insert own (k,j), H hit (k,v), G get (k, v or -1), C clear, M commit (k,j), B/E resize begin/end (n). */
    record Ev(char kind, int k, int v) {}

    record Op(List<Ev> evs, HBClock clock) {}

    static final class Spec {
      final long[] live, uncommitted, ghost; // ghost: lost into a retired table, visible only during a resize
      final long[] doomed; // lax=sweep: entries a clear in progress will remove (it walks the LRU one entry at a time)
      final int[] pins; // per serial: hitters between inLedger = true and their ledger append (lax=ledger)
      final int entries, max, resizing;
      final boolean evictable, window, lax;

      Spec(long[] live, long[] uncommitted, int entries, int max, int resizing, boolean evictable, boolean window, boolean lax) {
        this(live, uncommitted, entries, max, resizing, evictable, window, lax, new long[KEYS], new int[KEYS * PER_KEY], new long[KEYS]);
      }

      Spec(long[] live, long[] uncommitted, int entries, int max, int resizing, boolean evictable, boolean window, boolean lax, long[] ghost, int[] pins, long[] doomed) {
        this.doomed = doomed;
        this.live = live; this.uncommitted = uncommitted; this.entries = entries; this.max = max;
        this.resizing = resizing; this.evictable = evictable; this.window = window; this.lax = lax;
        this.ghost = ghost; this.pins = pins;
      }

      Spec with(long[] live, long[] unc, int entries, int max, int resizing, boolean evictable, boolean window, boolean lax) {
        if (entries > max) evictable = true;
        return new Spec(live, unc, entries, max, resizing, evictable, this.window | window, this.lax | lax, ghost, pins, doomed);
      }

      Spec withDoomed(long[] l, long[] d, boolean lax) {
        int n = 0;
        for (long x : l) n += Long.bitCount(x);
        return new Spec(l, uncommitted, n, max, resizing, evictable, window, this.lax | lax, ghost, pins, d);
      }

      boolean allDoomed(int k) { return LAX_SWEEP && live[k] != 0 && (live[k] & ~doomed[k]) == 0; }

      Spec withLax(boolean l) { return new Spec(live, uncommitted, entries, max, resizing, evictable, window, l, ghost, pins, doomed); }

      Spec withGhost(long[] g) { return new Spec(live, uncommitted, entries, max, resizing, evictable, window, true, g, pins, doomed); }

      Spec pin(int serial, int d) {
        int[] p = pins.clone();
        p[serial] += d;
        return new Spec(live, uncommitted, entries, max, resizing, evictable, window, lax, ghost, p, doomed);
      }

      long pinned(int k) {
        long m = 0;
        for (int j = 0; j < PER_KEY; j++) if (pins[k * PER_KEY + j] > 0) m |= 1L << j;
        return m;
      }

      @Override public boolean equals(Object o) {
        return o instanceof Spec s && Arrays.equals(live, s.live) && Arrays.equals(uncommitted, s.uncommitted)
            && entries == s.entries && max == s.max && resizing == s.resizing && evictable == s.evictable
            && window == s.window && lax == s.lax && Arrays.equals(ghost, s.ghost) && Arrays.equals(pins, s.pins)
            && Arrays.equals(doomed, s.doomed);
      }

      @Override public int hashCode() {
        return Objects.hash(Arrays.hashCode(live), Arrays.hashCode(uncommitted), entries, max, resizing, evictable, window, lax, Arrays.hashCode(ghost), Arrays.hashCode(pins), Arrays.hashCode(doomed));
      }

      @Override public String toString() { return Arrays.toString(live) + "/" + entries + "/" + max + "/" + resizing + (evictable ? "E" : ""); }
    }

    static long[] set(long[] a, int k, long v) { long[] b = a.clone(); b[k] = v; return b; }

    static List<Spec> step(Spec s, Ev e) {
      List<Spec> r = new ArrayList<>(2);
      boolean win = s.resizing > 0 && !NO_ALO;
      switch (e.kind) {
        case 'I' -> {
          int k = e.k; long bit = 1L << e.v;
          long unc = LAX_CLEAR ? s.uncommitted[k] | bit : s.uncommitted[k];
          if (s.live[k] == 0)
            r.add(s.with(set(s.live, k, bit), set(s.uncommitted, k, unc), s.entries + 1, s.max, s.resizing, s.evictable, false, false));
          else {
            if (win) r.add(s.with(set(s.live, k, s.live[k] | bit), set(s.uncommitted, k, unc), s.entries + 1, s.max, s.resizing, s.evictable, true, false));
            if (s.evictable) r.add(s.with(set(s.live, k, bit), set(s.uncommitted, k, unc), s.entries + 1, s.max, s.resizing, true, false, false));
            if (s.allDoomed(k)) {
              Spec swept = s.withDoomed(set(s.live, k, 0), set(s.doomed, k, 0), true);
              r.add(swept.with(set(swept.live, k, bit), set(swept.uncommitted, k, unc), swept.entries + 1, swept.max, swept.resizing, swept.evictable, false, false));
            }
          }
          if (LAX_RESIZE && win) r.add(s.with(s.live, s.uncommitted, s.entries + 1, s.max, s.resizing, s.evictable, false, true).withGhost(set(s.ghost, k, s.ghost[k] | bit)));
        }
        case 'M' -> r.add(s.with(s.live, set(s.uncommitted, e.k, s.uncommitted[e.k] & ~(1L << e.v)), s.entries, s.max, s.resizing, s.evictable, false, false));
        case 'H', 'G' -> {
          int k = e.k;
          if (e.v == -1) {
            if (s.live[k] == 0) r.add(s);
            else {
              if (s.allDoomed(k)) r.add(s.withDoomed(set(s.live, k, 0), set(s.doomed, k, 0), true));
              if (win) r.add(s.with(s.live, s.uncommitted, s.entries, s.max, s.resizing, s.evictable, true, false));
              if (s.evictable) {
                r.add(s);
                r.add(s.with(set(s.live, k, 0), set(s.uncommitted, k, 0), s.entries, s.max, s.resizing, true, false, false));
              }
            }
          } else if (e.v >= 0 && e.v / PER_KEY == k) {
            long bit = 1L << (e.v % PER_KEY);
            boolean ok = (s.live[k] & bit) != 0;
            boolean ghostOk = !ok && s.resizing > 0 && (s.ghost[k] & bit) != 0;
            if (ok || ghostOk) r.add(LAX_LEDGER ? s.pin(e.v, 1) : ghostOk ? s.withGhost(s.ghost) : s);
          }
        }
        case 'X' -> { // clear begins: what it will remove is fixed now (catchupCore, then the LRU walk)
          long[] d = new long[KEYS];
          for (int k = 0; k < KEYS; k++) d[k] = s.doomed[k] | (s.live[k] & ~(s.uncommitted[k] | s.pinned(k)));
          r.add(new Spec(s.live, s.uncommitted, s.entries, s.max, s.resizing, s.evictable, s.window, s.lax, s.ghost, s.pins, d));
        }
        case 'Y' -> { // clear ends
          long[] l = new long[KEYS];
          boolean lax = false;
          for (int k = 0; k < KEYS; k++) { l[k] = s.live[k] & ~s.doomed[k]; lax |= l[k] != 0; }
          Spec n = s.withDoomed(l, new long[KEYS], lax);
          r.add(new Spec(n.live, n.uncommitted, n.entries, n.max, n.resizing, false, n.window, n.lax, n.ghost, n.pins, n.doomed));
        }
        case 'U' -> r.add(s.pin(e.v, -1).withLax(s.lax));
        case 'C' -> {
          long[] l = new long[KEYS];
          int n = 0;
          for (int k = 0; k < KEYS; k++) { l[k] = s.live[k] & (s.uncommitted[k] | s.pinned(k)); n += Long.bitCount(l[k]); }
          boolean lax = !Arrays.equals(l, new long[KEYS]);
          r.add(new Spec(l, s.uncommitted.clone(), n, s.max, s.resizing, false, s.window, s.lax | lax, s.ghost, s.pins, s.doomed));
        }
        case 'B' -> r.add(s.with(s.live, s.uncommitted, s.entries, s.max, s.resizing + 1, s.entries > e.v, false, false));
        case 'E' -> {
          Spec n = s.with(s.live, s.uncommitted, s.entries, e.v, s.resizing - 1, s.evictable, false, false);
          r.add(n.resizing == 0 ? new Spec(n.live, n.uncommitted, n.entries, n.max, 0, n.evictable, n.window, n.lax, new long[KEYS], n.pins, n.doomed) : n);
        }
        default -> throw new IllegalStateException("" + e.kind);
      }
      return r;
    }

    static Op op(Actor a, Result res, HBClock clock) {
      String n = a.getMethod().getName();
      Object arg = a.getArguments().isEmpty() ? null : a.getArguments().get(0);
      int k = arg == null ? -1 : (Integer) arg;
      Integer v = res instanceof ValueResult vr ? (Integer) vr.getValue() : null;
      if (!(res instanceof ValueResult) && !(res instanceof VoidResult)) v = Integer.MIN_VALUE; // exception: matches nothing
      List<Ev> evs = switch (n) {
        case "putIfAbsent" -> {
          if (v == Integer.MIN_VALUE) yield List.of(new Ev('G', k, -2));
          if (v >= OWN) {
            int sj = v - OWN;
            yield LAX_CLEAR ? List.of(new Ev('I', k, sj % PER_KEY), new Ev('M', k, sj % PER_KEY)) : List.of(new Ev('I', k, sj % PER_KEY));
          }
          yield LAX_LEDGER ? List.of(new Ev('H', k, v), new Ev('U', k, v)) : List.of(new Ev('H', k, v));
        }
        case "getIfPresent" -> {
          int g = v == Integer.MIN_VALUE ? -2 : v;
          yield LAX_LEDGER && g >= 0 ? List.of(new Ev('G', k, g), new Ev('U', k, g)) : List.of(new Ev('G', k, g));
        }
        case "clear" -> LAX_SWEEP ? List.of(new Ev('X', -1, 0), new Ev('Y', -1, 0)) : List.of(new Ev('C', -1, 0));
        case "setMaxSize" -> { int sz = k == 0 ? 16 : 32; yield List.of(new Ev('B', -1, sz), new Ev('E', -1, sz)); }
        default -> throw new IllegalStateException(n);
      };
      return new Op(evs, clock);
    }

    static List<Op> ops(List<Actor> actors, List<? extends Result> results) {
      List<Op> l = new ArrayList<>();
      for (int i = 0; i < actors.size(); i++) l.add(op(actors.get(i), results.get(i), null));
      return l;
    }

    static void runSeq(Spec s, List<Op> ops, Set<Spec> out) {
      Set<Spec> cur = Set.of(s);
      for (Op o : ops) for (Ev e : o.evs) {
        Set<Spec> nxt = new HashSet<>();
        for (Spec x : cur) nxt.addAll(step(x, e));
        cur = nxt;
      }
      out.addAll(cur);
    }

    /** pos[t] = completed ops of thread t; ev[t] = events of op pos[t] already applied. */
    static void linearize(Spec s, List<List<Op>> par, int[] pos, int[] ev, Set<Spec> out, Set<String> seen) {
      if (!seen.add(Arrays.toString(pos) + Arrays.toString(ev) + s.hashCode() + s)) return;
      boolean done = true;
      for (int t = 0; t < par.size(); t++) {
        if (pos[t] == par.get(t).size()) continue;
        done = false;
        Op op = par.get(t).get(pos[t]);
        if (ev[t] == 0) {
          boolean enabled = true;
          for (int u = 0; u < par.size(); u++) if (u != t && op.clock.get(u) > pos[u]) enabled = false;
          if (!enabled) continue;
        }
        Ev e = op.evs.get(ev[t]);
        for (Spec n : step(s, e)) {
          int p0 = pos[t], e0 = ev[t];
          if (++ev[t] == op.evs.size()) { ev[t] = 0; pos[t]++; }
          linearize(n, par, pos, ev, out, seen);
          pos[t] = p0; ev[t] = e0;
        }
      }
      if (done) out.add(s);
    }

    @Override
    public boolean verifyResults(ExecutionScenario scenario, ExecutionResult result) {
      List<Op> init = ops(scenario.getInitExecution(), result.getInitResults());
      List<List<Op>> par = new ArrayList<>();
      var pe = scenario.getParallelExecution();
      var pr = result.getParallelResultsWithClock();
      for (int t = 0; t < pe.size(); t++) {
        List<Op> l = new ArrayList<>();
        for (int i = 0; i < pe.get(t).size(); i++) l.add(op(pe.get(t).get(i), pr.get(t).get(i).getResult(), pr.get(t).get(i).getClockOnStart()));
        par.add(l);
      }
      List<Op> post = ops(scenario.getPostExecution(), result.getPostResults());
      Set<Spec> a = new HashSet<>();
      runSeq(new Spec(new long[KEYS], new long[KEYS], 0, 16, 0, false, false, false), init, a);
      Set<Spec> b = new HashSet<>();
      for (Spec s : a) linearize(s, par, new int[par.size()], new int[par.size()], b, new HashSet<>());
      Set<Spec> c = new HashSet<>();
      for (Spec s : b) runSeq(s, post, c);
      boolean ok = !c.isEmpty();
      boolean plain = false, laxOnly = true;
      for (Spec s : c) { plain |= !s.window && !s.lax; laxOnly &= s.lax; }
      synchronized (MemoVerifier.class) {
        verified++;
        if (ok && !plain) usedWindow++;
        if (ok && laxOnly) usedLax++;
        if (verified % 5000 == 0) System.out.printf("=== progress: %d executions verified, %d window, %d lax%n", verified, usedWindow, usedLax);
      }
      return ok;
    }
  }

  // ------------------------------------------------------------------------------------------
  static Actor actor(String name, Integer arg) {
    try {
      return arg == null ? new Actor(H1CacheLincheck.class.getMethod(name), List.of())
          : new Actor(H1CacheLincheck.class.getMethod(name, int.class), List.of(arg));
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(name, e);
    }
  }

  /** Tiny DSL: "p0" put key 0, "g0" get key 0, "c" clear, "s16"/"s32" setMaxSize. */
  static List<Actor> actors(String... xs) {
    List<Actor> l = new ArrayList<>();
    for (String x : xs) {
      switch (x.charAt(0)) {
        case 'p' -> l.add(actor("putIfAbsent", Integer.parseInt(x.substring(1))));
        case 'g' -> l.add(actor("getIfPresent", Integer.parseInt(x.substring(1))));
        case 'c' -> l.add(actor("clear", null));
        case 's' -> l.add(actor("setMaxSize", x.equals("s16") ? 0 : 1));
        default -> throw new IllegalArgumentException(x);
      }
    }
    return l;
  }

  static ExecutionScenario scenario(String init, String[] par, String post) {
    List<List<Actor>> p = new ArrayList<>();
    for (String t : par) p.add(actors(t.split(" ")));
    return new ExecutionScenario(init.isEmpty() ? List.of() : actors(init.split(" ")), p, post.isEmpty() ? List.of() : actors(post.split(" ")), null);
  }

  static String randomActor(Random r) {
    return switch (r.nextInt(4)) {
      case 0 -> "p" + r.nextInt(KEYS);
      case 1 -> "g" + r.nextInt(KEYS);
      case 2 -> "c";
      default -> r.nextBoolean() ? "s16" : "s32";
    };
  }

  static String randomActors(Random r, int n) {
    StringJoiner j = new StringJoiner(" ");
    for (int i = 0; i < n; i++) j.add(randomActor(r));
    return j.toString();
  }

  /**
   * run.sh lincheck [-Dmc.*]. Scenarios are generated here (uniform over the four operations, keys 0..7, sizes
   * 16|32) from -Dmc.seed and passed to Lincheck as custom scenarios with iterations(0), so that independent JVMs
   * with different seeds can share the 100 x 10 000 budget (Lincheck 2.39's own generator is seeded identically in
   * every JVM). Each scenario is model checked with mc.invocations interleavings. -Dmc.handwritten=true (default
   * when seed is 0) adds the hand-written scenarios first.
   */
  public static void main(String[] args) {
    int iterations = Integer.getInteger("mc.iterations", 100);
    int invocations = Integer.getInteger("mc.invocations", 10000);
    int threads = Integer.getInteger("mc.threads", 3);
    int perThread = Integer.getInteger("mc.actorsPerThread", 3);
    int before = Integer.getInteger("mc.actorsBefore", 4), after = Integer.getInteger("mc.actorsAfter", 3);
    long seed = Long.getLong("mc.seed", 0);
    boolean handwritten = Boolean.parseBoolean(System.getProperty("mc.handwritten", String.valueOf(seed == 0)));
    String cfg = "concurrency " + Integer.getInteger("mc.concurrency", 2) + ", batch " + Integer.getInteger("mc.batch", 1)
        + ", padding " + Integer.getInteger("mc.padding", 1) + ", lax=" + System.getProperty("mc.lax", "")
        + (MemoVerifier.NO_ALO ? ", noAtLeastOnce" : "");
    System.out.println("=== H1 Lincheck model checking: UNodeCache(16, " + cfg + "), seed " + seed + ", " + iterations
        + " scenarios x " + invocations + " invocations, " + threads + " threads x " + perThread + " actors, "
        + before + " before, " + after + " after" + (handwritten ? ", plus 4 hand-written scenarios" : ""));
    long t0 = System.nanoTime();
    boolean failed = false;
    try {
      var opts = new ModelCheckingOptions()
          .iterations(0)
          .invocationsPerIteration(invocations)
          .threads(threads)
          .actorsPerThread(perThread)
          .actorsBefore(before)
          .actorsAfter(after)
          .verifier(MemoVerifier.class);
      if (handwritten) {
        // put racing a resize, with lookups after: the OPTIMUS-57931 window.
        opts.addCustomScenario(scenario("p0 p1", new String[] {"p2 g0", "s32", "g1 p2"}, "g0 g1 g2"));
        opts.addCustomScenario(scenario("p0 p1 s32", new String[] {"p2 g2", "s16", "p3 g3"}, "g2 g3 g0"));
        // an insert in flight across clear, observed before and after the clear by the clearing thread.
        opts.addCustomScenario(scenario("p0", new String[] {"p3", "g3 c g3", "p4"}, "g3 g4 g0"));
        // same key from three threads, with a clear.
        opts.addCustomScenario(scenario("", new String[] {"p5 g5", "p5 c", "p5 g5"}, "g5 p5 g5"));
      }
      Random r = new Random(seed);
      for (int i = 0; i < iterations; i++) {
        String[] par = new String[threads];
        for (int t = 0; t < threads; t++) par[t] = randomActors(r, perThread);
        opts.addCustomScenario(scenario(randomActors(r, before), par, randomActors(r, after)));
      }
      LinCheckerKt.check(opts, H1CacheLincheck.class);
      System.out.println("=== PASSED");
    } catch (Throwable e) {
      failed = true;
      System.out.println("=== FAILED");
      System.out.println(e.getMessage());
      if (!(e instanceof AssertionError)) e.printStackTrace(System.out);
    }
    System.out.printf("=== %d executions verified, %d needed the setMaxSize at-least-once window, %d only linearize with a lax relaxation; %.1fs%n",
        MemoVerifier.verified, MemoVerifier.usedWindow, MemoVerifier.usedLax, (System.nanoTime() - t0) / 1e9);
    System.exit(failed ? 1 : 0);
  }
}
