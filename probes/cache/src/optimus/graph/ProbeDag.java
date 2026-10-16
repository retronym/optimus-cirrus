package optimus.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * The probe's own description of a node DAG, scenario stacks and tweaks, independent of the system. Fixtures build
 * real property nodes / tweaks / stacks from it ({@code ProbeSystem} in CacheFixtures.scala); the two oracles
 * ({@link SequentialEvaluator}, {@link InvalidationOracle}) compute expected values and invalidation sets from it
 * without touching the graph runtime.
 *
 * <p>Values are longs. A key is (property, entity, arg): entity {@link #MODULE} is the shared FakeModuleEntity,
 * entity k >= 0 is ProbeEntity(k). In a {@link Ref} (a declared read), {@link #SAME} for entity or arg means "the
 * reading key's own entity / arg".
 */
public final class ProbeDag {
  public static final int MODULE = -1;
  public static final int SAME = -2;

  /** Variant of a property; selects the node class the fixtures build and what the oracle computes. */
  public enum Kind {
    /** value = combine(key, values of reads) */
    PLAIN,
    /** value = combine(key, [tag]) where tag = nearest plugin tag (EvaluationContext.findPluginTag), 0 if none */
    TAG_READER,
    /** value = combine(key, [envValue]) where envValue is the stack's env token (ScenarioStack.env, unmarked) */
    ENV_READER,
    /** Like PLAIN, but the node's key methods are broken in the way {@link Prop#badKey} says. */
    BAD_KEY
  }

  public enum BadKey {
    NONE,
    /** argsHash/argsEquals read a shared mutable cell (Prop.mutableArg) instead of the arg. */
    MUTABLE_ARG,
    /** argsEquals(that) = this.arg <= that.arg (not symmetric); argsHash = 0 */
    ASYMMETRIC_EQUALS,
    /** argsHash returns a different value on every call */
    UNSTABLE_HASH
  }

  /** Tweak RHS evaluation scope, as Tweak.evaluateIn. */
  public enum Mode {
    /** default (Tweak.evaluateInParentOfGiven): RHS evaluated in the parent of the scenario that holds the tweak */
    BYNAME,
    /** Tweaks.bind (Tweak.evaluateInCurrent): RHS evaluated in the requesting stack */
    BIND,
    /** Tweaks.bindOnce (Tweak.evaluateInGiven): RHS evaluated in the stack that holds the tweak */
    BIND_ONCE
  }

  /** value = f(key, child values) */
  @FunctionalInterface
  public interface Combine {
    long apply(Key key, long[] childValues);

    Combine SUM =
        (k, xs) -> {
          long s = k.prop.base + k.arg;
          for (long x : xs) s += x;
          return s;
        };
  }

  public static final class Prop {
    public final int id;
    public final String name;
    public Kind kind = Kind.PLAIN;
    public BadKey badKey = BadKey.NONE;
    public boolean tweakable = true;
    public boolean cacheable = true;
    public boolean si = false;
    public boolean atMostOneWaiter = false;
    public boolean localCache = false;
    public boolean dontTrack = false;
    /** Extra NodeTaskInfo flags OR-ed in by the fixtures. */
    public long extraFlags = 0;
    public long base = 0;
    public final List<Ref> reads = new ArrayList<>();
    public Combine combine = Combine.SUM;
    /** For BadKey.MUTABLE_ARG: the shared cell. */
    public final long[] mutableArg = new long[1];

    Prop(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public Prop reads(Prop p) {
      reads.add(new Ref(p, SAME, SAME));
      return this;
    }

    public Prop reads(Prop p, int entity, int arg) {
      reads.add(new Ref(p, entity, arg));
      return this;
    }

    public Prop base(long b) {
      base = b;
      return this;
    }

    public Prop notTweakable() {
      tweakable = false;
      return this;
    }

    public Prop dontCache() {
      cacheable = false;
      return this;
    }

    public Prop si() {
      si = true;
      return this;
    }

    public Prop kind(Kind k) {
      kind = k;
      return this;
    }

    public Prop badKey(BadKey b) {
      kind = Kind.BAD_KEY;
      badKey = b;
      return this;
    }

    public Prop combine(Combine c) {
      combine = c;
      return this;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** A declared read. entity/arg may be SAME. */
  public record Ref(Prop prop, int entity, int arg) {
    public Key resolve(Key reader) {
      return new Key(prop, entity == SAME ? reader.entity : entity, arg == SAME ? reader.arg : arg);
    }
  }

  public record Key(Prop prop, int entity, int arg) {
    @Override
    public String toString() {
      return prop.name + "[" + (entity == MODULE ? "M" : "e" + entity) + (arg == 0 ? "" : "," + arg) + "]";
    }
  }

  /** `when` predicate: true iff argPred(key.arg) and (read == null or valuePred(value of read under parent)). */
  public static final class When {
    public final java.util.function.IntPredicate argPred;
    public final Ref read;
    public final LongPredicate valuePred;

    public When(java.util.function.IntPredicate argPred, Ref read, LongPredicate valuePred) {
      this.argPred = argPred;
      this.read = read;
      this.valuePred = valuePred;
    }
  }

  /** By-name RHS: combine(tweakedKey, values of reads). Refs with SAME resolve against the tweaked key. */
  public static final class Rhs {
    public final List<Ref> reads;
    public final Combine combine;

    public Rhs(List<Ref> reads, Combine combine) {
      this.reads = List.copyOf(reads);
      this.combine = combine;
    }
  }

  /**
   * A tweak: target is an instance key (instanceKey != null) or a property (property != null, optional when). RHS is
   * a constant (rhs == null) or by-name (rhs != null) with a Mode.
   */
  public static final class Tw {
    public final Key instanceKey;
    public final Prop property;
    public final When when;
    public final long constant;
    public final Rhs rhs;
    public final Mode mode;

    private Tw(Key k, Prop p, When w, long c, Rhs r, Mode m) {
      instanceKey = k;
      property = p;
      when = w;
      constant = c;
      rhs = r;
      mode = m;
    }

    public static Tw instance(Key k, long v) {
      return new Tw(k, null, null, v, null, Mode.BYNAME);
    }

    public static Tw instance(Key k, Rhs rhs, Mode m) {
      return new Tw(k, null, null, 0, rhs, m);
    }

    public static Tw property(Prop p, long v) {
      return new Tw(null, p, null, v, null, Mode.BYNAME);
    }

    public static Tw property(Prop p, Rhs rhs, Mode m) {
      return new Tw(null, p, null, 0, rhs, m);
    }

    public static Tw propertyWhen(Prop p, When w, long v) {
      return new Tw(null, p, w, v, null, Mode.BYNAME);
    }

    public static Tw propertyWhen(Prop p, When w, Rhs rhs, Mode m) {
      return new Tw(null, p, w, 0, rhs, m);
    }

    public Prop targetProp() {
      return instanceKey != null ? instanceKey.prop : property;
    }

    @Override
    public String toString() {
      String t = instanceKey != null ? instanceKey.toString() : property.name + (when != null ? " when.." : "");
      return t + " := " + (rhs == null ? Long.toString(constant) : "byName(" + mode + ")");
    }
  }

  /** One flat scenario (one stack level). tag != null sets the plugin tag at this level. */
  public static final class Level {
    public final List<Tw> tweaks;
    public final Long tag;

    public Level(List<Tw> tweaks, Long tag) {
      this.tweaks = List.copyOf(tweaks);
      this.tag = tag;
    }

    public static Level of(Tw... tws) {
      return new Level(Arrays.asList(tws), null);
    }
  }

  /** A stack: levels from just above the root upwards. envToken is what ENV_READER sees. */
  public static final class Stack {
    public final List<Level> levels;
    public final long envToken;

    public Stack(List<Level> levels, long envToken) {
      this.levels = List.copyOf(levels);
      this.envToken = envToken;
    }

    public static Stack of(Level... ls) {
      return new Stack(Arrays.asList(ls), 0);
    }

    public Stack push(Level l) {
      List<Level> n = new ArrayList<>(levels);
      n.add(l);
      return new Stack(n, envToken);
    }
  }

  // ---------------------------------------------------------------------------------------------
  public final List<Prop> props = new ArrayList<>();

  public Prop prop(String name) {
    Prop p = new Prop(props.size(), name);
    props.add(p);
    return p;
  }

  public static Key key(Prop p) {
    return new Key(p, MODULE, 0);
  }

  public static Key key(Prop p, int entity, int arg) {
    return new Key(p, entity, arg);
  }

  // =============================================================================================
  /** Thrown (as a value) by the oracle where the system must throw IllegalScenarioDependenceException. */
  public static final class OracleISD extends RuntimeException {
    public final Key key;

    OracleISD(Key k) {
      super("ISD reading " + k, null, false, false);
      key = k;
    }
  }

  /**
   * Value oracle. Walks the stack top-down like TwkResolver (per level: instance tweak, then property tweak with its
   * when clause evaluated in the level's parent; last one wins within a level), evaluates a by-name RHS at the level
   * TweakNode.initialize picks (BYNAME: level-1, BIND_ONCE: level, BIND: requesting position), and evaluates
   * untweaked nodes at the requesting position (or, for SI properties, in an SI position where any tweakable read is
   * an ISD). No caching. Optionally records, per evaluated (key, position), the set of tweakable keys it looked up
   * transitively (for {@link InvalidationOracle}).
   */
  public static final class SequentialEvaluator {
    final Stack stack;
    /** (key@pos) -> tweakable keys looked up while computing it (including itself if tweakable). */
    public final Map<String, Set<Key>> reads = new LinkedHashMap<>();
    /** (key@pos) -> key, for every node the evaluation visited at a position. */
    public final Map<String, Key> visited = new LinkedHashMap<>();
    public int steps;

    public SequentialEvaluator(Stack stack) {
      this.stack = stack;
    }

    /** Position: top = number of levels visible (0 = root); si = scenario-independent stack. */
    record Pos(int top, boolean si) {
      @Override
      public String toString() {
        return si ? "SI" : Integer.toString(top);
      }
    }

    public long eval(Key k) {
      return eval(k, new Pos(stack.levels.size(), false), null);
    }

    /** Value of k when requested at a stack that has only the first `top` levels. */
    public long evalAt(Key k, int top) {
      return eval(k, new Pos(top, false), null);
    }

    /** Returns the value, or throws OracleISD. */
    long eval(Key k, Pos pos, Set<Key> acc) {
      if (++steps > 1_000_000) throw new IllegalStateException("oracle: runaway evaluation (cycle?) at " + k);
      String id = k + "@" + pos;
      Set<Key> mine = new LinkedHashSet<>();
      long v;
      try {
        v = evalUncached(k, pos, mine);
      } finally {
        visited.put(id, k);
        reads.merge(id, mine, (a, b) -> { a.addAll(b); return a; });
        if (acc != null) acc.addAll(mine);
      }
      return v;
    }

    private long evalUncached(Key k, Pos pos, Set<Key> acc) {
      Prop p = k.prop;
      if (p.tweakable) {
        acc.add(k);
        if (pos.si) throw new OracleISD(k);
        for (int lvl = pos.top; lvl >= 1; lvl--) {
          Tw tw = findTweak(stack.levels.get(lvl - 1), k, lvl, acc);
          if (tw != null) return evalTweak(tw, k, pos, lvl, acc);
        }
      }
      Pos at = p.si ? new Pos(pos.top, true) : pos;
      return compute(k, at, acc);
    }

    /** Instance tweak first (last wins), then property tweak (last wins) whose when clause holds. */
    private Tw findTweak(Level level, Key k, int lvl, Set<Key> acc) {
      Tw inst = null, prop = null;
      for (Tw t : level.tweaks) {
        if (t.instanceKey != null && t.instanceKey.equals(k)) inst = t;
        else if (t.property == k.prop) prop = t;
      }
      if (inst != null) return inst;
      if (prop != null && prop.when != null) {
        When w = prop.when;
        if (!w.argPred.test(k.arg)) return null;
        if (w.read != null) {
          long rv = eval(w.read.resolve(k), new Pos(lvl - 1, false), acc);
          if (!w.valuePred.test(rv)) return null;
        }
      }
      return prop;
    }

    private long evalTweak(Tw tw, Key k, Pos req, int lvl, Set<Key> acc) {
      if (tw.rhs == null) return tw.constant;
      Pos at =
          switch (tw.mode) {
            case BYNAME -> new Pos(lvl - 1, false);
            case BIND_ONCE -> new Pos(lvl, false);
            case BIND -> req;
          };
      long[] xs = new long[tw.rhs.reads.size()];
      for (int i = 0; i < xs.length; i++) xs[i] = eval(tw.rhs.reads.get(i).resolve(k), at, acc);
      return tw.rhs.combine.apply(k, xs);
    }

    private long compute(Key k, Pos pos, Set<Key> acc) {
      Prop p = k.prop;
      switch (p.kind) {
        case TAG_READER:
          return p.combine.apply(k, new long[] {tagAt(pos.top)});
        case ENV_READER:
          return p.combine.apply(k, new long[] {stack.envToken});
        default:
          long[] xs = new long[p.reads.size()];
          for (int i = 0; i < xs.length; i++) xs[i] = eval(p.reads.get(i).resolve(k), pos, acc);
          return p.combine.apply(k, xs);
      }
    }

    long tagAt(int top) {
      for (int lvl = top; lvl >= 1; lvl--) {
        Long t = stack.levels.get(lvl - 1).tag;
        if (t != null) return t;
      }
      return 0;
    }
  }

  // =============================================================================================
  /**
   * Invalidation oracle. Two flavours:
   *
   * <ul>
   *   <li>{@link #dependents(SequentialEvaluator, Key)}: exact, from the reads an oracle evaluation actually made (so
   *       a tweaked node that no longer reads its declared children does not depend on them, a when clause's read
   *       counts, and a by-name RHS's reads count). This is the set the tracker must invalidate after a change to
   *       `changed` (an instance tweak on that key) when the cached nodes are exactly those the evaluation visited.
   *   <li>{@link #dependents(SequentialEvaluator, Prop)}: same, for a property-level change.
   *   <li>{@link #staticDependents}: over-approximation from declared reads only (every key transitively reachable
   *       upward from `changed` in the declared DAG, among `universe`).
   * </ul>
   */
  public static final class InvalidationOracle {
    private InvalidationOracle() {}

    public static Set<Key> dependents(SequentialEvaluator ev, Key changed) {
      Set<Key> out = new LinkedHashSet<>();
      ev.reads.forEach((id, rs) -> { if (rs.contains(changed)) out.add(ev.visited.get(id)); });
      return out;
    }

    public static Set<Key> dependents(SequentialEvaluator ev, Prop changed) {
      Set<Key> out = new LinkedHashSet<>();
      ev.reads.forEach(
          (id, rs) -> {
            for (Key r : rs) if (r.prop == changed) { out.add(ev.visited.get(id)); break; }
          });
      return out;
    }

    public static Set<Key> staticDependents(Iterable<Key> universe, Key changed) {
      Set<Key> out = new LinkedHashSet<>();
      boolean grew = true;
      List<Key> all = new ArrayList<>();
      universe.forEach(all::add);
      if (all.contains(changed)) out.add(changed);
      while (grew) {
        grew = false;
        for (Key k : all) {
          if (out.contains(k)) continue;
          for (Ref r : k.prop.reads)
            if (r.resolve(k).equals(changed) || out.contains(r.resolve(k))) {
              out.add(k);
              grew = true;
              break;
            }
        }
      }
      return out;
    }
  }

  public static <T> Set<T> setOf(Iterable<T> it) {
    Set<T> s = new LinkedHashSet<>();
    it.forEach(s::add);
    return s;
  }

  static String fmt(Set<Key> s) {
    List<String> l = new ArrayList<>();
    for (Key k : s) l.add(k.toString());
    Collections.sort(l);
    return l.toString();
  }

  static boolean eqKeys(Set<Key> a, Set<Key> b) {
    return Objects.equals(a, b);
  }
}
