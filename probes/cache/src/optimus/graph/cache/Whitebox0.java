package optimus.graph.cache;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import optimus.core.TPDMask;
import optimus.entity.EntityInfo;
import optimus.graph.DiagnosticSettings;
import optimus.graph.EvaluationState;
import optimus.graph.InstancePropertyTarget;
import optimus.graph.NodeTaskInfo;
import optimus.graph.PropertyInfo;
import optimus.graph.PropertyInfo0;
import optimus.graph.PropertyNode;
import optimus.graph.SSCacheID;
import optimus.graph.TweakTemplate$;
import optimus.graph.tracking.ttracks.TTrackProbeSupport;
import optimus.platform.EvaluationContext;
import optimus.platform.ReadsTweakable;
import optimus.platform.Scenario;
import optimus.platform.Scenario$;
import optimus.platform.ScenarioStack;
import optimus.platform.ScenarioStack$;
import optimus.platform.Tweak;
import optimus.platform.Tweak$;
import optimus.platform.storable.Entity;
import optimus.platform.storable.EntityFlavor;
import optimus.platform.storable.EntityReference;
import optimus.platform.storable.StorageInfo;
import optimus.platform.TemporalContext;
import optimus.platform.pickling.PickledOutputStream;
import scala.collection.immutable.Nil$;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Fixture-free Tier 0 probes for the caching-layer review: W1 (D3), W5 (D13, F1), W6 (F7), W7 (F2,
 * F3), W12 (D14), W14 (CleanupVisitor model check). One RESULT line per probe.
 */
public final class Whitebox0 {
  enum Outcome {
    DEMONSTRATED,
    NOT_REPRODUCED,
    QUANTIFIED,
    ERROR
  }

  record Result(String id, Outcome outcome, String detail) {}

  interface Probe {
    Result run() throws Throwable;
  }

  static Result run(String id, Probe p) {
    try {
      return p.run();
    } catch (Throwable t) {
      java.io.StringWriter sw = new java.io.StringWriter();
      t.printStackTrace(new java.io.PrintWriter(sw));
      System.err.println(sw);
      return new Result(id, Outcome.ERROR, t.toString());
    }
  }

  // ---------------------------------------------------------------- fixtures (plugin-free)

  /** Minimal Entity: identity equality, everything else unsupported. */
  static final class StubEntity implements Entity {
    final String name;

    StubEntity(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "E(" + name + ")";
    }

    @Override
    public msjava.slf4jutils.scalalog.Logger log() {
      return null;
    }

    @Override
    public EntityInfo $info() {
      return null;
    }

    @Override
    public EntityReference $permReference() {
      return null;
    }

    @Override
    public EntityReference dal$entityRef() {
      return null;
    }

    @Override
    public StorageInfo dal$storageInfo() {
      return null;
    }

    @Override
    public TemporalContext dal$loadContext() {
      return null;
    }

    @Override
    public TemporalContext dal$temporalContext() {
      return null;
    }

    @Override
    public boolean $inline() {
      return false;
    }

    @Override
    public EntityFlavor entityFlavorInternal() {
      return null;
    }

    @Override
    public void pickle(PickledOutputStream out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean dal$isTemporary() {
      return true;
    }
  }

  /** Hand-written property node key: e.<info>() */
  static final class Key<T> extends PropertyNode<T> {
    final Entity e;
    final NodeTaskInfo info;

    Key(Entity e, NodeTaskInfo info) {
      this.e = e;
      this.info = info;
    }

    @Override
    public Entity entity() {
      return e;
    }

    @Override
    public NodeTaskInfo propertyInfo() {
      return info;
    }

    // PropertyNode's constructor calls this before our fields are set; tracking state is not needed
    @Override
    public void initTrackingValue() {}

    @Override
    public String toString() {
      return e + "." + info.name();
    }
  }

  /** A by-name RHS ("a + 1"): an incomplete node, so TweakTemplate gives TweakTemplateByNode. */
  static final class Rhs extends PropertyNode<Object> {
    final String label;

    Rhs(String label) {
      this.label = label;
    }

    @Override
    public Entity entity() {
      return null;
    }

    @Override
    public String toString() {
      return label;
    }
  }

  static PropertyInfo0<Entity, Object> tweakableInfo(String name) {
    @SuppressWarnings("unchecked")
    scala.collection.immutable.Seq<scala.annotation.StaticAnnotation> nil =
        (scala.collection.immutable.Seq<scala.annotation.StaticAnnotation>) (Object) Nil$.MODULE$;
    return new PropertyInfo0<>(name, NodeTaskInfo.TWEAKABLE, nil);
  }

  static Tweak valueTweak(PropertyNode<?> key, Object v) {
    return new Tweak(new InstancePropertyTarget<>(key), TweakTemplate$.MODULE$.apply(v));
  }

  static Scenario scenario(Tweak... ts) {
    return Scenario$.MODULE$.apply(CollectionConverters.asScala(Arrays.asList(ts)).toSeq());
  }

  static Scenario unordered(Tweak... ts) {
    return Scenario$.MODULE$.unordered(CollectionConverters.asScala(Arrays.asList(ts)).toSeq(), true);
  }

  static Object tweakValue(Tweak t) {
    return t == null ? null : t.tweakTemplate().computeGenerator();
  }

  static ScenarioStack root() {
    return EvaluationContext.scenarioStack();
  }

  // ---------------------------------------------------------------- W1 (D3)
  static Result w1() {
    int width = TPDMask.empty.toArray().length;
    // indexes (1-based, [REVERSE_INDEX_COMPUTE]): word0 bit3 = 4, word2 bit7 = 136, word5 bit1 = 322
    long[] six = new long[6];
    six[0] = 1L << 3;
    six[2] = 1L << 7;
    six[5] = 1L << 1;
    String enc = TPDMask.stringEncoded(six);
    TPDMask decoded = TPDMask.stringDecoded(enc);
    long[] dec = decoded.toArray();
    List<String> survive = new ArrayList<>();
    List<String> lost = new ArrayList<>();
    int[][] bits = {{0, 3}, {2, 7}, {5, 1}};
    for (int[] b : bits) {
      boolean present = b[0] < dec.length && (dec[b[0]] & (1L << b[1])) != 0;
      (present ? survive : lost).add("w" + b[0] + "b" + b[1]);
    }
    // the node/tweak side: a property whose tweakable id lands on word2 bit7 (index 136), as this
    // process would build it (fromIndex folds modulo width)
    TPDMask prop = TPDMask.fromIndex(136);
    boolean subset = prop.subsetOf(decoded);
    boolean inter = decoded.intersects(prop);
    long[] fold = TPDMask.indexListToMask(new int[] {200});
    String foldStr = Arrays.toString(fold) + " (" + TPDMask.stringEncoded(fold) + ")";
    String detail =
        "width="
            + width
            + " (tweakUsageQWords="
            + DiagnosticSettings.tweakUsageQWords
            + " enablePerNodeTPDMask="
            + DiagnosticSettings.enablePerNodeTPDMask
            + ") encoded="
            + enc
            + " decoded="
            + decoded.stringEncodedFixedWidth()
            + " survive="
            + survive
            + " lost="
            + lost
            + " propMask(idx136)="
            + prop.stringEncoded()
            + " prop.subsetOf(decoded)="
            + subset
            + " decoded.intersects(prop)="
            + inter
            + " indexListToMask([200])="
            + foldStr;
    Outcome o = lost.isEmpty() ? Outcome.NOT_REPRODUCED : (inter ? Outcome.QUANTIFIED : Outcome.DEMONSTRATED);
    return new Result("W1", o, detail);
  }

  // ---------------------------------------------------------------- W5 (D13, F1)
  static Result w5() {
    StringBuilder d = new StringBuilder();
    boolean demonstrated = false;
    StubEntity e = new StubEntity("w5");
    PropertyInfo0<Entity, Object> kInfo = tweakableInfo("w5k");
    Key<Object> k = new Key<>(e, kInfo);
    Tweak t1 = valueTweak(k, 1);
    Tweak t2 = valueTweak(k, 2);
    ScenarioStack root = root();

    // (a.0) public API with duplicate targets
    Scenario ord = scenario(t1, t2); // warns, last wins
    String unorderedPlain;
    try {
      unordered(t1, t2);
      unorderedPlain = "accepted";
    } catch (IllegalArgumentException ex) {
      unorderedPlain = "throws IAE";
    }
    d.append("(a) Scenario.unordered(t1,t2) same target: ").append(unorderedPlain).append("; ");
    // resolveDupsByTakingLast is public and lets duplicates through both constructors
    Tweak d1 = t1.resolveDupsByTakingLast();
    Tweak d2 = t2.resolveDupsByTakingLast();
    Scenario o12 = scenario(d1, d2);
    Scenario u12 = unordered(d1, d2);
    Scenario u21 = unordered(d2, d1);
    d.append("with resolveDupsByTakingLast: ")
        .append("ord(t1,t2).equals(unord(t1,t2))=")
        .append(o12.equals(u12))
        .append(" unord(t1,t2).equals(ord)=")
        .append(u12.equals(o12))
        .append(" unord(t1,t2).equals(unord(t2,t1))=")
        .append(u12.equals(u21))
        .append(" reverse=")
        .append(u21.equals(u12))
        .append(" hash ord=")
        .append(Integer.toHexString(o12.hashCode()))
        .append(" unord12=")
        .append(Integer.toHexString(u12.hashCode()))
        .append(" unord21=")
        .append(Integer.toHexString(u21.hashCode()))
        .append("; ");
    // cache-id sharing via the real path (createChild -> new SSCacheKey -> CacheIDs.putIfAbsent)
    ScenarioStack sOrd = root.createChild(o12, null);
    ScenarioStack sU12 = root.createChild(u12, null);
    ScenarioStack sU21 = root.createChild(u21, null);
    Object vOrd = tweakValue(sOrd._cacheID().get(k));
    Object vU12 = tweakValue(sU12._cacheID().get(k));
    Object vU21 = tweakValue(sU21._cacheID().get(k));
    // what u21 would resolve to on its own (fresh, uncached id)
    SSCacheID fresh = SSCacheID.newUninitialized(u21);
    fresh.ensureInitialized();
    Object vU21Own = tweakValue(fresh.get(k));
    d.append("cacheIDs: ord=")
        .append(sOrd._cacheID().id())
        .append(" unord12=")
        .append(sU12._cacheID().id())
        .append(" unord21=")
        .append(sU21._cacheID().id())
        .append(" shared(unord12,unord21)=")
        .append(sU12._cacheID() == sU21._cacheID())
        .append(" resolved k: ord=")
        .append(vOrd)
        .append(" unord12=")
        .append(vU12)
        .append(" unord21=")
        .append(vU21)
        .append(" (unord21 with its own id would resolve ")
        .append(vU21Own)
        .append("); ");
    if (sU12._cacheID() == sU21._cacheID() && !java.util.Objects.equals(vU21, vU21Own)) demonstrated = true;
    // also: ordered (t1,t2) vs ordered (t2,t1) control
    d.append("ord(t1,t2) plain-API scenario hash=").append(Integer.toHexString(ord.hashCode())).append("; ");

    // (b) Tweak.equals ignores evaluateIn / readsTweakables / flags
    PropertyInfo0<Entity, Object> bInfo = tweakableInfo("w5b");
    PropertyInfo0<Entity, Object> aInfo = tweakableInfo("w5a");
    Key<Object> bk = new Key<>(e, bInfo);
    Rhs rhs = new Rhs("a+1");
    Tweak byName = new Tweak(new InstancePropertyTarget<>(bk), TweakTemplate$.MODULE$.apply(rhs));
    Tweak bound = Tweak$.MODULE$.bind(byName);
    Tweak boundOnce = Tweak$.MODULE$.bindOnce(byName);
    Tweak withReads =
        new Tweak(
            byName.target(),
            byName.tweakTemplate(),
            new ReadsTweakable(new PropertyInfo[] {aInfo}),
            byName.evaluateIn(),
            byName.flags(),
            null);
    Tweak withFlags = byName.keepLazyEvaluation();
    d.append("(b) template=")
        .append(byName.tweakTemplate().getClass().getSimpleName())
        .append(" evaluateIn bind=")
        .append(bound.evaluateIn())
        .append(" bindOnce=")
        .append(boundOnce.evaluateIn())
        .append(" bind.equals(bindOnce)=")
        .append(bound.equals(boundOnce))
        .append(" hash ")
        .append(Integer.toHexString(bound.hashCode()))
        .append("/")
        .append(Integer.toHexString(boundOnce.hashCode()))
        .append("; byName.equals(onlyReads-variant)=")
        .append(byName.equals(withReads))
        .append(" hash ")
        .append(Integer.toHexString(withReads.hashCode()))
        .append("; byName.equals(keepLazyEvaluation flags=")
        .append(withFlags.flags())
        .append(")=")
        .append(byName.equals(withFlags))
        .append("; ");
    ScenarioStack sBind = root.createChild(scenario(bound), null);
    ScenarioStack sOnce = root.createChild(scenario(boundOnce), null);
    Tweak resolvedInOnce = sOnce._cacheID().get(bk);
    ScenarioStack sReads = root.createChild(scenario(withReads), null);
    Tweak resolvedInReads = sReads._cacheID().get(bk);
    d.append("stacks under same parent: shared cacheID(bind,bindOnce)=")
        .append(sBind._cacheID() == sOnce._cacheID())
        .append(" bindOnce stack resolves evaluateIn=")
        .append(resolvedInOnce.evaluateIn())
        .append(" (own=")
        .append(boundOnce.evaluateIn())
        .append("); onlyReads stack shares id with bind=")
        .append(sReads._cacheID() == sBind._cacheID())
        .append(" resolves readsTweakables=")
        .append(resolvedInReads.readsTweakables() == null ? "null" : "set")
        .append(" (own=set)");
    if (sBind._cacheID() == sOnce._cacheID() && resolvedInOnce.evaluateIn() != boundOnce.evaluateIn())
      demonstrated = true;
    return new Result("W5", demonstrated ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, d.toString());
  }

  // ---------------------------------------------------------------- W6 (F7)
  static String describe(Scenario s) {
    StringBuilder sb = new StringBuilder("top{");
    for (Tweak t : CollectionConverters.asJava(s.topLevelTweaks())) sb.append(short_(t)).append(' ');
    sb.append("} nested[");
    for (Scenario n : CollectionConverters.asJava(s.nestedScenarios())) sb.append(describe(n)).append(' ');
    return sb.append("]").toString();
  }

  static String short_(Tweak t) {
    Object v = tweakValue(t);
    return ((Key<?>) t.target().hashKey()).info.name() + ":=" + v;
  }

  /** Walk the stack from the top and return the first level whose cache id has a tweak for k. */
  static String resolve(ScenarioStack ss, PropertyNode<?> k) {
    int depth = 0;
    for (ScenarioStack cur = ss; cur != null && !cur.isRoot(); cur = cur.parent()) {
      Tweak t = cur._cacheID().get(k);
      if (t != null) return short_(t) + "@level-" + depth + "-from-top";
      depth++;
    }
    return "untweaked";
  }

  static Result w6() {
    StubEntity e = new StubEntity("w6");
    PropertyInfo0<Entity, Object> aInfo = tweakableInfo("a");
    PropertyInfo0<Entity, Object> bInfo = tweakableInfo("b");
    Key<Object> a = new Key<>(e, aInfo);
    Key<Object> b = new Key<>(e, bInfo);
    Tweak a1 = valueTweak(a, 1);
    Rhs rhs = new Rhs("a+1");
    Tweak bByName = new Tweak(new InstancePropertyTarget<>(b), TweakTemplate$.MODULE$.apply(rhs));
    // public onlyReads returns `this` for a TweakTemplateByNode (isReducibleToByValue) - record that
    Tweak viaOnlyReads =
        bByName.onlyReads(CollectionConverters.asScala(List.<PropertyInfo<?>>of(aInfo)).toSeq());
    Tweak bReads =
        new Tweak(
            bByName.target(),
            bByName.tweakTemplate(),
            new ReadsTweakable(new PropertyInfo[] {aInfo}),
            bByName.evaluateIn(),
            bByName.flags(),
            null);
    Tweak b5 = valueTweak(b, 5);
    Scenario s1 = scenario(a1), s2 = scenario(bReads), s3 = scenario(b5);
    Scenario flat = s1.nestWithFlatten(s2).nestWithFlatten(s3);
    Scenario nest = s1.nest(s2).nest(s3);
    ScenarioStack root = root();
    ScenarioStack sFlat = root.createChild(flat, null);
    ScenarioStack sNest = root.createChild(nest, null);
    String rFlat = resolve(sFlat, b), rNest = resolve(sNest, b);
    String detail =
        "public onlyReads on by-name tweak returns same tweak="
            + (viaOnlyReads == bByName)
            + " (readsTweakables built via the constructor instead); s2.hasTweaksWithUndeclaredDependencies="
            + s2.hasTweaksWithUndeclaredDependencies()
            + "; s1.nestWithFlatten(s2)="
            + describe(s1.nestWithFlatten(s2))
            + "; .nestWithFlatten(s3)="
            + describe(flat)
            + "; s1.nest(s2).nest(s3)="
            + describe(nest)
            + "; b resolves: flattened -> "
            + rFlat
            + " (by-name a+1, evaluateIn parent-of-given => a=1 => b=2), nested -> "
            + rNest;
    // same fold without onlyReads (plain by-name RHS: undeclared dependencies, so s2 is nested up front)
    Scenario flatPlain = s1.nestWithFlatten(scenario(bByName)).nestWithFlatten(s3);
    String rPlain = resolve(root.createChild(flatPlain, null), b);
    detail += "; without onlyReads: fold=" + describe(flatPlain) + " b resolves -> " + rPlain;
    boolean demo = rFlat.startsWith("b:=a+1") && rNest.startsWith("b:=5");
    return new Result("W6", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, detail);
  }

  // ---------------------------------------------------------------- W7 (F2, F3)
  static String diffProduct(scala.Product a, scala.Product b) {
    List<String> out = new ArrayList<>();
    for (int i = 0; i < a.productArity(); i++) {
      Object x = a.productElement(i), y = b.productElement(i);
      if (!java.util.Objects.equals(x, y)) out.add(a.productElementName(i) + "(" + x + " vs " + y + ")");
    }
    return out.toString();
  }

  static Result w7() throws Throwable {
    StringBuilder d = new StringBuilder();
    boolean demo = false;
    StubEntity e = new StubEntity("w7");
    Key<Object> k = new Key<>(e, tweakableInfo("w7k"));
    ScenarioStack root = root();
    ScenarioStack s = root.createChild(scenario(valueTweak(k, 7)), null);
    Method equiv = null;
    for (Method m : ScenarioStack$.class.getDeclaredMethods())
      if (m.getName().contains("equivSIParams")) equiv = m;
    equiv.setAccessible(true);
    ScenarioStack si = s.siRoot();
    d.append("(a) root flags=0x")
        .append(Integer.toHexString(root.flags()))
        .append(" s flags=0x")
        .append(Integer.toHexString(s.flags()))
        .append(" siRoot flags=0x")
        .append(Integer.toHexString(si.flags()))
        .append(" siParams eq/equals=")
        .append(s.siParams() == si.siParams())
        .append("/")
        .append(s.siParams().equals(si.siParams()))
        .append(" root.siParams eq si.siParams=")
        .append(root.siParams() == si.siParams())
        .append(" differing SIParams fields(s vs siRoot)=")
        .append(diffProduct(s.siParams(), si.siParams()))
        .append(" cacheSelector.withoutScope eq=")
        .append(s.cacheSelector().withoutScope() == si.cacheSelector())
        .append("; ");
    int[] flags = {
      EvaluationState.AUDITOR_CALLBACKS_DISABLED,
      EvaluationState.IGNORE_SYNC_STACKS,
      EvaluationState.DISABLE_PICKLING_INTERNING,
      EvaluationState.FAIL_ON_SYNC_STACKS
    };
    String[] names = {"AUDITOR_CALLBACKS_DISABLED", "IGNORE_SYNC_STACKS", "DISABLE_PICKLING_INTERNING", "FAIL_ON_SYNC_STACKS"};
    ScenarioStack sharedRoot = root.ssShared().scenarioStack();
    ScenarioStack s2 = sharedRoot.createChild(scenario(valueTweak(k, 8)), null);
    d.append("sharedRoot child: flags=0x")
        .append(Integer.toHexString(s2.flags()))
        .append(" differing SIParams fields=")
        .append(diffProduct(s2.siParams(), si.siParams()))
        .append("; ");
    ScenarioStack[] bases = {s, s2};
    String[] baseNames = {"EC-child", "sharedRoot-child"};
    for (int bi = 0; bi < 2; bi++)
    for (int i = 0; i < flags.length; i++) {
      ScenarioStack b = bases[bi];
      ScenarioStack x =
          flags[i] == EvaluationState.AUDITOR_CALLBACKS_DISABLED ? b.withAuditorCallbacksDisabled() : b.withFlag(flags[i]);
      boolean eq = (Boolean) equiv.invoke(ScenarioStack$.MODULE$, x, si);
      ScenarioStack asSi = x.asSiStack();
      boolean kept = asSi.hasFlag(flags[i]);
      d.append(baseNames[bi]).append(" ").append(names[i])
          .append(": equivSIParams(x,siRoot)=")
          .append(eq)
          .append(" asSiStack eq siRoot=")
          .append(asSi == si)
          .append(" flagKept=")
          .append(kept)
          .append("; ");
      if (!kept) demo = true;
    }
    // control: equal flag sets vs disjoint
    boolean eqSelf = (Boolean) equiv.invoke(ScenarioStack$.MODULE$, s, s);
    d.append("equivSIParams(s,s)=").append(eqSelf).append("; ");

    // (b) withCacheableTransitively after the _stableSS upgrade
    ScenarioStack nc = s.withoutCacheableTransitively();
    d.append("(b) s.CT=").append(s.cachedTransitively()).append(" nc.CT=").append(nc.cachedTransitively());
    ScenarioStack up = nc.withCacheableTransitively();
    ScenarioStack ncFlag = nc.withFlag(EvaluationState.IGNORE_SYNC_STACKS);
    ScenarioStack upFlag = ncFlag.withCacheableTransitively();
    boolean kept = upFlag.hasFlag(EvaluationState.IGNORE_SYNC_STACKS);
    d.append(" nc.withCT eq s=")
        .append(up == s)
        .append(" nc.withFlag(IGNORE_SYNC_STACKS).hasFlag=")
        .append(ncFlag.hasFlag(EvaluationState.IGNORE_SYNC_STACKS))
        .append(" .withCacheableTransitively.hasFlag=")
        .append(kept)
        .append(" (returned eq upgraded=")
        .append(upFlag == up)
        .append(")");
    // TweakExpander shape: evaluateIn.withIgnoreSyncStack on a non-CT stack, then CT upgrade
    ScenarioStack te = nc.withIgnoreSyncStack().withCacheableTransitively();
    d.append("; nc.withIgnoreSyncStack.withCacheableTransitively.ignoreSyncStacks=").append(te.ignoreSyncStacks());
    // control: same sequence on a fresh non-CT stack, but setting the flag BEFORE any upgrade
    ScenarioStack fresh = root.createChild(scenario(valueTweak(k, 9)), null);
    boolean keptFresh =
        fresh.withFlag(EvaluationState.IGNORE_SYNC_STACKS).withCacheableTransitively().hasFlag(EvaluationState.IGNORE_SYNC_STACKS);
    d.append("; control (flag set before the parent's upgrade) keeps flag=").append(keptFresh);
    if (!kept) demo = true;
    return new Result("W7", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, d.toString());
  }

  // ---------------------------------------------------------------- W12 (D14)
  static WeakReference<Object> internAndDrop(Interner in) {
    String s = new String("x");
    Object r = in.internFromNew(s);
    return new WeakReference<>(r);
  }

  static boolean gcUntilCleared(WeakReference<?> w) {
    for (int i = 0; i < 50 && w.get() != null; i++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (InterruptedException ignored) {
      }
    }
    return w.get() == null;
  }

  /** Runs f on a daemon thread; returns the value or "TIMEOUT" after ms. */
  static Object callWithTimeout(String name, Supplier<Object> f, long ms) throws InterruptedException {
    AtomicReference<Object> out = new AtomicReference<>(null);
    Thread t =
        new Thread(
            () -> {
              try {
                out.set(f.get());
              } catch (Throwable ex) {
                out.set(ex);
              }
            },
            name);
    t.setDaemon(true);
    t.start();
    t.join(ms);
    if (t.isAlive()) {
      StackTraceElement[] st = t.getStackTrace();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(3, st.length); i++) sb.append(st[i]).append(" | ");
      return new Timeout("TIMEOUT after " + ms + " ms, thread still RUNNABLE=" + (t.getState() == Thread.State.RUNNABLE) + " in " + sb);
    }
    return out.get();
  }

  record Timeout(String what) {
    @Override
    public String toString() {
      return what;
    }
  }

  static Object[] w12bSetup(Interner in, Object o) {
    // w is interned first; o (equal, distinct) is interned through the identity path and maps to w's
    // Holder. Caller keeps o and drops w.
    String w = new String("x");
    Object rw = in.internFromNew(w);
    Object ro = in.intern(o);
    return new Object[] {new WeakReference<>(rw), rw == w, ro == w};
  }

  static Result w12() throws Throwable {
    StringBuilder d = new StringBuilder();
    boolean demo = false;
    // (a) as specified: internFromNew, drop, cleaner stopped, gc, internFromNew equal value
    Interner in = new Interner();
    // Thread.suspend throws UnsupportedOperationException on JDK 21; stop the cleaner with the
    // TEST_ONLY hook (interrupt -> ReferenceQueue.remove throws -> thread exits).
    String suspendNote;
    try {
      in.thread().suspend();
      suspendNote = "suspended";
    } catch (UnsupportedOperationException ex) {
      in.afterTest();
      in.thread().join(2000);
      suspendNote = "Thread.suspend unsupported on JDK " + Runtime.version().feature() + "; cleaner stopped via afterTest() (alive=" + in.thread().isAlive() + ")";
    }
    WeakReference<Object> w = internAndDrop(in);
    boolean cleared = gcUntilCleared(w);
    int hashBefore = in.targetSize(), idBefore = in.sourceSize();
    String fresh = new String("x");
    Object r = callWithTimeout("w12a", () -> in.internFromNew(fresh), 2000);
    d.append("(a) ")
        .append(suspendNote)
        .append("; referent cleared=")
        .append(cleared)
        .append(" tables before: hash=")
        .append(hashBefore)
        .append(" identity=")
        .append(idBefore)
        .append("; internFromNew(new \"x\") -> ")
        .append(r instanceof Timeout ? r : r == fresh ? "returned the argument (fresh Holder)" : "returned " + r)
        .append(" tables after: hash=")
        .append(in.targetSize())
        .append(" identity=")
        .append(in.sourceSize());
    if (r instanceof Timeout) demo = true;

    // (b) identity path, cleaner running: intern(o) where o's identity entry maps to the Holder of a
    // value w that has since been collected
    Interner in2 = new Interner();
    Object o = new String("x");
    Object[] setup = w12bSetup(in2, o);
    @SuppressWarnings("unchecked")
    WeakReference<Object> ww = (WeakReference<Object>) setup[0];
    boolean cleared2 = gcUntilCleared(ww);
    Thread.sleep(200); // give the (running) cleaner time to drain the queue
    Object r2 = callWithTimeout("w12b-spinner", () -> in2.intern(o), 2000);
    d.append("; (b) cleaner alive=")
        .append(in2.thread().isAlive())
        .append(" internFromNew(w)==w:")
        .append(setup[1])
        .append(" intern(o)==w:")
        .append(setup[2])
        .append(" w cleared=")
        .append(cleared2)
        .append(" tables: hash=")
        .append(in2.targetSize())
        .append(" identity=")
        .append(in2.sourceSize())
        .append("; intern(o) again (o still strongly held) -> ")
        .append(r2 instanceof Timeout ? r2 : r2 == o ? "returned o" : "returned " + r2);
    if (r2 instanceof Timeout) demo = true;
    return new Result("W12", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, d.toString());
  }

  // ---------------------------------------------------------------- W14 (CleanupVisitor)
  static Result w14() {
    boolean selfTest = TTrackProbeSupport.oracleSelfTest();
    long t0 = System.nanoTime();
    TTrackProbeSupport.Report a = Boolean.getBoolean("w14.quick") ? TTrackProbeSupport.run("A(quick) states{LIVE,NIL,NULL} n<=5 redundant<=3", 5, 3, new int[] {0, 1, 2}) : TTrackProbeSupport.run("A states{LIVE,NIL,NULL} n<=6 redundant<=3", 6, 3, new int[] {0, 1, 2});
    long t1 = System.nanoTime();
    TTrackProbeSupport.Report b =
        TTrackProbeSupport.run("B states{LIVE,NIL,NULL,DEAD,INVALID} n<=5 redundant<=3", 5, 3, new int[] {0, 1, 2, 3, 4});
    long t2 = System.nanoTime();
    TTrackProbeSupport.Report c =
        TTrackProbeSupport.run(
            "C as B but only states where every caller of an INVALID track is INVALID", 5, 3, new int[] {0, 1, 2, 3, 4}, true);
    long t3 = System.nanoTime();
    boolean demo = a.liveViolations > 0 || b.liveViolations > 0;
    String detail =
        "verdict: no violation on graphs whose Invalid tracks have only Invalid callers (A, C); violations only when an INVALID track has a non-INVALID caller (B), where cleanup drops a direct root->X edge as redundant via a path through the INVALID track and then drops the edge into the INVALID track, orphaning X (needs a post-invalidation attach, ring4b S5/D6); "
            + "oracleSelfTest(detects a hand-dropped edge)="
            + selfTest
            + "; variants per state assignment: callerOrder{asc,desc,shuffled} x rootOrder{asc,desc} x initialIds{0,stale-random} (+1 duplicate parallel edge per caller of the first callee with >=3 callers), 2 consecutive cleanups each; "
            + a
            + " ("
            + (t1 - t0) / 1_000_000
            + " ms); "
            + b
            + " ("
            + (t2 - t1) / 1_000_000
            + " ms); "
            + c
            + " ("
            + (t3 - t2) / 1_000_000
            + " ms)";
    return new Result("W14", demo ? Outcome.DEMONSTRATED : Outcome.NOT_REPRODUCED, detail);
  }

  // ---------------------------------------------------------------- runner
  public static void main(String[] args) throws Exception {
    Set<String> only = args.length == 0 ? null : Set.of(args);
    List<Result> results = new ArrayList<>();
    String[] ids = {"W1", "W5", "W6", "W7", "W14", "W12"}; // W12 last: (b) leaves a spinning daemon thread
    boolean needCtx = only == null || !(only.size() == 1 && only.contains("W1"));
    if (needCtx) EvaluationContext.initializeWithoutRuntime();
    for (String id : ids) {
      if (only != null && !only.contains(id)) continue;
      Probe p =
          switch (id) {
            case "W1" -> Whitebox0::w1;
            case "W5" -> Whitebox0::w5;
            case "W6" -> Whitebox0::w6;
            case "W7" -> Whitebox0::w7;
            case "W12" -> Whitebox0::w12;
            case "W14" -> Whitebox0::w14;
            default -> throw new IllegalArgumentException(id);
          };
      Result r = run(id, p);
      results.add(r);
      System.out.println("RESULT " + r.id() + " " + r.outcome() + " " + r.detail());
      System.out.flush();
    }
    System.exit(0);
  }
}
