package optimus.graph.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import optimus.graph.CacheFixtures;
import optimus.graph.NodeTaskInfo;
import optimus.graph.ProbeDag;
import optimus.graph.ProbeProperty;
import optimus.graph.ProbeSystem;
import optimus.graph.PropertyNode;
import optimus.graph.TweakableLeaf;
import optimus.platform.ScenarioStack;

/**
 * Shared helpers for the cache model checks (W3, H1). Lives in optimus.graph.cache for package access to
 * NodeCacheBase.table, NCEntry.value/next and NCEntryLRU.before/after; the private header/size of
 * NodeCacheWithLRU are read through a private lookup.
 *
 * <p>Keys are the P0 fixture's hand-written PropertyNodes (TweakableLeaf of one ProbeProperty on ProbeEntity(0)),
 * attached to one root stack, with the property's cache policy forced to Basic (no XSFT proxies). Nodes are never
 * run: the cache only needs equalsForCaching, the cache id and the cancel scope.
 */
public final class CacheMC {
  private CacheMC() {}

  public static final boolean AGENT_LOADED = agentLoaded();

  static {
    if (!AGENT_LOADED && !Boolean.getBoolean("mc.allowNoAgent"))
      throw new IllegalStateException("entity agent not loaded in this VM (pass -javaagent, or -Dmc.allowNoAgent=true)");
  }

  private static boolean agentLoaded() {
    try {
      var f = Class.forName("optimus.EntityAgent").getDeclaredField("instrumentation");
      f.setAccessible(true);
      return f.get(null) != null;
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }

  public static final ProbeDag DAG = new ProbeDag();
  public static final ProbeDag.Prop PROP;
  public static final ProbeSystem SYS;
  public static final ProbeProperty PP;
  public static final NodeTaskInfo INFO;
  public static final ScenarioStack ROOT;

  static {
    CacheFixtures.init();
    PROP = DAG.prop("p");
    SYS = new ProbeSystem(DAG);
    PP = SYS.pp(PROP);
    INFO = PP.info();
    INFO.setCachePolicy(NCPolicy.Basic);
    ROOT = CacheFixtures.newRoot(CacheFixtures.newRoot$default$1());
    if (INFO.cachePolicy() != NCPolicy.Basic) throw new IllegalStateException("policy " + INFO.cachePolicy());
  }

  /** A key node carrying a serial number (H1 tells nodes apart by it). */
  public static final class Tagged extends TweakableLeaf {
    public final int serial;

    Tagged(int arg, int serial) {
      super(SYS, PP, ProbeDag.key(PROP, 0, arg));
      this.serial = serial;
    }

    @Override public String toString() { return "k" + arg() + "#" + serial; }
  }

  /** A fresh key (scenarioStack attached, never run) for model arg {@code arg}. */
  @SuppressWarnings("unchecked")
  public static PropertyNode<Long> key(int arg) { return key(arg, -1); }

  @SuppressWarnings("unchecked")
  public static PropertyNode<Long> key(int arg, int serial) {
    var k = new Tagged(arg, serial);
    k.attach(ROOT);
    return (PropertyNode<Long>) (PropertyNode<?>) k;
  }

  public static int serial(PropertyNode<?> n) { return n == null ? -1 : n instanceof MCKey m ? m.serial : ((Tagged) n).serial; }

  public static int hash(PropertyNode<?> k) { return NCPolicy.Basic.hashOf(k); }

  /** Smallest arg > from whose key lands in the same bucket as {@code key(arg0)} in a table of {@code len}. */
  public static int collidingArg(int arg0, int len, int from) {
    int b = hash(key(arg0)) & (len - 1);
    for (int a = from; ; a++) if (a != arg0 && (hash(key(a)) & (len - 1)) == b) return a;
  }

  // ---------- caches ----------
  private static final AtomicInteger names = new AtomicInteger();

  /** UNodeCache(name, maxSize): concurrency 1 (no ledgers; every touch takes updateLock), batch 1. */
  public static UNodeCache cache(int maxSize) { return unregister(new UNodeCache(name(), maxSize)); }

  /** Full constructor: evictOnOverflow, evictOnLowMemory, no scoped-cache reduction. */
  public static UNodeCache cache(int maxSize, int concurrency, int batch, int padding) {
    return unregister(new UNodeCache(name(), maxSize, concurrency, batch, padding, true, true, false));
  }

  private static String name() { return "mc" + names.incrementAndGet(); }

  /** UNodeCache's constructor registers it in Caches.sharedCaches (strong); jcstress builds millions. */
  private static UNodeCache unregister(UNodeCache c) {
    Caches.unregisterSharedCache(c.getName());
    return c;
  }

  public static <T> PropertyNode<T> put(NodeCacheWithLRU c, PropertyNode<T> k) { return c.putIfAbsent(NCPolicy.Basic, k, null); }

  public static <T> PropertyNode<T> get(NodeCacheWithLRU c, PropertyNode<T> k) { return c.getIfPresent(INFO, k, null); }

  public static void clear(NodeCacheWithLRU c) { c.clear(CauseTestCase$.MODULE$); }

  // ---------- inspection (call on a quiescent cache) ----------
  static final VarHandle HEADER, SIZE;

  static {
    try {
      var l = MethodHandles.privateLookupIn(NodeCacheWithLRU.class, MethodHandles.lookup());
      HEADER = l.findVarHandle(NodeCacheWithLRU.class, "header", NCEntryLRU.class);
      SIZE = l.findVarHandle(NodeCacheWithLRU.class, "size", int.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static NCEntry[] table(NodeCacheWithLRU c) { return c.table; }

  public static int sizeField(NodeCacheWithLRU c) { return (int) SIZE.get(c); }

  /** Live (value non-null, not invalid) non-proxy entries in k's bucket of the current table that equal k for caching. */
  public static int liveFor(NodeCacheWithLRU c, PropertyNode<?> k) {
    NCEntry[] t = c.table;
    int n = 0;
    for (NCEntry e = t[hash(k) & (t.length - 1)]; e != null; e = e.next) {
      PropertyNode<?> v = e.value;
      if (v != null && !v.isInvalidCache() && !NCSupport.isProxy(v) && v.equalsForCaching(k)) n++;
    }
    return n;
  }

  public static Set<NCEntry> lruEntries(NodeCacheWithLRU c) {
    Set<NCEntry> s = Collections.newSetFromMap(new IdentityHashMap<>());
    NCEntryLRU h = (NCEntryLRU) HEADER.get(c);
    for (NCEntryLRU e = h.after; e != h; e = e.after) if (!e.removed()) s.add(e);
    return s;
  }

  public static Set<NCEntry> tableEntries(NodeCacheWithLRU c) {
    Set<NCEntry> s = Collections.newSetFromMap(new IdentityHashMap<>());
    for (NCEntry b : c.table) for (NCEntry e = b; e != null; e = e.next) if (!e.removed()) s.add(e);
    return s;
  }

  /**
   * Flushes the ledgers (getSize takes updateLock and runs catchupCore), then returns {LRU-not-table,
   * table-not-LRU, size field minus live LRU length}.
   */
  public static int[] desync(NodeCacheWithLRU c) {
    c.getSize();
    Set<NCEntry> l = lruEntries(c), t = tableEntries(c);
    int lNotT = 0, tNotL = 0;
    for (NCEntry e : l) if (!t.contains(e)) lNotT++;
    for (NCEntry e : t) if (!l.contains(e)) tNotL++;
    return new int[] {lNotT, tNotL, sizeField(c) - l.size()};
  }
}
