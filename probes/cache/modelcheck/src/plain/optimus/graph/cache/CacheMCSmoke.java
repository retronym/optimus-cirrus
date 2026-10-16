package optimus.graph.cache;

import static optimus.graph.cache.CacheMC.*;

import java.util.Arrays;

/** Sequential sanity checks of the CacheMC helpers before running jcstress/Lincheck on them. */
public class CacheMCSmoke {
  static void check(String what, boolean ok) {
    System.out.println((ok ? "ok   " : "FAIL ") + what);
    if (!ok) failed = true;
  }

  static boolean failed;

  public static void main(String[] args) {
    System.out.println("agent=" + AGENT_LOADED + " policy=" + INFO.cachePolicy() + " info=" + INFO);
    for (int conc : new int[] {1, 2}) {
      UNodeCache c = conc == 1 ? cache(16) : cache(16, 2, 1, 1);
      System.out.println("--- concurrency " + conc + " table=" + table(c).length + " ledgers=" + c.getLedger());
      var k1 = key(1, 1);
      var k1b = key(1, 2);
      check("put returns own key on miss", put(c, k1) == k1);
      check("put of an equal fresh key returns the first", put(c, k1b) == k1);
      check("get hits", get(c, key(1)) == k1);
      check("liveFor == 1", liveFor(c, k1) == 1);
      int a2 = collidingArg(1, 16, 2);
      check("collidingArg " + a2 + " shares bucket", (hash(key(a2)) & 15) == (hash(k1) & 15));
      var k2 = key(a2);
      check("put colliding key", put(c, k2) == k2 && get(c, key(1)) == k1 && get(c, key(a2)) == k2);
      check("desync zero " + Arrays.toString(desync(c)), Arrays.equals(desync(c), new int[3]));
      for (int i = 100; i < 120; i++) put(c, key(i));
      check("size after overflow = 16: " + c.getSize(), c.getSize() == 16);
      check("k1 evicted", get(c, key(1)) == null);
      check("desync zero after eviction " + Arrays.toString(desync(c)), Arrays.equals(desync(c), new int[3]));
      c.setMaxSize(32);
      check("table 32 after setMaxSize(32)", table(c).length == 32);
      check("key 119 survives resize", get(c, key(119)) != null);
      check("desync zero after resize " + Arrays.toString(desync(c)), Arrays.equals(desync(c), new int[3]));
      c.setMaxSize(16);
      check("table 16 after setMaxSize(16)", table(c).length == 16 && c.getSize() == 16);
      clear(c);
      check("empty after clear", c.getSize() == 0 && get(c, key(119)) == null && tableEntries(c).isEmpty());
      check("desync zero after clear " + Arrays.toString(desync(c)), Arrays.equals(desync(c), new int[3]));
    }
    System.exit(failed ? 1 : 0);
  }
}
