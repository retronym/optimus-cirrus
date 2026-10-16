package probes;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Runtime side of {@link CacheDelayAgent}. Every inserted hook calls {@link #point(String)}, which
 *
 * <ol>
 *   <li>counts the hit when counting is on ({@link #startCounting()}),
 *   <li>runs a global handler registered for that id, on whatever thread hit it ({@link #onHit}); a handler may block
 *       (latch, park) to force a cross-thread interleaving, which {@link Delay#arm} cannot do since it is thread-local,
 *   <li>then delegates to {@link Delay#point(String)}, so {@code Delay.arm(ctl, ids)} (step schedules on the calling
 *       thread) and {@code Delay.randomDelay(maxNanos, ids)} / {@code -Dprobes.delay.maxNanos} work unchanged.
 * </ol>
 *
 * Cost when nothing is on: two volatile reads.
 */
public final class CacheDelay {
  private CacheDelay() {}

  /** Points the agent actually inserted, "Class.method:id". Filled at class-load time. */
  public static final Set<String> instrumented = ConcurrentHashMap.newKeySet();

  private static volatile boolean active;
  private static volatile boolean counting;
  private static final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Consumer<String>> handlers = new ConcurrentHashMap<>();

  public static void point(String id) {
    if (active) {
      if (counting) counts.computeIfAbsent(id, k -> new AtomicLong()).incrementAndGet();
      Consumer<String> h = handlers.get(id);
      if (h != null) h.accept(id);
    }
    Delay.point(id);
  }

  public static void startCounting() {
    counts.clear();
    counting = true;
    active = true;
  }

  public static void stopCounting() {
    counting = false;
    recomputeActive();
  }

  public static long count(String id) {
    AtomicLong c = counts.get(id);
    return c == null ? 0 : c.get();
  }

  public static Map<String, Long> counts() {
    TreeMap<String, Long> m = new TreeMap<>();
    counts.forEach((k, v) -> m.put(k, v.get()));
    return m;
  }

  public static void resetCounts() {
    counts.clear();
  }

  /** Run h(id) on every hit of id, on the hitting thread. Replaces any previous handler for id. */
  public static void onHit(String id, Consumer<String> h) {
    handlers.put(id, h);
    active = true;
  }

  public static void clearHandler(String id) {
    handlers.remove(id);
    recomputeActive();
  }

  public static void clearAll() {
    handlers.clear();
    counting = false;
    counts.clear();
    active = false;
  }

  private static void recomputeActive() {
    active = counting || !handlers.isEmpty();
  }

  static void recordInstrumented(String s) {
    instrumented.add(s);
  }
}
