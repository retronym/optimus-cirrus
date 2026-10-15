package probes;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * Runtime side of the P3 delay injection. {@link DelayAgent} rewrites target classes to call
 * {@link #point(String)} at named points. The call is a no-op (one volatile read) unless a probe has
 * armed it:
 *
 * <ul>
 *   <li>{@link #arm(Steps.Ctl, String...)}: on the calling thread only, each hit of one of the named
 *       points becomes a {@code ctl.step()}, so the {@link Steps} schedule string controls the
 *       interleaving inside the instrumented method.
 *   <li>{@link #randomDelay(long, String...)}: on all unarmed threads, each hit of one of the named
 *       points (or any point if none named) parks for a random 0..maxNanos. For fuzz/hunter runs;
 *       also enabled from startup by -Dprobes.delay.maxNanos=N [-Dprobes.delay.points=a,b].
 * </ul>
 */
public final class Delay {
  private Delay() {}

  /** Points the agent actually inserted, "Class.method:id". Filled at class-load time. */
  public static final Set<String> instrumented = ConcurrentHashMap.newKeySet();

  private record Arm(Steps.Ctl ctl, Set<String> ids) {}

  private static final ThreadLocal<Arm> armed = new ThreadLocal<>();
  private static volatile boolean active;
  private static volatile long randomMaxNanos;
  private static volatile Set<String> randomIds;

  static {
    // -Dprobes.delay.maxNanos=N [-Dprobes.delay.points=steal.read,pop.read]: random delays from startup
    long n = Long.getLong("probes.delay.maxNanos", 0);
    String pts = System.getProperty("probes.delay.points", "");
    if (n > 0) randomDelay(n, pts.isEmpty() ? new String[0] : pts.split(","));
  }

  public static void point(String id) {
    if (!active) return;
    Arm a = armed.get();
    if (a != null) {
      if (a.ids.contains(id)) a.ctl.step();
      return;
    }
    long max = randomMaxNanos;
    if (max > 0) {
      Set<String> ids = randomIds;
      if (ids == null || ids.contains(id)) {
        long n = ThreadLocalRandom.current().nextLong(max);
        if (n < 1000) Thread.yield();
        else LockSupport.parkNanos(n);
      }
    }
  }

  /** Arm the named points on the current thread so that each hit calls {@code ctl.step()}. */
  public static void arm(Steps.Ctl ctl, String... ids) {
    armed.set(new Arm(ctl, Set.of(ids)));
    active = true;
  }

  public static void disarm() {
    armed.remove();
  }

  public static void randomDelay(long maxNanos, String... ids) {
    randomIds = ids.length == 0 ? null : Set.of(ids);
    randomMaxNanos = maxNanos;
    active = maxNanos > 0 || active;
  }

  static void recordInstrumented(String s) {
    instrumented.add(s);
  }
}
