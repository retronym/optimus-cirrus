package probes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Two-or-more-thread step harness. Each participant is a lambda that receives a {@link Ctl} and
 * calls {@code ctl.step()} between the actions it wants to be scheduled. The schedule is a string
 * like "A1 B1 A2 B2": letters name participants in registration order, numbers are 1-based step
 * counts. A participant's step k is released only when the schedule reaches token Xk; everything a
 * participant does after its last listed step runs unscheduled once the schedule is exhausted.
 *
 * <p>A participant that reaches a step() not in the schedule waits until the schedule is exhausted,
 * then proceeds freely. If the schedule names a step the participant never reaches (it returned
 * early), the harness times out and reports it.
 */
public final class Steps {
  public static final class Ctl {
    private final Steps owner;
    final int id;
    int stepsTaken;

    Ctl(Steps owner, int id) {
      this.owner = owner;
      this.id = id;
    }

    /** Block until the schedule releases this participant's next step. */
    public void step() {
      stepsTaken++;
      owner.await(id, stepsTaken);
    }
  }

  private final List<Consumer<Ctl>> participants = new ArrayList<>();
  private final String[] tokens;
  private int cursor; // index into tokens of the token currently waiting to be released
  private boolean exhausted;
  private final Object lock = new Object();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  private Steps(String schedule) {
    tokens = schedule.trim().isEmpty() ? new String[0] : schedule.trim().split("\\s+");
    exhausted = tokens.length == 0;
  }

  public static Steps schedule(String schedule) {
    return new Steps(schedule);
  }

  public Steps thread(Consumer<Ctl> body) {
    participants.add(body);
    return this;
  }

  private void await(int id, int stepNo) {
    String mine = (char) ('A' + id) + "" + stepNo;
    synchronized (lock) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!exhausted && !tokens[cursor].equals(mine)) {
        long left = deadline - System.nanoTime();
        if (left <= 0)
          throw new IllegalStateException(
              "step " + mine + " waited 5s; schedule stuck at " + tokens[cursor]);
        try {
          lock.wait(Math.max(1, left / 1_000_000));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      if (!exhausted) {
        cursor++;
        if (cursor == tokens.length) exhausted = true;
        lock.notifyAll();
      }
    }
  }

  /** Runs all participants, returns when all have finished. Rethrows the first failure. */
  public void run() {
    int n = participants.size();
    CountDownLatch done = new CountDownLatch(n);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Ctl ctl = new Ctl(this, i);
      Consumer<Ctl> body = participants.get(i);
      Thread t =
          new Thread(
              () -> {
                try {
                  body.accept(ctl);
                } catch (Throwable e) {
                  failure.compareAndSet(null, e);
                  synchronized (lock) {
                    exhausted = true; // release everyone else
                    lock.notifyAll();
                  }
                } finally {
                  done.countDown();
                }
              },
              "steps-" + (char) ('A' + i));
      threads.add(t);
    }
    for (Thread t : threads) t.start();
    try {
      if (!done.await(20, TimeUnit.SECONDS)) {
        StringBuilder sb = new StringBuilder("harness timeout; schedule cursor=");
        synchronized (lock) {
          sb.append(exhausted ? "exhausted" : tokens[cursor]);
        }
        for (Thread t : threads) {
          sb.append("\n  ").append(t.getName()).append(" ").append(t.getState());
          for (StackTraceElement e : t.getStackTrace()) sb.append("\n    at ").append(e);
        }
        throw new IllegalStateException(sb.toString());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Throwable f = failure.get();
    if (f != null) {
      if (f instanceof RuntimeException re) throw re;
      if (f instanceof Error er) throw er;
      throw new RuntimeException(f);
    }
  }

  /** All permutations of the given per-participant step counts, as schedule strings. */
  public static List<String> permutations(int... stepsPerParticipant) {
    List<String> out = new ArrayList<>();
    int[] taken = new int[stepsPerParticipant.length];
    permute(stepsPerParticipant, taken, new StringBuilder(), out);
    return out;
  }

  private static void permute(int[] max, int[] taken, StringBuilder cur, List<String> out) {
    boolean any = false;
    for (int p = 0; p < max.length; p++) {
      if (taken[p] < max[p]) {
        any = true;
        taken[p]++;
        int len = cur.length();
        if (len > 0) cur.append(' ');
        cur.append((char) ('A' + p)).append(taken[p]);
        permute(max, taken, cur, out);
        cur.setLength(len);
        taken[p]--;
      }
    }
    if (!any) out.add(cur.toString());
  }
}
