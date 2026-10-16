package optimus.graph;

import java.util.*;
import org.jetbrains.kotlinx.lincheck.*;
import org.jetbrains.kotlinx.lincheck.annotations.*;
import org.jetbrains.kotlinx.lincheck.execution.*;
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions;
import org.jetbrains.kotlinx.lincheck.verifier.Verifier;

/**
 * H1 Lincheck model check of OGSchedulerContext.WorkQueue.
 *
 * <p>Operations: owner push/pop/tryLocalPopLast/scan (one Lincheck non-parallel group, so all on one
 * thread, as _top is owner-only), thief steal (any thread). Every operation returns the task id it
 * got (-1 for none), so the verifier sees every hand-out.
 *
 * <p>The real queue may hand a task out twice (R7), which no deterministic sequential spec can
 * express, so linearizability is checked by a custom {@link AtLeastOnceVerifier}: it searches for a
 * linearization (respecting Lincheck's happens-before clocks) against a nondeterministic spec, an
 * at-least-once deque: push appends at the top; pop takes the top, steal the bottom, scan the
 * topmost takeable task (moving the bottom task into its slot, as removeAt does); any consumer may
 * also return a task that was already delivered (a stale duplicate). A consumer may return none only
 * when the spec deque holds nothing it could take. After the post part, every task the spec still
 * holds must still be in the real queue's live region (read via @StateRepresentation): no loss.
 *
 * <p>Configuration via system properties (read in the constructor): mc.cap (array capacity, power
 * of two; small values exercise growArray), mc.start (initial top = base; near MAX_VALUE exercises
 * resetBounds).
 */
public class H1Lincheck {
  static final int MAX_TASKS = 64;
  static final int TID = 7, CID = 2; // scan's thread/causality; even task ids are takeable by it

  final OGSchedulerContext.WorkQueue q;
  final MC.T[] tasks = new MC.T[MAX_TASKS];
  int nextId; // owner-only

  public H1Lincheck() {
    int cap = Integer.getInteger("mc.cap", 4);
    int start = Integer.getInteger("mc.start", 0);
    q = MC.newQueue(start, start, cap);
    for (int i = 0; i < MAX_TASKS; i++) {
      tasks[i] = new MC.T(i);
      tasks[i].setCausality(i % 2 == 0 ? CID : 1, TID);
    }
  }

  static int id(NodeTask t) { return t == null ? -1 : ((MC.T) t).n; }

  @Operation(nonParallelGroup = "owner")
  public int push() {
    int id = nextId++;
    q.localPush(tasks[id]);
    return id;
  }

  @Operation(nonParallelGroup = "owner")
  public int pop() { return id(q.localPop(0)); }

  @Operation(nonParallelGroup = "owner")
  public void tryLocalPopLast() { if (nextId > 0) q.tryLocalPop(tasks[nextId - 1]); }

  @Operation(nonParallelGroup = "owner")
  public int scan() { return id(q.scanToFindWork(TID, CID)); }

  @Operation
  public int steal() { return id(q.tryToSteal()); }

  /** Live task ids in [base, top), bottom first; "!" marks a null hole. */
  @StateRepresentation
  public String state() {
    NodeTask[] a = MC.array(q);
    int m = a.length - 1;
    StringBuilder sb = new StringBuilder("[");
    for (int i = MC.base(q); i < MC.top(q); i++) {
      if (sb.length() > 1) sb.append(',');
      NodeTask t = a[i & m];
      sb.append(t == null ? "!" : String.valueOf(id(t)));
    }
    return sb.append("] top=").append(MC.top(q)).append(" base=").append(MC.base(q)).toString();
  }

  // ------------------------------------------------------------------------------------------
  /** Linearizability against the nondeterministic at-least-once deque spec described above. */
  public static final class AtLeastOnceVerifier implements Verifier {
    static long verified, needingDup, finalChecked;
    /** -Dmc.strict=true: exactly-once spec (no duplicate hand-outs), to show the known R7 traces. */
    static final boolean STRICT = Boolean.getBoolean("mc.strict");

    public AtLeastOnceVerifier(Class<?> seqSpec) {}

    record Spec(List<Integer> d, long delivered, int nextId, boolean usedDup) {}

    record Op(String name, Integer value, HBClock clock) {}

    @Override
    public boolean verifyResults(ExecutionScenario scenario, ExecutionResult result) {
      List<Op> init = ops(scenario.getInitExecution(), result.getInitResults(), null);
      List<List<Op>> par = new ArrayList<>();
      List<List<Actor>> pe = scenario.getParallelExecution();
      List<List<ResultWithClock>> pr = result.getParallelResultsWithClock();
      int ownerThreads = 0;
      for (int t = 0; t < pe.size(); t++) {
        List<Op> l = new ArrayList<>();
        boolean owner = false;
        for (int i = 0; i < pe.get(t).size(); i++) {
          String n = pe.get(t).get(i).getMethod().getName();
          owner |= !n.equals("steal");
          l.add(op(n, pr.get(t).get(i).getResult(), pr.get(t).get(i).getClockOnStart()));
        }
        if (owner) ownerThreads++;
        par.add(l);
      }
      if (ownerThreads > 1) throw new IllegalStateException("owner operations on more than one thread: " + scenario);
      List<Op> post = ops(scenario.getPostExecution(), result.getPostResults(), null);
      Set<Integer> remaining = parseRemaining(result.getAfterPostStateRepresentation());

      Set<Spec> afterInit = new HashSet<>();
      runSeq(new Spec(List.of(), 0, 0, false), init, 0, afterInit);
      Set<Spec> afterPar = new HashSet<>();
      for (Spec s : afterInit) linearize(s, par, new int[par.size()], afterPar, new HashSet<>());
      Set<Spec> afterPost = new HashSet<>();
      for (Spec s : afterPar) runSeq(s, post, 0, afterPost);
      boolean ok = false, okNoDup = false;
      for (Spec s : afterPost) {
        if (remaining == null || remaining.containsAll(s.d)) {
          ok = true;
          okNoDup |= !s.usedDup;
        }
      }
      synchronized (AtLeastOnceVerifier.class) {
        verified++;
        if (ok && !okNoDup) needingDup++;
        if (remaining != null) finalChecked++;
      }
      return ok;
    }

    static Set<Integer> parseRemaining(String rep) {
      if (rep == null) return null;
      String inner = rep.substring(rep.indexOf('[') + 1, rep.indexOf(']'));
      Set<Integer> s = new HashSet<>();
      if (!inner.isEmpty()) for (String x : inner.split(",")) if (!x.equals("!")) s.add(Integer.parseInt(x));
      return s;
    }

    static List<Op> ops(List<Actor> actors, List<? extends Result> results, HBClock c) {
      List<Op> l = new ArrayList<>();
      for (int i = 0; i < actors.size(); i++) l.add(op(actors.get(i).getMethod().getName(), results.get(i), c));
      return l;
    }

    static Op op(String name, Result r, HBClock c) {
      Integer v = null;
      if (r instanceof ValueResult vr) v = (Integer) vr.getValue();
      else if (!(r instanceof VoidResult)) v = Integer.MIN_VALUE; // exception etc.: matches nothing
      return new Op(name, v, c);
    }

    static void runSeq(Spec s, List<Op> ops, int i, Set<Spec> out) {
      if (i == ops.size()) { out.add(s); return; }
      for (Spec n : step(s, ops.get(i))) runSeq(n, ops, i + 1, out);
    }

    static void linearize(Spec s, List<List<Op>> par, int[] pos, Set<Spec> out, Set<String> seen) {
      if (!seen.add(Arrays.toString(pos) + s)) return;
      boolean done = true;
      for (int t = 0; t < par.size(); t++) {
        if (pos[t] == par.get(t).size()) continue;
        done = false;
        Op op = par.get(t).get(pos[t]);
        boolean enabled = true;
        for (int u = 0; u < par.size(); u++) if (u != t && op.clock.get(u) > pos[u]) enabled = false;
        if (!enabled) continue;
        for (Spec n : step(s, op)) {
          pos[t]++;
          linearize(n, par, pos, out, seen);
          pos[t]--;
        }
      }
      if (done) out.add(s);
    }

    static boolean takeable(int id) { return id % 2 == 0; }

    /** All spec states reachable by applying op with its observed result. */
    static List<Spec> step(Spec s, Op op) {
      List<Spec> r = new ArrayList<>();
      List<Integer> d = s.d;
      Integer v = op.value;
      switch (op.name) {
        case "push" -> {
          if (v != null && v == s.nextId) r.add(new Spec(with(d, d.size(), v), s.delivered, s.nextId + 1, s.usedDup));
        }
        case "tryLocalPopLast" -> {
          if (!d.isEmpty() && d.get(d.size() - 1) == s.nextId - 1)
            r.add(new Spec(without(d, d.size() - 1), s.delivered | (1L << (s.nextId - 1)), s.nextId, s.usedDup));
          else r.add(s);
        }
        case "pop", "steal", "scan" -> {
          int idx;
          if (op.name.equals("pop")) idx = d.size() - 1;
          else if (op.name.equals("steal")) idx = d.isEmpty() ? -1 : 0;
          else {
            idx = -1;
            for (int i = d.size() - 1; i >= 0; i--) if (takeable(d.get(i))) { idx = i; break; }
          }
          if (v == null) break;
          if (v == -1) {
            if (idx < 0) r.add(s);
          } else {
            if (idx >= 0 && d.get(idx).equals(v)) {
              List<Integer> nd;
              if (op.name.equals("scan") && idx > 0) { // removeAt: bottom task moves into the slot
                nd = new ArrayList<>(d);
                nd.set(idx, nd.get(0));
                nd.remove(0);
              } else nd = without(d, idx);
              r.add(new Spec(nd, s.delivered | (1L << v), s.nextId, s.usedDup));
            }
            if (!STRICT && v >= 0 && v < 64 && (s.delivered & (1L << v)) != 0) r.add(new Spec(d, s.delivered, s.nextId, true));
          }
        }
        default -> throw new IllegalStateException(op.name);
      }
      return r;
    }

    static List<Integer> with(List<Integer> d, int i, int v) { var n = new ArrayList<>(d); n.add(i, v); return List.copyOf(n); }
    static List<Integer> without(List<Integer> d, int i) { var n = new ArrayList<>(d); n.remove(i); return List.copyOf(n); }
  }

  // ------------------------------------------------------------------------------------------
  static ExecutionScenario scenario(List<String> init, List<List<String>> par, List<String> post) {
    return new ExecutionScenario(actors(init), par.stream().map(H1Lincheck::actors).toList(), actors(post), null);
  }

  static List<org.jetbrains.kotlinx.lincheck.Actor> actors(List<String> names) {
    List<org.jetbrains.kotlinx.lincheck.Actor> l = new ArrayList<>();
    for (String n : names) {
      try {
        l.add(new org.jetbrains.kotlinx.lincheck.Actor(H1Lincheck.class.getMethod(n), List.of()));
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(n, e);
      }
    }
    return l;
  }

  /** run.sh lincheck [name=cap:start ...]; defaults cover small-array grow, default array, and wrap. */
  public static void main(String[] args) {
    MC.ss();
    List<String> configs = args.length > 0 ? List.of(args) : List.of("grow=4:0", "plain=64:0", "wrap=4:" + (Integer.MAX_VALUE - 5));
    int iterations = Integer.getInteger("mc.iterations", 100);
    int invocations = Integer.getInteger("mc.invocations", 5000);
    int threads = Integer.getInteger("mc.threads", 3);
    int perThread = Integer.getInteger("mc.actorsPerThread", 3);
    boolean failed = false;
    for (String c : configs) {
      String name = c.substring(0, c.indexOf('='));
      String[] cs = c.substring(c.indexOf('=') + 1).split(":");
      System.setProperty("mc.cap", cs[0]);
      System.setProperty("mc.start", cs[1]);
      AtLeastOnceVerifier.verified = AtLeastOnceVerifier.needingDup = AtLeastOnceVerifier.finalChecked = 0;
      long t0 = System.nanoTime();
      System.out.println("=== Lincheck model checking: " + name + " (capacity " + cs[0] + ", start " + cs[1] + "), "
          + iterations + " iterations x " + invocations + " invocations, " + threads + " threads x " + perThread + " actors");
      try {
        var opts = new ModelCheckingOptions()
            .iterations(iterations)
            .invocationsPerIteration(invocations)
            .threads(threads)
            .actorsPerThread(perThread)
            .actorsBefore(Integer.getInteger("mc.actorsBefore", 4))
            .actorsAfter(Integer.getInteger("mc.actorsAfter", 3))
            .verifier(AtLeastOnceVerifier.class);
        // Hand-written scenarios that force growArray (capacity 4: the 4th live task grows) and, for
        // the wrap configuration, resetBounds, while two thieves steal.
        opts.addCustomScenario(scenario(List.of("push", "push", "push"),
            List.of(List.of("push", "push", "pop"), List.of("steal", "steal"), List.of("steal")),
            List.of("pop", "steal", "scan", "pop")));
        opts.addCustomScenario(scenario(List.of("push", "push", "push"),
            List.of(List.of("push", "scan", "push", "tryLocalPopLast"), List.of("steal", "steal"), List.of("steal")),
            List.of("pop", "steal", "scan", "pop")));
        LinCheckerKt.check(opts, H1Lincheck.class);
        System.out.println("=== " + name + ": PASSED");
      } catch (Throwable e) {
        failed = true;
        System.out.println("=== " + name + ": FAILED");
        System.out.println(e.getMessage());
        if (!(e instanceof AssertionError)) e.printStackTrace(System.out);
      }
      System.out.printf("=== %s: %d executions verified (%d with final-state check), %d needed a duplicate hand-out to linearize; %.1fs%n",
          name, AtLeastOnceVerifier.verified, AtLeastOnceVerifier.finalChecked, AtLeastOnceVerifier.needingDup, (System.nanoTime() - t0) / 1e9);
    }
    System.exit(failed ? 1 : 0);
  }
}
