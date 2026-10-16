# Adversarial test campaign for the cirrus scheduler

Companion to [SCHEDULER_REVIEW.md](SCHEDULER_REVIEW.md). Goal: turn each reported hazard into a deterministic or statistically reliable reproducer, then widen the net with techniques that find hazards nobody has named yet. Everything here is parked as TODO until the build situation is settled (see "Prerequisites").

Principle: attack the invariants, not the API. The scheduler's correctness rests on a handful of claims (every runnable task is reachable by some thread; a waiting thread is always woken; a node completes exactly once; causality never lets a thread wait on something below it on its own stack; queue indices stay consistent). Each probe should state which claim it is trying to falsify and what "falsified" looks like (hang, panic exit code 66/121, assertion, wrong result).

## Prerequisites

- **P0 Harness.** A `main`-based runner (not a test framework) that builds a scheduler with chosen `Settings`, runs a scenario N times, and fails loudly on hang (watchdog thread that dumps `OGScheduler.toString()` and `jstack` then `System.exit(3)`), on panic (install a hook on `InfoDumper.graphPanic` or run in a child JVM and check the exit code), and on wrong result. Child-JVM execution is preferred: most hazards end in `System.exit`.
- **P1 Knobs.** Every scenario runs under the matrix `optimus.scheduler.asserts ∈ {true,false}` × `optimus.graph.limitThreads ∈ {true,false}` × `optimus.gthread.ideal ∈ {1, 2, 4, cores}`. Asserts on is the oracle; asserts off is production.
- **P2 State injection.** A small `Unsafe`/VarHandle helper in package `optimus.graph` to set private state: `OGScheduler.threadCounter`, `WorkQueue._top/_base`, `NodeTask._state`. Cheaper than waiting 2^31 pushes.
- **P3 Delay injection.** A ByteBuddy or entityagent transformer that inserts `Thread.yield()`/`parkNanos(random)` at chosen bytecode offsets (after every volatile read/CAS in `WorkQueue`, `tryAddToWaiterList`, `notifyWaiters`, `tryWakeupThread`). This is the single most effective way to widen interleavings on a 10-core box.
- **P4 Model-checking tier.** jcstress for the lock-free pieces (`WorkQueue`, `_waiters` CAS list, `_state` transitions) and Lincheck for linearizability of `WorkQueue` against a sequential deque spec. Both run the unit in isolation and do not need the entity agent if the class can be loaded standalone (TBD from the build attempt).

## Tier 0: whitebox component probes (isolated, primed)

Each component is exercised on its own, in-package (`optimus.graph`) so package-private members are reachable, with internal state set directly to the edge where the hazard lives and with the interleaving *forced* by a two-thread step harness (each thread advances one step on a `Phaser`/`CountDownLatch`, so the schedule is a parameter, not luck). These need none of the scheduler's static initialisation if the class loads standalone; where it does not, a stub `Settings`/`OGTrace` on the classpath ahead of the real one is acceptable for this tier only.

Step harness: `Steps.run(threadA = { s1; s2; s3 }, threadB = { t1; t2 }, schedule = "A1 B1 A2 B2 A3")`. Every probe below lists the schedule that should expose the bug; a probe also runs the schedule's permutations to find ones nobody predicted.

### W1 `WorkQueue` (OGSchedulerContext.java:368–620)

Construct directly; `schedulerContext` null (global-queue mode) and non-null with `currentCausalityID > 0` (sync-stack mode, so `growArray` keeps `nullTask` placeholders).

- **W1a wrap at MAX.** Set `_top = _base = Integer.MAX_VALUE - 3`. Owner pushes 3, thief steals 1, owner pushes 1 more (crosses MAX). Check `size()`, `isEmpty()`, then owner pops until empty: every pushed task must come back exactly once across owner+thief. Repeat with the thief stealing the element at `base == MAX_VALUE` so `_base` becomes `MIN_VALUE` (R2 case 1); then owner push, pop, `scanToFindWork(…, causality 1)`.
- **W1b marker after reset.** Set `_top = MAX_VALUE - 1`, record `marker = getTop()`, push once (reset fires), push 5 more, then `localPop(marker)`. Expect the 5 to be poppable; today it returns null.
- **W1c pop/steal on one element.** Schedule: owner reads slot (A1), thief CASes and bumps base (B1,B2), owner CAS fails and returns stale `r` (A2). Assert `_top`/`_base` consistent afterwards and the returned task is the same object both saw (double dispatch is allowed; a *lost* task is not).
- **W1d grow during steal.** Fill to capacity-1; schedule thief read-slot (B1), owner push → `growArray` copy (A1), thief CAS (B2), owner swap arrays (A2). Assert task is in old or new array or taken, never dropped; `_base` after swap ≥ thief's base.
- **W1e removeAt vs steal.** `scanToFindWork` with causality that matches only a middle element while a thief runs; assert no null hole below `_top` with live elements under it, and the matched element is returned once.
- **W1f localCleanup with incomplete tasks.** Queue of [done, live, done, done]; `localCleanup(null)` leaves `live` and stops; assert the dones below it are gone or report that they remain (F8-style retention).
- **W1g resize with sync marker.** `currentCausalityID = 1`, queue of 70 tasks of which 60 are done; push to force grow; assert `nullTask` placeholders preserve positions so a marker taken before the grow still fences correctly.

### W2 `IndexedArrayList` and `Contexts` (optimus/core/IndexedArrayList.java)

- **W2a foreach vs tryRemove.** Schedule: iterator reads `q[i]` (A1), remover moves last into `i` and nulls last (B1), iterator continues (A2). Assert visited set ⊆ live ∪ removed and count of visits per item ≤ 1; today the moved item is skipped. This is R3 with the list alone.
- **W2b foreach vs add-with-grow.** Iterator holds old array while `add` doubles; assert the iterator terminates (it does, nulls) and report which items it misses.
- **W2c tryStealing count race.** `Contexts` with 3 items; reader copies `q` and `count`; writer pops 2; reader indexes with the stale count. Assert no AIOOBE and null-tolerance (OGScheduler.java:1391 already guards with `Math.min`, so this documents the guard).

### W3 `NodeTask` state and waiter list

A concrete `NodeTask` subclass with no-op `run`, scenario stack = `ScenarioStack.constant()` (or stub).

- **W3a add vs notify.** Schedule: adder reads `_waiters` (A1), completer CASes `completed` and iterates (B1,B2), adder CASes (A2 fails, returns false → caller invokes callback inline). Assert the waiter's `onChildCompleted` is called exactly once. Permute.
- **W3b continueWithLast ordering under contention.** N adders with `tryAddToWaiterListLast` while one uses `tryAddToWaiterList`; assert the "last" waiter is notified after all others registered before it.
- **W3c double complete.** Two threads call `completeWithResult` on a primed `STATE_STARTED|NOT_RUNNABLE` node; run in a child JVM; assert exactly one exit(121). Then same with `tryCompleteWithResult` vs `completeWithResult` to show the "try" variant does not protect (F1).
- **W3d causality packing.** `setCausality(1, 32768)` then `causalityThreadID()`; `setCausality(1, 65537)` vs `(1, 1)`; `poisonCausality()`. Assert round-trip (fails today) and that `OGScheduler.canTake` is false across the aliasing pair (R1). Pure unit test, no threads.
- **W3e invalidate while waited.** Prime a node to running; register a `SyncFrame`-like awaiter; `invalidateCache`; assert `isDoneEx()` true but waiter not notified; then `complete` and assert the stale awaiter is called (R6 mechanism).
- **W3f markAsRunnable on a done node.** With and without asserts; documents that production silently ignores it.
- **W3g tryRun idempotence.** Two threads `tryRun` the same runnable node; exactly one sees `NOT_RUNNABLE` clear in the previous state.

### W4 `SyncStack` / `SyncFrame` (OGSchedulerContext.java:195–330)

- **W4a frame reuse.** Push A, pop, push B; assert `frames[0]` is the same object and its `_waitingOn` from A's registration was cleared before reuse (it is not; R6).
- **W4b concurrentlyVisitNodesOnStack during pop.** Visitor reads `count` (A1), owner pops (B1), visitor reads `frames[count-1].ntsk` (A2) → null; assert visitor tolerates it.

### W5 `OutOfWorkListener` bookkeeping (OGScheduler.java:1457–1619)

Needs an `OGScheduler` instance but no threads: with `forcedSingleThreaded=true` and the graph empty, `runOnStalledCallbacks` can be invoked reflectively.

- **W5a CME.** Listener whose `checkForNewWork` removes itself; call `runOnStalledCallbacks` → expect CME (F2).
- **W5b re-add during callback.** Listener re-registers itself in `onGraphStalled`; assert it lands in the pending list and is present after the pass, called once per pass.
- **W5c priority gating.** Priorities [0,0,1]; first callback adds work; assert the priority-1 listener is not called this pass; then no work added and all three are called.
- **W5d contains/remove cost.** 50,000 listeners; time `addOutOfWorkListener` and `removeOutOfWorkListener` (I4).

### W6 `Context` bookkeeping transitions (OGScheduler.java:93–176, 627–760)

Drive one real `OGScheduler` with `ideal=2`, but call the package-private transitions directly from a controlled thread: `enterGraph`, `exitGraph`, `tryWakeupThread`, `injectThread`, `nextStepForWorkingThread`, with `_lock` held by the test where the method requires it.

- **W6a wakeup of a non-waiter.** `tryWakeupThread` on a ctx in workers; assert returns false and `awaitedTask` untouched.
- **W6b asIfRunning id collision.** `asIfRunning` stamps a task; assert `causalityThreadID()==1` equals the first injected worker's id (F4).
- **W6c injectThread after shutdown.** `shutdown()` then `enqueue`; assert a thread is still created (F3) and whether that is intended.
- **W6d spinner invariant.** After each transition, `_spinningThreadID == 0 || workers.contains(that ctx)`; crank through all pairs of transitions.
- **W6e thread start under lock.** Hold `_lock` from the test thread, call `enqueue` from another; measure that the caller blocks for the duration of a `Thread.start` (I3) by stubbing `OGThread` with a slow constructor via the step harness.

### W7 `fullWait` entry cost (OGScheduler.java:939–960)

Call `wait` through a real sync `get` whose child is completed by a timer thread 1 ms later; count `Exception` allocations with an allocation sampler or by subclassing `PrettyStringBuilder` through a stub. Shows I1 per blocking wait with profiling off.

Exit criterion for Tier 0: every W-probe either demonstrates the reported mechanism or is recorded as "not reproducible as described" with the schedule that was tried. New failures found by schedule permutation get their own R/F ids in the review.

## Tier 1: probes for reported hazards

| Id | Hazard | Probe | Falsified when |
|----|--------|-------|----------------|
| T1 | R1 thread-id overflow | Set `threadCounter` to 32,760 then 65,530 via P2. Run a sync-stack-heavy scenario (every node calls `child.get` from inside `run`) on `ideal=2, limitThreads=true`. | Stall > deadlock timeout (1 s) with runnable work on the global queue; at 65,530+ a sync-stack deadlock or a `GraphInInvalidState` under asserts. |
| T2 | R2 index wrap | Set a worker's `WorkQueue._top = _base = MAX_VALUE - 100` via P2, then run fan-out of 1,000 children per node with a thief thread stealing concurrently. Second variant: set `_top` so the wrap lands while a sync frame holds a `currentLocalQueueMarker`. | `size()` negative, `isEmpty()` wrong, owner cannot drain its own queue (observe via `drainQueue` returning early while `foreach` shows tasks), or a hang. |
| T3 | R3 tracked-nodes iteration | 8 non-graph threads hammer `enqueueAndTrackCompletion` with trivial nodes while the single graph thread repeatedly stalls (empty graph, so `findWork` reaches the `_evalAsyncTrackedNodes.foreach`). Run under P3 with a delay inside `foreach`. | `NullPointerException`/`ArrayIndexOutOfBounds` from `foreach`, or detectCycles called on a task already removed. |
| T4 | R4 cross-scheduler lock order | Two `OGScheduler`s, `keepTaskSchedulerAffinity=true`. Build two XSFT proxy cycles whose recovery enqueues onto the other scheduler. Trigger cycle detection on both simultaneously (two threads each `get` on their own cycle root). Needs an XSFT fixture; if too heavy, substitute a `NodeTask` subclass whose `tryToRecoverFromCycle` enqueues to the other scheduler. | Both threads blocked in `synchronized(_lock)` / `_visitLock.lock()` per jstack; no progress. |
| T5 | R5 busy-poll on plain fields | Cancel a scope while a node runs a 2 s busy loop; call `waitForCancelledNodesToStop` from another thread. Run with `-XX:+UseSerialGC -Xcomp` and a long chain so `containsEQ` is expensive. | Wall time of the wait ≫ 2 s (spin cost), or (with a compiler that hoists) never returns. Also measure CPU burned. |
| T6 | R6 SyncFrame reuse | Node A is waited on sync by thread T; from another thread call `A.invalidateCache` while A is in flight; T then does a second sync wait on B; then complete A. Under asserts add a check that a popped frame has `_waitingOn == null`. | Assertion fires, or B's wait is woken/`setWaitingOn(null)` by A's completion. |
| T7 | F1 double completion | Node whose `run` calls `completeWithResult` then throws. Run in child JVM. | Exit code 121. Then decide whether that is the wanted behaviour. |
| T8 | F2 CME in callbacks | `OutOfWorkListener` whose `checkForNewWork` calls `scheduler.removeOutOfWorkListener(this)`. Register two, stall the graph. | Exit code 66 with CME in the log. |
| T9 | F3 unbounded injection | 10,000 calls to `evaluateOnGraphThreadAndTrackCompletion` with nodes that block on a latch. | Thread count ≫ ideal; OOM on thread stacks with `-Xss512k` and a small heap. |
| T10 | F4 `asIfRunning` id 1 | Enqueue from a non-graph thread via `asIfCalledFrom` while the real thread 1 holds a sync stack; node completes with a sync `get` on something lower on thread 1's stack. | Deadlock that disappears when `asIfRunning` uses a fresh id. |
| T11 | I1 wait cost | Micro-benchmark (JMH or a plain loop) of a sync `get` that always blocks on a cross-thread child, with and without the `toString`/`new Exception` patched out. | Quantifies the overhead; not a falsification. |
| T12 | I2 spinner cost | One node that busy-loops 5 s, `ideal=4`. Sample CPU per thread via `ThreadMXBean`. | Spinner thread shows ~100% CPU for 5 s. |

## Tier 2: invariant hunters (find new hazards)

### H1 Queue linearizability (Lincheck + jcstress)

Extract `WorkQueue` as a unit. Lincheck operations: owner `localPush`, owner `localPop`, owner `tryLocalPop(x)`, thief `tryToSteal` (N thieves), `growArray` via push overflow, `scanToFindWork`/`removeAt`. Spec: a multiset in which each task may be observed by several consumers but every pushed non-null task is observed at least once before the queue reports empty with no thieves active. The "off by +1" states documented in the class comment should be encoded as allowed states so the checker finds the undocumented ones. jcstress cases: `localPop` vs `tryToSteal` on a one-element queue (expected outcomes {owner, thief, both-see-it}); `growArray` vs `tryToSteal` (task must appear in exactly one array or both, never neither); `removeAt` vs `tryToSteal` with `base` moving.

### H2 Waiter list and completion (jcstress)

`tryAddToWaiterList` vs `notifyWaiters`: every waiter registered before the CAS to `completed` is called exactly once; a waiter registered after sees `false` and is never called. `tryAddToWaiterListLast` ordering. `complete` vs `complete` from two threads: exactly one wins and the other must be observable (today it is `exit(121)`, so run in a child JVM and count).

### H3 Lost-wakeup search

Scenario generator: random DAGs (10–10,000 nodes, fan-in/fan-out from a seed), random subset of edges evaluated via sync `get` from inside `run` (creates sync stacks), random subset via `continueWith`, random nodes completed from non-graph threads (`NodePromise`), random `evaluateAsync`. Run each seed 200× under P3 with delays after every `parked`/`awaitedTask` write and before every `park()`. Watchdog at 10× the unperturbed wall time. A hang is a lost wakeup or a causality stall; the dump distinguishes them (waiters with runnable work on queues = causality; waiters with `parked=false` and nothing running = lost wakeup).

### H4 Causality fuzz

Same generator, but record for every blocking wait the pair (thread id, causality id) and every task's stamped causality at the moment it is taken. Offline check: no thread ever ran a task whose stamp belongs to another thread's deeper sync frame. This checks the invariant `canTake` claims to enforce, independently of whether a deadlock happened to result.

### H5 Thread accounting

Instrument `enterGraph`/`exitGraph`/`injectThread`/`nextStepForWorkingThread` to log transitions. Invariants: every `Context` is in exactly one of {workers, waiters, idle, externallyBlocked, none}; `_spinningThreadID` names a context that is in workers; after the graph is quiescent `_workers.count == _waiters.count == 0` and `schedulerInBadState()` is false. Run after every scenario in H3; this catches leaks like T9 and spinner-stuck states for free.

### H6 Cancellation and exceptions mid-flight

Random DAGs with random cancellation of a scope while nodes are in every state (new, enqueued, running, waiting on a sync child, completed). Check: every node ends done; no node completes twice; `waitForCancelledNodesToStop` returns; a node cancelled while on a sync stack produces `GraphInInvalidState` rather than a hang. Combine with H3 delays.

### H7 Multi-scheduler

Two to four schedulers with `keepTaskSchedulerAffinity` on and off; nodes attached to scenario stacks naming different schedulers; cross-scheduler `continueWith` and `enqueue`. Oracle: H3/H5 invariants plus "every node ran on the scheduler its scenario stack names" when affinity is on.

### H8 Resource exhaustion

- `-Xss256k` with `syncStackSafetyLimit` raised to 10,000: deep sync stacks should go through the causality bump, not `StackOverflowError`.
- `proxyChainStackLimit` lowered to 2 and long `continueWith` chains: exercises `delayOnChildCompleted`.
- Heap pressure: 1 M completed nodes retained by `_queue` between stalls (F8) — measure retained size with a heap histogram.

### H9 Virtual threads

Repeat T1–T12 and H3 with `optimus.graph.useVirtualThreads=true` on a loom-on build. The code paths differ (no idle pool, `limitThreads=false`, `ctx.shutdown` always). Expect new findings here: this configuration has the least shared history with the platform-thread paths.

### H10 Long-haul soak

A 24 h run of H3 scenarios in a loop on a 4-core box with `threadCounter` and `_top` *not* pre-set, to see whether the wrap hazards are reachable organically and whether anything else drifts (thread count, heap, callback queue size).

## Order of attack

1. P0–P2, then Tier 0 (W1, W3d, W2a, W5a first: no threads or a two-thread schedule, decisive).
2. T2, T1, T7, T8, then H1 and H2 (model checking; independent of the harness).
3. P3, then H3 + H5 (the generator with invariants is where new hazards will come from).
4. T3–T6, T9–T12.
5. H4, H6, H7, H8, H9.
6. H10 as a background job once the rest is green.

## Status

- Build: done (see SCHEDULER_REVIEW.md "Build and run"). Probes run in-process with `-javaagent`; `probes/run.sh` in the worktree compiles and runs them.
- Step harness `probes/src/probes/Steps.java`: done.
- Tier 0: every probe that needs no delay injection has run (`probes/RESULTS.txt`). Results and amendments in SCHEDULER_REVIEW.md "Tier 0 probe results". Demonstrated: W1b, W1f, W2a, W2b, W3c (as new F1b), W3d, W3e, W3f, W4a, W5a, W5d, W6c. Not reproduced or quantified: the rest. Withdrawn/downgraded: R2 base-overrun case, F4, I1, I3.
- P3 delay injection: done (ASM agent, `probes/src/probes/DelayAgent.java`, armed per thread via `Delay.arm(ctl, ids...)`, or random parks via `-Dprobes.delay.maxNanos=N` for hunter runs). W1c–e run: W1c and W1e demonstrated (stale returns), W1d not reproduced (resurrection noted). Tier 0 complete.
- Tier 1 T1, T2, T7, T8: done (`probes/run-tier1.sh`, `probes/RESULTS-tier1.txt`). All four demonstrated; T1 is a permanent hang, T7 and T8 end in a surviving JVM or a hang instead of the predicted exit because `InfoDumper` cannot initialise here (F1b, F2b). Next: H1/H2 (jcstress/Lincheck on WorkQueue and the waiter list), then H3 with `-Dprobes.delay.maxNanos`, then T3–T6, T9–T12.
- H1/H2 model checking: done (`probes/modelcheck/`). WorkQueue: no loss anywhere, R7 double hand-outs quantified. Waiters: protocol holds. New R8 from H2e: `markAsRunnable` vs `complete` leaves a done node runnable (forbidden 2% of samples). Lincheck cannot instrument the agent-rewritten `NodeTask`.

## Open questions

- Resolved: `OGScheduler` cannot load without the entity agent; probes run with `-javaagent` in-process, so P4 (jcstress/Lincheck) needs the agent on the forked JVMs' command line.
- Whether XSFT fixtures (T4) are reachable from the open-source core or need `platform`.
- Which `Settings` are mutable at runtime (several are `static final`); the matrix in P1 may need separate JVMs per cell.
