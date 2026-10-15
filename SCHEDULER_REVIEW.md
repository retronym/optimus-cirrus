# Cirrus core scheduler review

Scope: `optimus/platform/projects/core/src/main/scala/optimus/graph/{OGScheduler.java, OGSchedulerContext.java, NodeTask.java, NodeAwaiter.java, Scheduler.scala, Node.scala, OGLocalTables.java, Settings.java}` plus `optimus/core/IndexedArrayList.java` and the cycle-recovery entry points in `cache/NCProxies.java`. Read, not run (see the build section at the end for what could be executed locally). Line numbers refer to the current worktree.

Severity key: **H** likely to bite in production, **M** real but needs specific conditions, **L** hygiene / latent.

## Races and correctness

### R1 (M) Causality packs the thread id into 16 bits and overflows

`NodeTask.setCausality` (NodeTask.java:198) stores `(causality & 0xFFFF) | (threadID << 16)` and `causalityThreadID()` (line 194) recovers it with an arithmetic `>> 16`. `threadID` comes from `OGScheduler.threadCounter`, which is bumped for every injected worker **and** every `newEvaluationContext` call (OGScheduler.java:490), i.e. every thread that ever initialises an `EvaluationContext`. Idle worker contexts are reused, but user threads are not: a server whose request pool churns threads, or a long test run, will pass 32,767 ids.

- From id 32,768 the sign bit is set, `>> 16` sign-extends, and `taskTID == threadID` in `canTake` (OGScheduler.java:1096) is never true for that thread's own sync-stack work. With `limitThreads` the work can only be taken by a causality-0 thief, so a graph where every thread is on a sync stack stalls until the 1 s deadlock timer walks the forward chain.
- From id 65,536 the shift wraps completely and ids alias. `canTake` then lets thread N take work stamped by thread N+65536 under a sync stack, which is exactly the sync-stack deadlock the causality check exists to prevent.
- `poisonCausality` (line 205) uses `Integer.MAX_VALUE << 16`, which extracts as thread id -1. Works by accident.

Fix: widen `_causality` to a `long` (the field is transient and already padded by the volatile `_state` neighbour), or mask with `>>> 16` and assert/saturate at 0xFFFF in `injectThread`/`newEvaluationContext`.

### R2 (M) `WorkQueue` index reset breaks sync-frame markers

`OGSchedulerContext.WorkQueue` compares `_top` and `_base` directly (`_top <= _base` at line 372, `top < _base` at 413 and 426, `(top - base) >= mask` at 442) and relies on `resetBounds` (455) firing exactly when `_top == Integer.MAX_VALUE` (452). The reset itself is sound (probe W1a) and tasks are not lost across it (W1s). The problem is state held outside the queue: a sync frame that captured `currentLocalQueueMarker = localQ.getTop()` before the reset (OGSchedulerContext.java:744) now holds a marker larger than any future `_top`, so `localPop(queueMarker)` returns null for the rest of that frame (demonstrated by W1b). The thread cannot drain its own local queue and depends on thieves or the deadlock timer. The growArray comment ("we cannot move _top because of causality mark") shows this was known for the resize case but not the reset case.

A second case I reported in the first draft, `_base` passing `MAX_VALUE` via the "+1 overrun" state described in the class comment, is withdrawn: probe W1a' could not produce `base > top` in 20,000 pop-vs-steal races. The comment describes an older `localPop` that did not CAS; the current one does, so the overrun state is unreachable and the reset guard is always hit.

At 100 k–1 M pushes per second per thread, 2^31 is hours to a day. Fix as in `ForkJoinPool.WorkQueue`: compare differences only (`top - base <= 0`), make the sync-frame marker a difference too, and delete `resetBounds`. Direct comparisons of the raw indices remain a hygiene issue even though no path currently wraps them.

### R3 (L) `_evalAsyncTrackedNodes` is iterated without its monitor

Adds/removes synchronise on the list itself (any thread, including non-graph threads via `enqueueAndTrackCompletion`, OGScheduler.java:528), but `findWork` iterates it with `foreach` under `_lock` only (line 1181) and `generateSchedulerStatus` iterates with no lock (1769). `IndexedArrayList.tryRemove` moves the last element into the hole and `add` replaces the array, so a concurrent iteration can skip or double-visit. Benign today because `detectCycles` is idempotent, but a one-line `synchronized` fixes it.

### R4 (L) Lock order inversion across schedulers in cycle recovery

`findWorkOnForwardChain` takes `_lock` then the process-wide static `NodeTask._visitLock` (OGScheduler.java:1293, comment [SEE_XSFT_GLOBAL_SCHEDULER_ENQUEUE]). Under `_visitLock`, `tryToRecoverFromCycle` enqueues (`NCProxies.java:223, 489`); with `keepTaskSchedulerAffinity`, auxiliary schedulers, or the fresh `OGScheduler` that `Scheduler.default` creates per call when the default is single-threaded (Scheduler.scala:111), that enqueue can take a *different* scheduler's `_lock`. Two schedulers detecting cycles at once gives S1._lock → _visitLock → S2._lock against S2._lock → _visitLock → S1._lock. The comment's reasoning only covers one scheduler. Rare, but it is a genuine deadlock with no timeout.

### R5 (L) `waitForCancelledNodeToStop` spins on plain fields

OGScheduler.java:1680–1702 loops on `frame.getCurrentNodeTask()` (plain read of `SyncFrame.ntsk`) and `owner.getCurrentNodeTask()` (plain read of `_currentNode`, whose writes in `OGSchedulerContext.run` at lines 835 and 872 are plain stores; only `runAndWait` uses the opaque VarHandle). Works because `Thread.yield()` is opaque to C2, not because the code is correct. Each iteration also does `visitForwardChain().containsEQ(callerNode)`, O(chain), and the spin is unbounded while the cancelled node is mid-computation. Use `getOpaque`/`setOpaque` consistently and back off to a timed park.

### R6 (L, mechanism demonstrated) `SyncFrame` objects are reused while possibly still registered as waiters

`runAndWait` registers the pooled `SyncFrame` via `tryAddToWaiterList(frame)` (OGSchedulerContext.java:755) and exits the wait loop on `isDoneEx()`, which includes `STATE_INVALID_CACHE`. `invalidateCache` (NodeTask.java:1140) sets that flag without notifying waiters. If it can hit an in-flight node (the tracking callers in `UserNodeTracker`/`TweakableTracker` look like they can), the frame is popped and reused for the next sync wait while still on the old node's waiter list; the old node's eventual `notifyWaiters` then calls `setWaitingOn(null)` and `onChildCompleted` on a frame that now describes a different wait. `awakeOnComplete` compares `ctx.awaitedTask == child`, so no spurious wake, and `result` throws after the exit, so this is a broken invariant rather than an observed bug. Probes W3e and W4a demonstrate both halves of the mechanism (stale waiter survives `invalidateCache`; popped frame is reused with `waitingOn` still set). Whether an in-flight node is ever invalidated in practice is still unconfirmed; worth a `schedulerAsserts` check that a popped frame has `_waitingOn == null`.

## Foot-guns

### F1 Several ordinary mistakes kill the process

- Completing a node twice goes to `logAndDie` and exits with 121 (NodeTask.java:1274). `complete`/`completeWithResult` take no lock; only the `try*` variants synchronise, so "try" is safe only against other "try" calls.
- `OGSchedulerContext.run` turns any `Throwable` from `ntsk.run` into `completeWithException` (line 869). A node that completes itself and *then* throws (including `StackOverflowError`/OOM after completion) therefore double-completes and exits the JVM.
- Anything escaping `OGScheduler.wait` panics with 66 (line 929). That includes exceptions from `tryToRecoverFromCycle`, `BlockingWaitCounter.report` and plugin `onGraphStalled` callbacks.

These are deliberate ("we are probably heading for a bad deadlock"), but the surface is wide. At minimum the `run` catch should check `ntsk.isDone()` first and log instead of re-completing.

### F2 Mutating the callback set from `checkForNewWork` throws CME → panic

`runOnStalledCallbacks` iterates `_onStalledCallbacks` with for-each while calling `checkForNewWork` (OGScheduler.java:1475). `_lock` is reentrant, so a callback that adds or removes a listener from inside `checkForNewWork` gets `ConcurrentModificationException` from the `PriorityQueue` iterator. Only `onGraphStalled` has the pending-list protection. The exception propagates out of `findWork` into `wait` and panics.

### F3 Unbounded thread injection

`evaluateOnGraphThreadAndTrackCompletion` always calls `injectThread(nodeTask)` (line 603): N concurrent calls are N platform threads regardless of ideal count. Likewise `shutdown()` only parks the idle pool; `enqueue` after shutdown still spawns threads. And `Scheduler.default` allocates a new `OGScheduler` (registered in the weak map, consuming a scheduler id) on every call when the default is single-threaded (Scheduler.scala:111).

### F4 (cosmetic) `asIfRunning` uses thread id 1

OGScheduler.java:496 creates a `Context` with `threadID = 1`, which `threadCounter` also hands to the first real worker. Probe W6b shows nothing is ever stamped with it: `asIfRunning` only hosts enqueues, never runs nodes, so tasks leave with causality 0/0. Harmless today; a fresh id would make it robust against a future caller that does run something there.

### F5 Behaviour depends on a build-time flag

`useVirtualThreads` defaults to `LoomDefaults.enabled`, which is `true` in `entityagent/src/main/loom-on` and `false` in `loom-off`. `limitThreads = !useVirtualThreads` follows. Which source set a given jar was built from is invisible at runtime; `addWorkerIfNeeded`, `findWork`, `nextStepForWorkingThread` and the idle pool all branch on it. Log the effective values at scheduler start.

### F6 The scheduler cannot be instantiated without the entity agent

`OGScheduler`'s static initialiser calls `AgentInfo.agentInfo()` and `EntityAgent.retransform(ReentrantLock.class)` (lines 82–87). Every unit test of the scheduler needs `-javaagent`. See the build section.

### F7 Invariants are only checked under `optimus.scheduler.asserts`

Default off. With it off, `markAsRunnableAndEnqueue` on a node that still has `waitingOn` only logs (line 517), `IndexedArrayList.remove` silently no-ops on a missing item, and `wakeupThread` ignores a failed removal. There is no `src/test` for `core` in this export, so nothing here runs the asserting configuration.

### F8 Minor

- `OGSchedulerContext.hasEnoughWork()` NPEs off-graph (line 1027).
- `_queue` (priority queue) is only purged of done tasks when `_workers.count == 0` (line 715); between stalls it retains completed nodes.
- `Context.lastTask()` walks an unbounded forward chain (line 117); it is called from the sampling profiler's stack recorder.

## Inefficiencies

### I1 Every blocking wait pretty-prints the node and captures a stack trace

`fullWait` builds `awaitTask.toString()` (a `PrettyStringBuilder` render) and `new Exception("Blocking Wait")` unconditionally (OGScheduler.java:948–949) before anyone checks whether the blocking-wait counter has a consumer; `BlockingWaitCounter.report` just forwards to `OGTrace.observer.writeEvent`. Probe W7 measured 0.25 µs for both on a trivial node with a shallow stack against a 29 µs median wait, so this is minor unless nodes pretty-print expensively (entity keys, args) or the stack is deep. Gate on the observer, or pass a supplier; cheap and strictly better.

### I2 The spinner burns a core with `Thread.yield()`

`spin` (line 864) yields in a tight loop whenever there are ≥2 workers and no work. One long-running node plus the spinner is a wasted core for the node's duration. `ForkJoinPool` spins briefly with `Thread.onSpinWait()` then parks with a timed wakeup; the same shape would keep the latency benefit without the steady-state cost.

### I3 Thread start and unpark under the global lock

`injectThread` calls `new OGThread(...).start()` (line 673) inside `synchronized (_lock)`, reached from every `enqueue`. Probe W6e measured a median 28 µs for an `enqueue` that creates a thread vs 12.5 µs for one that unparks an idle one, on macOS; the idle pool makes creation rare in steady state. Reserve the `Context` under the lock and start the thread after releasing it; same for `LockSupport.unpark` in `tryWakeupThread`. Low priority.

### I4 Linear scans under `_lock`

- `enqueue` scans all waiters for a `canTake` match on every global enqueue (line 581).
- `findWorkFromLowPriorityQueue` is O(n) with iterator removal (line 1204).
- `addOutOfWorkListenerImpl` does `PriorityQueue.contains` (line 1565) and `removeOutOfWorkListener` does `PriorityQueue.remove`; both O(n), so registering n batchers is O(n²).

A side identity set for the callbacks and a per-causality index for waiters would remove the quadratic cases.

### I5 Timer-driven recovery wakes every waiter every second

With `schedulerRecoveryInWait` (default true) each blocked thread wakes every second (line 997), and once per second runs `findWorkOnForwardChain`, O(chain). Idle pool threads also wake every second (line 440). Hundreds of waiters means hundreds of forward-chain walks per second as background noise. One scheduled task per scheduler would do the same job.

### I6 CAS on every owner pop

`localPop` (line 410) CASes each pop; the TODO (OPTIMUS-28068) already notes this. Because `tryRun` is itself a CAS, the owner can use the FJP owner-pop protocol (one fence, no CAS) and tolerate the occasional double-dispatch that `tryRun` rejects. `tryToSteal` also serialises all thieves of a queue on `synchronized (this)` (line 390); CAS on `_base` is enough once slots are CASed to null.

## Missing wins

- Wrap-safe deque (R2) plus lock-free steal (I6) in one change, modelled on `ForkJoinPool.WorkQueue`, with the ABA concern discharged by the existing null-CAS on slots and the `tryRun` guard.
- A long `_causality` (R1) and `threadCounter` as the only source of ids (F4).
- `Thread.onSpinWait()` with a spin budget (I2) and plain `park()` for idle threads instead of `parkNanos(1 s)`.
- Opaque VarHandle access for `_currentNode` and `SyncFrame.ntsk` everywhere, not just in `runAndWait` (R5).
- A stress harness: fan-out/fan-in trees, deliberate sync stacks, cancellation mid-run, two schedulers, run under `-Doptimus.scheduler.asserts=true`. None of the findings above has a reproducer because this export ships no scheduler tests.

## Build and run

The full `core` module and its 15 sibling modules compile locally with plain `scalac`/`javac`; no OBT, no entityplugin (core only needs the stagingplugin). Scripts, stubs and logs are under the session scratchpad `build/` directory (`fetch.sh`, `build.sh`, `build-smoke.sh`, `run-smoke.sh`). None of it touches the repo.

What it took:

- JDK 21 (Temurin). JDK 24+ fails in agent premain because `java24_compat.sc` is not in the export.
- Scala 2.13.17+: the alarms plugin reflects on `PerRunReporting.nowarnAction`, which first exists there. MS builds with a patched scalac (unsealed `Profiler`, assignable `Run.profiler`); two alarms sources were overridden to drop the slow-compilation profiler.
- `-Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq` for every module from `utils` up. Not documented anywhere; inferred from missing implicits.
- Logback pinned to 1.2.13 (`CachedNamedConverter` reaches into a package-private field).
- 22 stub classes for internal libraries (`msjava.*`, `com.ms.*`, the MS async-profiler fork, five `optimus.debug.Instrumented*` classes that live in the missing entityagent-ext jar). Three small source overrides (breadcrumbs `Properties`, instrumentation `DefensiveManagementFactory`, the alarms profiler).
- Runtime: `-javaagent:entityagent.jar` is mandatory (`OGScheduler.<clinit>` fails fast without it, and the agent rewrites `NodeTask` at load time, so the reviewed source is not exactly what runs); `--add-exports` for `jdk.internal.vm`, `jdk.internal.misc`, `sun.management`; a `ServiceLoader` provider for `BreadcrumbsSetupFactory`; `-Doptimus.graph.async.profiler=false`.

Smoke test (`smoke/Smoke.scala`): a hand-written `CompletableNode` fan-out/fan-in tree with async, sync-stack and mixed modes, N client threads, optional failing leaves, hang watchdog. All configurations passed with no hangs or wrong sums: default 18 threads 4.4 M nodes in 0.25 s; `optimus.scheduler.asserts=true`; `failEvery=7`; 16 clients on `gthread.ideal=2`; depth 8 (21 M nodes); `gthread.ideal=1` with 12 clients; `ideal=3` with failures. Loom mode runs but is not meaningful: `vt_helpers.sc` is missing from the export so the virtual-thread instrumentation is not applied, and `-ea` aborts in premain.

Build-related foot-guns found on the way: `-ea` does nothing in the scheduler core (no Java `assert`s; everything is behind `optimus.scheduler.asserts`, read once into a `static final`); "Cannot patch core classes" and "Native Method Prefix not supported" are printed because the ext jar is absent, so `System.exit` and exception-constructor hooks are inactive; `gthread.ideal=-1` resolves to all cores with a floor of 4.

## Tier 0 probe results

Probes live in the scratchpad `probes/` directory (`run.sh` compiles and runs them against the build above; `Steps.java` is the schedule-driven step harness, `Probes.java` the cases). See TEST_CAMPAIGN.md for the design.

| Probe | Hazard | Outcome | What was observed |
|-------|--------|---------|-------------------|
| W1a | R2 reset at `MAX_VALUE` | not reproduced | Queue primed at `top = base = MAX-3`; three pushes trigger `resetBounds`, giving `top=63 base=60`; a steal and a fourth push then pop back every task; final state empty and consistent. The reset itself is sound. |
| W1a' | R2 "base overrun" | not reproduced | 20,000 single-element pop-vs-steal races never produced `base > top`. The class comment's "+1 overrun" state comes from an older `localPop` that did not CAS; the current one does. R2 case 1 withdrawn (see amended R2 above). |
| W1b | R2 marker after reset | **demonstrated** | Marker captured at `top = MAX-1`, one push fires the reset, five more pushes land; `localPop(marker)` returns null instead of the newest task. The owner cannot drain its own queue for the rest of that sync frame. |
| W1f | F8 retention | **demonstrated** | `[Done, Done, Live, Done]` after `localCleanup(null)` is `[Done, Done, Live]`: cleanup stops at the first live task and keeps the done ones beneath it. |
| W1s | R2 loss across wrap | not reproduced | 200 rounds × 2,000 pushes starting 50 below `MAX_VALUE`, owner popping every fourth, three thieves: every task observed at least once, queue empty at the end. |
| W3d | R1 id packing | **demonstrated** | `setCausality(1, 32768)` reads back as -32768; 65536 reads back as 0, 65537 as 1. `canTake(task stamped by thread 65537, thread 1, causality 1)` is `true` (aliasing lets thread 1 take another thread's sync-stack work) and `canTake(task stamped by 32768, thread 32768, causality 1)` is `false` (owner cannot take its own). `poisonCausality` extracts as thread -1. |
| W2a | R3 iteration vs removal | **demonstrated** | With the step harness forcing `tryRemove(a)` between the iterator's first and second element, `foreach` visits `[a, b]` and never visits the live item `c`, which `tryRemove` moved into the slot already passed. |
| W5a | F2 CME in callbacks | **demonstrated** | Three self-removing listeners; `runOnStalledCallbacks` throws `ConcurrentModificationException` from the `checkForNewWork` loop. Two listeners do not trigger it because the iterator never calls `next()` after the modification, so the window is size-dependent. |
| W1g | growArray with sync marker | not reproduced | With `currentCausalityID = 1`, `growArray` keeps `nullTask` placeholders and a marker taken before the grow still fences exactly the post-marker tasks. With causality 0 the dones are dropped (positions shift) but no sync frame exists then, so no marker is live. Works as designed. |
| W2b | R3 iteration vs grow | **demonstrated** | `foreach` holds the pre-grow array: 4 of 8 items visited after a concurrent `add` doubled the array. Benign for cycle detection (new tracked nodes are checked next stall), misleading for the status dump. |
| W2c | `tryStealing` count race | not reproduced | 4.8 M `tryStealing` calls against a thread adding/popping `Contexts` entries: no exception. The `Math.min(count, length)` guard and null check hold. |
| W3a | waiter add vs complete | not reproduced | 2,000 rounds × 4 racing `continueWith` against `complete`: every waiter called exactly once. The `_waiters` CAS protocol is sound as far as a race without delay injection can show. |
| W3b | `continueWithLast` ordering | not reproduced | `[second, first, last, last2]`: LIFO for `continueWith`, FIFO at the tail for `continueWithLast`, as documented. |
| W3c | F1 double completion | **demonstrated, differently** | Two racing `complete()` calls were detected ("Cannot complete node twice") but the child JVM exited 0. `InfoDumper.<clinit>` threw here (it reads an internal `utils.conf`), the exception escaped `logAndDie`, `complete()`'s `catch (Throwable)` re-entered `logAndDie`, and the static `inLogAndDie` guard returned silently. The process continued with a double-completed node. See new F1b. The `tryCompleteWithException` vs `complete` variant did not race into double completion in this run. |
| W3e | R6 stale waiter after invalidate | **demonstrated** | `invalidateCache` on an in-flight node: `isDoneEx()` becomes true, the registered waiter is not called and its `waitingOn` still points at the node; a later `complete()` then calls it. |
| W3f | F7 asserts off | **demonstrated** | `markAsRunnable` on a done node is silently ignored with `schedulerAsserts=false`. |
| W3g | `tryRun` idempotence | not reproduced | 20,000 racing `tryRun` pairs: exactly one winner every time. |
| W4a | R6 frame reuse | **demonstrated** | `push(A)`, register the frame as A's waiter, `pop()`, `push(B)`: the same `SyncFrame` object comes back with `waitingOn == A`. |
| W4b | visit during pop | not reproduced | Visitor sees the popped frame's node as null and continues; the production visitor null-checks. |
| W5b | re-add during callback | not reproduced | Pending-list re-add works: one call per pass, listener still registered. |
| W5c | priority gating | not reproduced | `[0, 0, 1]` with the first adding work: both priority-0 listeners called, priority-1 deferred to the next pass. As documented. |
| W5d | I4 registration cost | **demonstrated** (mild) | 20,000 listeners: 2.2 µs per add, 1.0 µs per remove, linear in the registered count. Quadratic but small at this scale. |
| W6a | wake of a non-waiter | not reproduced | `tryWakeupThread` on a working context returns false and leaves `awaitedTask` null. |
| W6b | F4 `asIfRunning` id 1 | not reproduced | A task enqueued through `asIfRunning` carries causality 0/0: nothing runs under that context, so its id is never stamped. F4 downgraded to cosmetic. |
| W6c | F3 inject after shutdown | **demonstrated** | After `shutdown()`, `enqueue` spawned two worker threads for one task; they exit instead of parking once done. |
| W6d | spinner invariant | not reproduced | 1,200 checkpoints across enter/exit/enqueue transitions: `_spinningThreadID` always names a context in `_workers`, or is 0. |
| W6e | I3 thread start under lock | not reproduced (quantified) | `enqueue()` holding `_lock`: median 28 µs when it must create a platform thread vs 12.5 µs when it unparks an idle one, on this Mac. Real but smaller than I guessed; I3 downgraded. |
| W7 | I1 wait entry cost | quantified | Median cross-thread blocking wait 29 µs; the unconditional `toString` + `new Exception` cost 0.25 µs on this trivial node with a shallow stack. Negligible here; only matters for nodes with expensive pretty-printing or deep stacks. I1 downgraded. |

Not yet run (need delay injection inside `WorkQueue` methods, P3 in the campaign): W1c, W1d, W1e.

### F1b (M) A failed kill is swallowed, and the guard then disables every later kill

Found by W3c. `NodeTask.logAndDie` (NodeTask.java:1291) sets a static `inLogAndDie = true`, logs, dumps graph state and calls `InfoDumper.graphPanic`, which is expected to `Runtime.exit`. If anything on that path throws (here `InfoDumper.<clinit>` failing on a missing config; in production a breadcrumb or dump failure would do), the exception leaves `logAndDie`, is caught by `complete()`'s `catch (Throwable)`, which calls `logAndDie` again, hits the guard, logs "called recursively" and returns. The node is left double-completed and the JVM keeps running. `inLogAndDie` is never reset, so from then on every `logAndDie` in the process is a silent return. The guard should only short-circuit re-entrancy on the same thread, and the fallback when the kill path itself fails should be `Runtime.halt`.
