# A field guide to the cirrus scheduler

A whistle-stop tour of the data structures and concurrency techniques in `optimus.graph`, written for a human doing a concurrency review. It says what each piece is, which well-known system it most resembles, where the resemblance breaks down, and what to look at. It deliberately does not repeat the findings in [SCHEDULER_REVIEW.md](SCHEDULER_REVIEW.md); where a section ends with "see Rn", that is the pointer.

All paths are under `optimus/platform/projects/core/src/main/scala/optimus/graph/`.

## 1. The node: a future, a task and a graph edge in one object

`NodeTask` is the unit of work. It is at once a lazily computed future (it has a result or an exception), a schedulable task (it has `run(OGSchedulerContext)`), and a vertex in two graphs: the *waiter* graph (who wants my result) and the *waitingOn* chain (what I am blocked on). `Node[T]` and `CompletableNode[T]` in `Node.scala` add the typed result; the `@node` macros and the Loom-based async transformation in `loom/` generate subclasses whose `run` is a resumable state machine (`NodeStateMachine`).

The whole lifecycle lives in one `volatile int _state` mutated with CAS loops (`updateState`, `tryUpdateState`). The low five bits are the run state: `STARTED`, `NOT_RUNNABLE`, `DONE_FLAG`, failed, hide-exception. Higher bits are orthogonal flags: scenario-dependent, cache-invalidated, tracking, adapted by a plugin, visited-gray/black for graph walks, and so on.

**Parallels.** `java.util.concurrent.FutureTask` (state word with CAS transitions), `CompletableFuture` (result plus a lock-free completion-callback stack), Rust's `Future` plus a `Waker`. The closest is Kotlin coroutines' `DispatchedTask`/`JobSupport`: state machine, children, cancellation all in one object.

**Where it differs.** `FutureTask` has four states and one transition path. `NodeTask` has 23 flag bits and many writers: the scheduler (`tryRun`, `markAsRunnable`), the cache (`invalidateCache` clears `DONE_FLAG`), plugins (`markAsAdapted`), the dependency tracker, graph walkers (`VISITED_*`). The one rule that matters: `run()` is guarded by the CAS in `tryRun` that sets `NOT_RUNNABLE`; everything downstream assumes a task that fails that CAS was not run twice. "Done" is `DONE_FLAG | NOT_RUNNABLE`, two bits, so a writer that clears one can produce a state the predicates disagree about.

**What to check.** Every method that reads `_state` then writes it: is the write a `tryUpdateState` (re-checks done inside the loop) or a plain `updateState`? Every predicate (`isDone`, `isDoneEx`, `isDoneOrRunnable`, `isDoneOrNotRunnable`) has a slightly different mask; find the callers that rely on the differences. See R8, R6.

## 2. The waiter list: a persistent linked list swapped by CAS

`NodeTask._waiters` holds who to notify on completion. It is a tagged union: `null`, a single `NodeAwaiter`, an `NWList` (immutable singly linked list), or the sentinel `completed`. `tryAddToWaiterList` copies-and-CASes; `notifyWaiters` swaps in `completed` and walks what it got. A registration that loses to completion returns false and the caller invokes the callback itself, so every waiter is called exactly once.

**Parallels.** `CompletableFuture`'s Treiber stack of `Completion` nodes with the `null`/stack/done states; `AbstractQueuedSynchronizer`'s CLH queue; JavaScript promise reaction lists. The exactly-once-via-CAS contract is the same one as in `CompletableFuture.uniPush`.

**Where it differs.** Callbacks run synchronously on the completing thread (`onChildCompleted`), and the default callback is "mark me runnable and enqueue me on the *completer's* queue". There is a stack-depth guard (`delayOnChildCompleted`, `proxyChainStackLimit`) rather than a trampoline. `continueWithLast` appends by rebuilding the list, O(n), for the rare "notify me after everyone else" case. Waiters are usually `NodeTask`s themselves but can be `SyncFrame`s (see §5) or ad hoc `NodeAwaiter`s.

**What to check.** Who else reads `_waiters` without the CAS protocol (graph walkers, debug dumps). What `onChildCompleted` is allowed to do on an arbitrary thread with an `EvaluationQueue` that may be a scheduler rather than a context. Objects reused while still on a list. See R6, W3a–c.

## 3. The work-stealing deque: a Chase–Lev variant with at-least-once semantics

`OGSchedulerContext.WorkQueue` is a circular array with an owner-only `_top` and a volatile `_base`. The owner pushes and pops at the top; thieves CAS a slot to null at the base and advance `_base` under `synchronized (this)`. Growth copies live tasks into a doubled array and swaps. A sync frame can fence the owner off tasks below a captured `_top` (`currentLocalQueueMarker`), and `scanToFindWork` does a linear, causality-checked search for the desperate case.

**Parallels.** `ForkJoinPool.WorkQueue` (same shape, same "owner at top, thieves at base", same `& mask` indexing), .NET `ThreadPool`'s `WorkStealingQueue`, Tokio's `Local` run queue, Go's per-P run queue. The class comment cites the Chase–Lev and "idempotent work stealing" papers and explains why it departs from them.

**Where it differs.** This is explicitly *not* linearizable: a task may be present in two queues (grow copies while thieves steal), may be returned to two takers (owner's failed CAS still returns `r`), and `size()` is "unstable". The design pushes correctness down to `tryRun` (§1) rather than into the queue; Michael et al. call this idempotent work stealing, and ForkJoinPool explicitly rejects it. Thieves are serialised on the queue's monitor, which FJP avoids. Indices are compared directly rather than by difference, with a one-shot `resetBounds` at `Integer.MAX_VALUE`; FJP's indices wrap naturally because it only ever compares `top - base`.

**What to check.** Every reader of `size()`/`isEmpty()` that treats the value as a count. Every piece of state outside the queue that remembers an index. Whether the "+1 overrun" state in the class comment is still reachable. See R2, R7, and the jcstress/Lincheck tables.

## 4. The scheduler: a work pool with one big lock, one spinner, and three thread sets

`OGScheduler` keeps every graph thread's `Context` in one of `_workers` (running), `_waiters` (blocked in a sync wait), `_idlePool` (parked helper), or `_externallyBlocked` (temporarily out of the graph). These are `IndexedArrayList`s: packed arrays with swap-remove and an index stored on the item, O(1) everything, no checking, "all methods are expected to be called under a lock". Two global queues back the per-thread deques: `_qqueue` (a `WorkQueue` owned by whoever holds `_lock`) and `_queue` (a `PriorityQueue` for nodes with a non-zero priority, used by batchers).

Thread count is governed by an *ideal* (`optimus.gthread.ideal`, cores by default), a single *spinning thread* id used as a gate so that at most one new thread is being ramped up at a time, and an idle pool capped at twice the ideal. New workers are injected only by `addWorkerIfNeeded`, which under `limitThreads` counts waiters against the ideal.

**Parallels.** ForkJoinPool's `ctl` word and "signalWork" ramp-up (one activator at a time), Go's `spinning M` (at most GOMAXPROCS/2 spinning, the rest parked), Tokio's single "searching" worker gate, .NET's hill-climbing thread injection. The idle pool with `parkNanos(1 s)` is the classic "keep a few warm threads" trick.

**Where it differs.** FJP and Go are lock-free on the hot path; here `enqueue` from outside, every wake-up, thread injection, stall handling, cycle detection and the stalled callbacks all run under one `synchronized (_lock)` (23 sites). The spinner uses `Thread.yield()` in a loop rather than `onSpinWait` plus timed park. Waiting threads are first-class citizens of the pool: a thread blocked in a sync wait is still "in the graph", helps by stealing, and counts toward the ideal.

**What to check.** What runs while holding `_lock` (user callbacks? thread start?). What happens to a `Context` when its thread dies unexpectedly. Whether every transition between the four sets keeps the spinner id consistent. See F2b, I3, I4, W6d.

## 5. Sync stacks and causality: the part with no textbook

Optimus code can call `node.get` from inside a running node. That blocks the thread in `runAndWait`, which pushes a `SyncFrame`, registers the frame as a waiter on the awaited node, and then *helps*: it drains its own queue and steals from others until the awaited node is done. The frames on a thread form its *sync stack*. This is "blocking by helping", as in FJP's `helpJoin`/`awaitJoin` and `CompletableFuture.join` on a pool thread, but with an extra problem: if the thread helps by running a task that itself (transitively) needs a node that is suspended lower on this very stack, it deadlocks itself.

The defence is *causality*. Each sync frame that could matter bumps `currentCausalityID` (a depth) on the context; every task the thread runs is stamped `(threadID, depth)` into `NodeTask._causality` (two 16-bit halves of one int). `canTake(task, threadID, depth)` lets a thread at depth `d` take only tasks stamped with its own id at depth ≥ `d`, or tasks with no stamp at all; the forward-chain walk in `wait()` re-stamps the chain so the awaited work is always takeable; `poisonCausality` marks work that nobody with a sync stack may touch. The whole thing is a cheap, conservative approximation of "would running this put me below myself?"

**Parallels.** FJP's join helping (`tryHelpStealer`, `helpComplete`) only helps with tasks that are descendants of the joined task; Go's scheduler simply forbids blocking a P and hands the M off. Causality is closer to the "lock levels" trick in deadlock-avoidance schemes: a monotone number per stack that bounds what you may wait on. Loom's answer, used in the `loom-on` build, is to let the carrier go and never block the thread at all; that is why `limitThreads` is tied to `useVirtualThreads`.

**Where it differs.** Depth is not identity: two frames of the same thread at the same depth share a stamp. The stamp lives on the task and outlives the frame that wrote it. Under `limitThreads` no spinner with causality 0 is guaranteed to exist, so work that no waiter may take relies on other recovery. See R1, R9, R10.

**What to check.** Every path that stamps, every path that compares, and what each assumes about the lifetime of the other. `wait()`, `findWorkOnForwardChain`, `enqueue`'s waiter scan, `nextStepForWorkingThread`, `scanToFindWork`.

## 6. Stalls, batching and the out-of-work listeners

When the last working thread cannot find anything to run, the graph has *stalled*. That is a feature, not a failure: plugins such as the DAL batcher register `OutOfWorkListener`s and use the stall as the moment to fire a batch of deferred requests, so the engine is a natural batching system in the style of Haxl or Facebook's `DataLoader`. Listeners have priorities and are called in passes; a pass stops when a callback added work. `mustCallDelay`/`mustWaitDelay` add timers via `ScheduledNodeTask`.

**Parallels.** Haxl's "run until blocked, then fetch everything blocked on", GraphQL DataLoader's "batch at the end of the tick", Seastar's idle-time hooks.

**Where it differs.** Callbacks run under `_lock` (an acknowledged TODO), iterate a `PriorityQueue` with a reentrant lock held, and are the only thing standing between a stall and a hang. Recovery for "nobody called the callbacks" is a 1 s timer in `fullWait`.

**What to check.** Reentrancy (a callback that registers/removes), exceptions from callbacks, and whether a stall is detected when the last worker leaves abnormally. See F2, F2b, W5a–c.

## 7. Cycle detection: Floyd on the waitingOn chain, under a global lock

Every blocked thread's awaited task has a `waitingOn` chain. `findWorkOnForwardChain` walks it with a fast and a slow pointer (Floyd), re-stamping causality and returning the first runnable node. If the walk finds a cycle, `detectCyclesAndRecover` runs under the static `NodeTask._visitLock`: it collects the cycle, tries plugin-specific recovery (`tryToRecoverFromCycle`, used by the cross-scenario proxies to loosen a tweak-dependency mask), and otherwise completes one node with `CircularReferenceException`. The lock order `_lock` → `_visitLock` is documented in a comment and is what makes the recovery enqueue safe.

**Parallels.** Deadlock detection by wait-for graph (database lock managers, Go's `runtime.checkdead`), run on a timer when progress stops rather than on every wait. Completing a node with an exception to break the cycle is the same move as a database aborting the youngest transaction.

**What to check.** The lock order with more than one scheduler alive; which structures the walk reads without their own lock; the asymmetry between what is detected (waitingOn cycles) and what can actually deadlock (sync-stack inversions that form no cycle). See R3, R4, R9.

## 8. Cancellation: scopes, not interrupts

`CancellationScope` is a tree; cancelling a scope marks it and every child. The scheduler checks `cs.isCancelled()` in `run()` before running a node and completes it with the cancellation cause instead. Nodes already running are not interrupted; `waitForCancelledNodesToStop` polls the contexts' current nodes and sync frames until none of them is in the scope, with a check for the self-deadlock of waiting on a cancelled node that is on your own sync stack.

**Parallels.** Go's `context.Context` tree, Kotlin's structured concurrency `Job` hierarchy, .NET `CancellationToken`. The "cooperative, checked at scheduling points" model is the standard one.

**What to check.** The polling loop reads plain fields; cancellation racing completion (both complete the node); cancellation while a node is on a sync stack. See R5, H6.

## 9. The cache and proxies (only as far as the scheduler cares)

`cache/` holds node caches keyed by property and scenario stack, with proxies (`DelayedXSProxyNode`, `DelayedXSFTProxyNode`) that stand in for a possible cache hit while the "real" node runs elsewhere and complete by copying its result. Proxies are `NodeTask`s and therefore participate in waiter lists, forward chains and causality; cycle recovery exists mainly for them, and `delayOnChildCompleted` exists because proxy chains can be long. `invalidateCache` clears a node's `DONE_FLAG` from the dependency tracker's side.

**Parallels.** Memoising futures (Guava `LoadingCache` with `Future` values), Salsa and Adapton for the incremental-recomputation side (tweaks, dependency tracking, invalidation).

**What to check.** Any state transition made to a node by the cache or tracker rather than by the scheduler, and which scheduler predicates it changes. See R6.

## 10. Diagnostics, profiling and the agent

A lot of the code is observability: `OGTrace`/`OGLocalTables` per-thread profiling tables acquired and released on graph entry/exit, `PNodeTask` profile records, breadcrumbs, stall printing with rate limits, `GraphDiagnostics` dumps, `InfoDumper.graphPanic` as the kill switch. The entity agent (`entityagent/`) rewrites classes at load time: it adds fields to `TPDMask`, instruments `NodeTask`, and in the Loom build rewrites async code to continuations. The class you read is not quite the class that runs.

**Parallels.** JFR-style per-thread buffers, async-profiler integration, the "crash the process on invariant violation" discipline of Erlang supervisors or SQLite's `SQLITE_CORRUPT`.

**What to check.** Whether diagnostics can throw on the kill path, and what the recursion guards do then. Whether a probe or test is running the instrumented class. See F1, F1b, F6.

## 11. Settings and build-time switches

`Settings` and `DiagnosticSettings` are `static final` fields read from system properties once. `optimus.scheduler.asserts` turns on the scheduler's own invariant checks (there are no Java `assert`s). `optimus.graph.useVirtualThreads` defaults to a constant that depends on which entityagent source set was compiled (`loom-on`/`loom-off`), and `limitThreads` defaults to its negation. `gthread.ideal` of -1 means "all cores, at least 4".

**What to check.** Which configuration you are actually in before interpreting a run. See F5, F7.

## A reviewer's checklist, by technique

| Technique | Where | Textbook reference | The local twist |
|-----------|-------|--------------------|-----------------|
| State word + CAS loop | `NodeTask._state` | `FutureTask`, AQS | many writers, two-bit "done" |
| Lock-free callback list | `NodeTask._waiters` | `CompletableFuture` | synchronous callbacks, reusable frames |
| Chase–Lev deque | `WorkQueue` | FJP, Tokio | at-least-once, direct index compares, sync markers |
| Single-spinner ramp-up | `_spinningThreadID` | FJP `signalWork`, Go spinning M | `Thread.yield`, under `_lock` |
| Packed thread sets | `IndexedArrayList` | FJP's `queues[]` | swap-remove with unlocked readers |
| Blocking by helping | `runAndWait` | FJP `awaitJoin` | sync stacks, causality stamps |
| Causality / lock levels | `_causality`, `canTake` | lock-level deadlock avoidance | depth not identity, 16-bit packing |
| Stall-driven batching | `OutOfWorkListener` | Haxl, DataLoader | callbacks under the big lock |
| Wait-for cycle detection | `detectCyclesAndRecover` | DB deadlock detectors | static lock, plugin recovery |
| Scoped cancellation | `CancellationScope` | Go context, Kotlin Job | polling stop-wait on plain fields |
| Fail-fast kill | `logAndDie`, `panic` | Erlang let-it-crash | kill path can throw |

The campaign in [TEST_CAMPAIGN.md](TEST_CAMPAIGN.md) attacks the rows of this table one at a time; the probe harness in `probes/` is the practical way to answer "is this reachable?" for any new concern you form while reading.
