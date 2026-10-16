# Adversarial test campaign: caching and dependency tracking (Phase 2)

Targets the Phase 1 candidates in [CACHE_REVIEW.md](CACHE_REVIEW.md). Same conventions as the scheduler campaign: every probe names the invariant it attacks and what *falsified* looks like; results are recorded as demonstrated / not reproduced / quantified; a candidate the probes contradict is withdrawn or downgraded in the review, and anything a probe exposes that is not on the list gets its own id. Probe code lives under `probes/cache/` on top of the existing `probes/` harness (step harness, delay-injection agent, child-JVM driver, build recipe in `SCHEDULER_REVIEW.md`).

## Oracles

Two reference computations outside the system decide correctness; everything else is a hang or an accounting check.

- **Value oracle.** A sequential evaluator for the probe's node DAG under a given scenario stack: walk the stack from the top, resolve each tweakable the way `SSCacheID` would (instance tweak, then property tweak, last-one-wins within a scenario), evaluate without any cache. Every value the system returns must equal the oracle's for the stack the request was made under. A mismatch is a wrong-reuse finding; which tweak differs tells us which predicate lied.
- **Invalidation oracle.** For a tracker, the exact set of cached nodes that transitively read a tweakable, computed by the probe from the DAG it built. After a tweak update, every node in the set must be invalid (`isInvalidCache` or re-evaluated), and no node outside it may have been invalidated. Both directions are findings: the first is stale data, the second is over-invalidation (performance, and a signal of id or mask aliasing).

## Fixtures (P0)

Built once, shared by every tier; one Opus agent, first task.

- **Hand-written property nodes.** `PropertyNode[T]` subclasses in package `optimus.graph` with explicit `entity`, `args`, `argsHash`/`argsEquals`, a singleton `NodeTaskInfo` with chosen flags (`TWEAKABLE`, cacheable, SI, `AT_MOST_ONE_WAITER`, `LOCAL_CACHE`, `DONT_TRACK_FOR_INVALIDATION`), and a `run` that reads declared dependencies through `lookupAndGet` so tracking and recording fire. Variants: leaf tweakable, cacheable combinator over children, SI node, a deliberately bad key (mutable arg, non-symmetric `argsEquals`, `hashCode` that changes), a node that reads a plugin tag, one that reads the environment without marking it.
- **Entities.** `FakeModuleEntity` for property tweaks; a probe class in `optimus.platform.storable` to construct real entities for instance tweaks.
- **Tweaks and scenarios.** `SimpleValueTweak(key)(v)`, `new Tweak(info, TweakTemplate(v))`, by-name tweaks with a closure that reads another tweakable, `when`-predicated tweaks, `bind`/`bindOnce` modes; `Scenario(...)`, nested, ordered and unordered; `ScenarioStack.newRoot(minimal, uniqueID = true)` and `createChild`.
- **Tracker driver.** `DependencyTrackerRoot()`, consistent and concurrent children, `addTweaks`/`executeBatchUpdate`/`evaluateNodeKeys*`/`dispose`, `registerNodesInvalidated`, `TestEventCause()`, `TEST_ONLY_disposeAllRoots()`; reflection accessors for `workQueue`, `_currentCancellationScope`, `root.traversalId`, `tweakableTracker.ttracks`.
- **State priming.** Cache `maxSize` 16 (the minimum) to make eviction race lookups; `TEST_ONLY_setTweakableID` to pin bit ids and force aliasing; `TEST_ONLY_setAssumeDependsOn` for XSFT masks; `ttrackClearedNodeRefWatermark=1` for cleanup triggering.
- **Delay hooks.** Extend `DelayAgent` with points in `NodeCacheBase.putIfAbsentCore` (after head read, before CAS), `NodeCacheWithLRU.remove/catchupEntry/setMaxSize`, `TTrack.nodeCompleted/addCaller/merge`, `PtracksArray` resize, `DelayedXSFTProxyNode.runByWaitingOrEvaluating/offerValue`, `DelayedXSProxyNode.run`, `DependencyTrackerQueue.scheduleAction/evaluationBarrier`, `OGScheduler.waitForCancelledNodeToStop`. The existing `TESTABLE_FIXED_DELAY_CACHE/XS/XSFT` counters are used where they fit.

## Tier 0: whitebox, primed state, forced schedules

| Id | Target | Probe | Falsified when |
|----|--------|-------|----------------|
| W1 | D3 | `TPDMask.stringDecoded` of a 6-word mask with a bit in word 3 under width 2; compare `subsetOf`/`intersects` against a folded mask. | The decoded mask lacks the bit; a dependency on a tweak with that bit is reported as "not depends". |
| W2 | D9 | Two no-arg properties on one entity with `LOCAL_CACHE`; look up A then B; then invalidate A and look it up. | B's lookup returns A's node; A's lookup returns the invalidated node. |
| W3 | W1, R16 | jcstress: `putIfAbsent` same key from two threads; different keys same bucket; `putIfAbsent` vs `remove` under `updateLock`; count live nodes per key and check `getIfPresent` after. | A key with two live nodes that are both not proxies; a lost insert that is not explained by at-least-once. |
| W4 | S4 (known) | `setMaxSize` to a larger and a smaller table while 4 threads insert distinct keys; `verifyInvariants` after quiescence; count LRU-not-in-table and table-not-in-LRU. | Any table-not-in-LRU (a leak); quantify the other direction. |
| W5 | D13, F1 | Pure: `Scenario.equals` ordered vs unordered with duplicate targets; `Tweak.equals` for `bind` vs `bindOnce`; `SSCacheKey` sharing under each. | Shared cache id for scenarios the oracle resolves differently. |
| W6 | F7 | Pure: `nestWithFlatten` on the `(a:=1)`, `(b:=a+1 onlyReads a)`, `(b:=5)` example; evaluate b. | b = 2. |
| W7 | F2, F3 | Pure: `equivSIParams` with disjoint flag sets; `withCacheableTransitively` after `withFlag(IGNORE_SYNC_STACKS)` on a stack whose `_stableSS` was upgraded first. | Flag absent on the returned stack. |
| W8 | R13 | 16 threads call `scenarioStackWithInitialTime` on a fresh `ScenarioStackShared`; collect initial times. | More than one distinct time. |
| W9 | D7 | Three XSFT proxies on a chain; put P in WAIT on `U_old` (latch-held source at root); call `P.tryToRecoverFromCycle(maskWithK)`; release the latch. | P's value was computed at root (wrong scenario per the value oracle); `U1` never completes. |
| W10 | D8 | Child JVM with `-Doptimus.graph.tweakUsageQWords=0`; U at root completes first; P in a child that tweaks a property P reads. | P returns U's value. |
| W11 | D12 | jcstress: `PtracksArray` add-caller on the last slot vs resize; after settle, every added caller must be present with no gap. Run on an AArch64 box if available (this Mac is one). | A null gap below `nextFree`. |
| W12 | D14 | Intern an object, drop it, `System.gc()` until the weak ref clears, intern an equal object before the cleaner runs (suspend the cleaner thread). | The call does not return within 1 s. |
| W13 | D2 | Direct: two threads call `root.withFreshTraversalId` concurrently (reflection), each marking a shared TTrack graph built by hand; count tracks each traversal visited. | Both traversals get the same id and the union of visited tracks is incomplete, or a track visited by neither. |
| W14 | CleanupVisitor | Model check (Lincheck or an exhaustive small-graph enumerator): build every DAG of ≤ 6 TTracks with ≤ 3 redundant edges, run cleanup, assert reachability from every root to every live node is preserved. | A live node unreachable after cleanup. |

## Tier 1: primed end-to-end, real cache + real tracker + real scheduler

Child JVM per configuration; watchdog with scheduler and tracker dumps; the two oracles after every run.

| Id | Target | Probe | Falsified when |
|----|--------|-------|----------------|
| T1 | D1 | XS-flagged property (needs `getFavorReuseIsXS`; set the flag on a hand-written info); request under scenario A with a child cancel scope; let the owner complete; cancel the scope before the proxy's re-run (delay hook in `DelayedXSProxyNode.run`); request under scenario B that tweaks something the owner read. | B gets A's value. |
| T2 | D4 | SI node that calls a non-SI cacheable node that reads a never-tweaked tweakable, under the SI stack; then request the non-SI node from the root. | Root lookup throws `IllegalScenarioDependenceException`. |
| T3 | D5 | Impure tweak translator (throws `GraphFailedTrustException` once) with two concurrent sync requesters of the same expander. | One requester reads an incomplete node (exception from `result`, or a value the oracle rejects). |
| T4 | D6 | Four routes into the barrier hole: (a) evaluate via `tracker.scenarioStack` directly during an update; (b) `newChildScenario` from a client thread during evaluations (R4); (c) an orphan suspended on a promise that completes during the update (4b S8); (d) D5. After the update, apply the invalidation oracle. | A node the oracle says must be invalid is served from cache with the old value. |
| T5 | R1 | Tracker evaluation whose node's `argsHash` throws; then any further action on the queue with a 5 s timeout. | The second action never runs; `workQueue` non-empty with nothing in flight. |
| T6 | R2 | Many-node async evaluation from inside a node of tracker T on a different queue; end T's phase so its scope is cancelled before the counter runs; then another action on the first queue. | Queue wedged; the single-node variant passes. |
| T7 | R3 | `ttrackClearedNodeRefWatermark=1`; a low-priority evaluation whose nodes allocate many TTrackRefs; an update arrives mid-run (delay hook in `waitForCancelledNodeToStop`). | `findDeadlockedThreads` reports the updater and a graph thread, or the update never completes. |
| T8 | R5, R6 | (a) A `registerNodesInvalidated` callback that calls `addTweaks` synchronously. (b) A low-priority node that calls a queue API while the cancel path waits for it. | Permanent hang with the client parked on the latch. |
| T9 | R7 | Batch update over parent P and child C where C's tweak is by-name reading P's tweakable; vary map sizes to change iteration order; compare each tracker's resolved value with the oracle applied to the post-batch state. | A value from the pre-batch P under the post-batch C. |
| T10 | R8 | `setPluginTag` from a client thread while 8 graph threads call `findPluginTag` through `freeze`; then read the tag. | Stale tag observed after the write returned. |
| T11 | R9 | 8 graph threads call `evaluateNodeKeysAsync(removePendingInvalidate = true)` during an invalidation batch. | Exception from `pending`, or a callback lost (count callbacks vs invalidations). |
| T12 | R10 | Interrupt a thread blocked in sync `execute` while an update is queued ahead of it; then run another action. | Queue wedged. |
| T13 | R11 | Batcher plugin with `prioritiseConcurrency` and a pending batch whose waiter chain contains a WAITing XSFT proxy (`TEST_ONLY_setAssumeDependsOn(empty)`), on scheduler S1; a node with affinity to S2; S2 in `detectCycles` at the same moment (two cycles, as in the scheduler's T4). | `findDeadlockedThreads` with `_lock`/`_visitLock` in both stacks. |
| T14 | F5, F6 | (a) Dispose a tracker, then complete a cached tweakable node from its stack. (b) Cache nodes with `DONT_TRACK_FOR_INVALIDATION`, flip the flag, tweak. | (a) exit 121 (or the F1b swallow). (b) Stale value after the tweak. |
| T15 | F8 | Cacheable node reading a plugin tag; evaluate under two sibling stacks with different tag values. | Same value both times. |
| T16 | P1 | One batcher stall with an XSFT proxy on the chain; measure XSFT hit rate for that property before and after over 10 k requests. | Hit rate collapses after the stall and never recovers. |
| T17 | P2 | Concurrent child queue with a steady stream of low-priority work; time a root update's latency. | Unbounded (no completion within 10× the idle latency). |
| T18 | F9, F10 | `safeToEvaluate` without `finishEvaluating` once; then an update. Sync tracker call from a graph thread with `ideal=1`. | Update spins forever; scheduler thread blocked with no helper. |
| T19 | W3, W4 | Cancel-scope matrix: same key requested from scopes {same, child, sibling, cancelled sibling} with a running node, 1 M rounds; count proxies created and values returned; oracle on every value. | Any oracle mismatch; a value from a cancelled scope. |

## Tier 2: hunters

- **H1 Cache model checking.** Lincheck on `NodeCacheWithLRU` against a memo-table spec (key → at most one live non-proxy node; a lookup after a completed insert returns it or a usable equivalent; after `clear`, nothing from before), with `maxSize` 16 and `cacheConcurrency` 2 so eviction and ledger catch-up are frequent; jcstress for the pairs in W3 and W11. Entity agent in forked VMs as before.
- **H2 Tweak and reuse fuzzer.** Seeded random tweakable DAGs (10–500 nodes), random scenario trees (depth ≤ 4, ordered/unordered/nested, by-name tweaks with cross-references, `when` clauses, instance and property tweaks on class hierarchies with pinned bit ids to force aliasing), policies {Basic, XSFT, XS where the fixture allows, SI}, clients requesting random nodes under random stacks concurrently, under delay injection. Oracle: the value oracle on every result. Any mismatch is minimised by shrinking the scenario tree and the DAG.
- **H3 Invalidation fuzzer.** The same DAGs under `DependencyTrackerRoot` with consistent and concurrent children; random interleavings of `addTweaks`, batch updates, evaluations (sync and async, low priority), `newChildScenario`, plugin-tag changes, cancellation, dispose, under delay injection. Oracles: the invalidation oracle after every update (both directions), the value oracle on every evaluation, queue liveness (every action reaches `afterActionCompleted` within the watchdog), thread accounting from the scheduler harness, and TTrack graph sanity (every cached node reachable from its tweakables' roots; `DigraphReader` can dump it). Sweep: 50 seeds × {ideal 1, 2, 4} × {asserts on/off} × {delay on/off} × {cleanup watermark 1 / default}.
- **H4 Retention soak.** 60 minutes of H2/H3 traffic with the cache at production size; sample `getSizeIndicative`, LRU/table consistency (`verifyInvariants`), TTrack edge counts, `CacheIDs.global` size, cleared-ref counts and heap after GC every 5 minutes. Finding if any grows without bound while the live DAG is constant.

## Order of attack

1. P0 fixtures (one agent), in parallel with W1, W5, W6, W7, W12, W14 which need no fixture beyond `core` (me or a second agent).
2. Tier 0 remainder (W2–W4, W8–W11, W13) once the fixtures land.
3. Tier 1 in two agents on disjoint files: tracker side (T4–T12, T14, T17, T18) and cache/XS side (T1–T3, T13, T15, T16, T19).
4. H1 (jcstress/Lincheck agent) alongside step 3.
5. H2, then H3 (the long pole; reuse the Hunter structure), then H4.

## Bounds

XS/XSFT: T1, T13, T16, W9, W10 and the XS branch of H2 only; the `ips` protocol model (TLA+ or exhaustive step enumeration over three proxies) is listed as a gap unless W9 fails, in which case it is promoted. Retention: H4 only. Everything else is bounded by the candidate list; a probe that cannot be built in half an agent-day with the fixtures is recorded as "not attempted" with the reason.

## Exit criteria

Every D/R candidate graded H or M has a probe result; every W property has a probe that tried to falsify it; the fuzzers have run their full sweeps; the review is updated with results, withdrawals and new ids; `probes/cache/RESULTS-*.txt` hold the raw lines; everything is committed and pushed.
