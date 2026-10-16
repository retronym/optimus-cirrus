# Plan: review, adversarial testing and field guide for the caching and dependency-tracking layer

Second layer of the Optimus (cirrus) review, built on the scheduler work on branch `scheduler-review` (`SCHEDULER_REVIEW.md`, `FIELD_GUIDE.md`, `TEST_CAMPAIGN.md`, `probes/`). Same deliverables, same evidence discipline, one new lens.

## Scope

In, reviewed in full:

| Ring | Code | ~Lines |
|------|------|--------|
| 1 Cache | `graph/cache/`: `NodeCacheWithLRU`, `NodeCacheBase`, `NCEntry`, `NCSupport`, `NCProxies`, `NCPolicy`, `NCPolicyComposite`, `CacheSelector`, `NodeScopedCache`, `Interner`, `Caches`, `CacheImplementations`, `CacheGroupKey`, `CacheCounters`, `NodeCCache`, `Reachability` | 5.5 k |
| 2 Key | `PropertyNode`, `ProxyPropertyNode`, `PropertyInfo`, `NodeTaskInfo` (the flag matrix and `TPDMask`), `TweakNode`, `TweakExpander`, `TwkResolution`, `ScenarioStackShared`, `ScenarioStackMoniker`, `NodeExtendedInfo`, `SIParams`, `NodeClsID`/`NodeClsIDSupport` | 5 k |
| 3 Context | `platform/ScenarioStack`, `Scenario`, `Tweak`, `EvaluationContext`, `RuntimeEnvironment`, `TemporalContext` only where the cache keys on it | 3.4 k |
| 4 Invalidation | `graph/tracking/` and `tracking/ttracks/` in full: `DependencyTracker`, `DependencyTrackerRoot`, `DependencyTrackerQueue`, `DependencyTrackerAction`, `DependencyTrackerBatchUpdater`, `TweakableTracker`, `TTrack`, `TTrackRef`, `Ptrack`, `CleanupVisitor`, `UserNodeTracker`, `TweakContainer`, `UnderlayScenarioSupport`, `GarbageCollectionSupport`, `SnapshotSupport`, `CallbackSupport`, `EventCause` | 9.7 k |

Ring 4 is in full on the reasoning that it is added to with less care and exercised by fewer users, so it is the likelier home of latent bugs.

Bounded (time-boxed, see "Bounds"): XS and XSFT, i.e. `NCProxies`, `matchXscenario`, `RecordedTweakables`, cycle recovery. Reviewed to the depth of one agent-day and one deterministic probe set; anything deeper is listed as a gap.

Noted as gaps, parked: `RecordingScenario` and XS recording; the Stargazer hot-swap; pickling/DAL; `@entity` identity beyond what `PropertyNode.entity` hashes; the temporal-surface and bitemporal keys beyond their effect on `cacheID`.

Out: the entity plugin. We reason as if its rewrites were written by hand: `f$queued`/`f$newNode` returning a `PropertyNode[T]` subclass with `propertyInfo`, `entity`, `args`, `argsHash`/`argsEquals`, a `NodeTaskInfo` with flags, and `lookupAndGet`/`lookupAndEnqueue` doing the lookup. The review covers the contract that surface must satisfy; the probes hand-write `PropertyNode`s the way the scheduler probes hand-wrote `NodeTask`s. Memory and retention are treated as a foot-gun category to reason about, with one measurement probe, not a line of investigation.

## Lenses

Five, applied to every ring, each finding or property tagged with one:

1. **Races.** Lookup vs insert vs eviction in a CAS-only cache that never removes; adopting a cached node that is mid-completion; proxies completing from a source in another scenario; `invalidateCache` racing lookup or completion (R6/R8 territory from the scheduler review); scenario-stack child creation and its per-stack caches; `Interner`; the tracker queue, batch updater and TTrack graph under concurrent tweak changes and evaluations.
2. **Data loss and wrong results.** A hit that should have been a miss: `isUsableWRTCancelScopeAndEnv`, `matchXscenario`, `TPDMask` subset logic, SI vs scenario-dependent, cancel scope, runtime environment, temporal context. A lost, late or partial invalidation (the dangerous one: a stale value served forever). A key interned to the wrong instance. A tweak resolved against the wrong stack. The benign dual, duplicate computation, is recorded as a performance item.
3. **Foot-guns.** The `NodeTaskInfo` flag matrix and which combinations are checked; policies that change caching silently; `argsHash`/`argsEquals` contracts (mutable args, asymmetric equality, identity vs value); cacheable nodes that read scenario-dependent state without declaring it; `cacheID` collisions; the number of distinct "usable" predicates; tracker API misuse (updates outside the tracker, nested trackers, disposal); retention (caches that never shrink, `WeakReference` churn, TTrack graphs that only a cleanup visitor prunes).
4. **Performance gotchas.** The ledger/LRU locking and the force-lock path; `cacheConcurrency` defaults; hash quality and bucket distribution; proxy chains and `delayOnChildCompleted`; XSFT recovery cost; `Interner` contention; tracker batch sizes and queue fairness; TTrack cleanup cost and frequency.
5. **Properties that hold.** What the layer does well, stated with the same precision as a finding: mechanism, design choice, and the probe or model check that tried to falsify it. Candidates to confirm or refute: the CAS-only no-remove design, ledger-batched LRU, hash stored on the node, `TPDMask`-based reuse, centralised usability predicates in `NCSupport`, immutable scenario stacks with structural sharing, the tracker's single-writer queue. Each finding whose fix could damage one of these gets a "do not break" note.

## Phase 1: review (reading)

Me, with Opus agents as readers on the larger rings. Split: ring 1 + 2 (me), ring 3 (one agent, read-only, returns a structured list of invariants and suspects), ring 4 (two agents: tracker/queue/batch-updater, and ttracks/TweakableTracker/cleanup), XS/XSFT (one agent, time-boxed). Each reader returns: the invariants the code relies on, every place that reads or writes shared state and under which lock, the predicates that decide reuse, and suspects graded H/M/L with file:line. I consolidate, cross-check the suspects against the code myself, and write `CACHE_REVIEW.md` with the same structure as the scheduler review plus a "Properties that hold" section and a "Gaps" section.

Reading order inside rings: start from the two entry points, `PropertyNode.lookupAndGet` → `NCSupport.lookup*` → `NodeCacheWithLRU.putIfAbsent` (ring 1/2) and `DependencyTracker` tweak update → `TweakableTracker.invalidate` → `NodeTask.invalidateCache` (ring 4), and follow every shared-state access outward.

## Phase 2: campaign plan

`CACHE_CAMPAIGN.md`, same three tiers, written after Phase 1 so the probes target real suspects. Expected shape:

- **Tier 0, whitebox.** `NodeCacheWithLRU` with primed ledgers and forced schedules (insert vs insert same key, insert vs evict, lookup during LRU rebuild, the force-lock path); `NCEntry` lifecycle; `Interner`; `TPDMask` algebra as pure unit tests; `matchXscenario` with hand-built scenario stacks; `TTrack` graph operations with the step harness; `DependencyTrackerQueue` ordering. Delay injection via the existing ASM agent pattern, with hooks in `NodeCacheWithLRU` and `TTrack`.
- **Tier 1, primed end-to-end.** Real scheduler plus real cache, hand-written `PropertyNode`s: cache size forced to tiny to make eviction race lookups; a node cached in one scenario and requested from a child/sibling/XS scenario with and without matching tweaks; invalidation racing evaluation at every node lifecycle state (the H6 mode generalised to tweaks); cancellation of a scope whose nodes are cached; two trackers sharing cached nodes; a deliberately bad key (mutable arg, bad hash) to see what the cache does.
- **Tier 2, hunters.** jcstress on the cache's lock-free pairs; Lincheck on `NodeCacheWithLRU` against a memo-table spec (key → at most one live node; a lookup after an insert sees it or a usable equivalent); a fuzzer that generates tweak trees and node DAGs with declared dependencies and checks every value against a sequential evaluator of the same tweaks (the correctness oracle for stale hits), applies random tweak updates through a `DependencyTracker` and checks that every affected cached node is invalidated and no unaffected one is (the invalidation oracle, both directions), under delay injection; a soak for retention.

Oracles are the heart of it: a sequential reference evaluator for values, and an exact dependency set computed outside the system for invalidation. Anything the system gets wrong relative to those is a finding; anything it gets right under delay injection goes to lens 5.

## Phase 3: field guide

`CACHE_FIELD_GUIDE.md`, same shape as the scheduler one: per mechanism, what it is, the nearest known system, where cirrus differs, distilled commented code, a mermaid state or sequence diagram, what to check, and now also what it does better than the reference. Parallels to use: Salsa and Adapton (incremental recomputation, dependency tracking, "durability" of inputs), Caffeine and ConcurrentHashMap (the ledger LRU vs W-TinyLFU; CAS-only vs CHMv8's remove), Haskell's thunk memoisation and GHC's blackholing (adopting an in-flight node), React/MobX-style observable graphs (TTracks), Bazel/Skyframe (keys, dirtying, restart), Jane Street's Incremental (height-ordered recomputation), database MVCC (scenarios as snapshots), and the Clojure STM for ref-tweaks if it fits.

## Agents

Opus agents for every coding and most reading task; me for consolidation, judgement, verification and the documents. Rules carried over from the scheduler work: disjoint files per concurrent agent, every result rerun by me before it is recorded, briefs that state the goal, what is already known and what done looks like. Expected count: 4 readers in Phase 1, 3–4 builders in Phase 2 (whitebox + delay hooks, Tier 1, jcstress/Lincheck, fuzzer), 1 for the soak.

## Bounds

- XS/XSFT: one agent-day of reading, one deterministic probe set, then stop and list gaps.
- Retention: one measurement probe (cache size and TTrack graph size over a soak), reasoning only otherwise.
- Phase 1 total: about two agent-days of reading plus my consolidation. Phase 2: the fuzzer with both oracles is the long pole; everything else is bounded by the suspect list.
- The whole layer in roughly the effort of the scheduler layer, 1.5× at most; if Phase 1 shows ring 4 is bigger than that allows, the tracker's batch-updater internals move to the gap list rather than stretching the budget.

## Deliverables

`CACHE_REVIEW.md` (findings, properties that hold, gaps, results, method), `CACHE_CAMPAIGN.md`, `CACHE_FIELD_GUIDE.md`, `probes/cache/` with the harness and results, an updated `PROMPT.md` section for replay, all on a branch off `scheduler-review` so the existing probes and build recipe are reused, committed per phase and pushed to the fork.

## Open before starting

- Whether `tracking/` can be exercised without `platform` (the DependencyTracker is used from UI/reactive code; the probes need enough of its API surface reachable from `core`). First task of the ring-4 reader is to answer this.
- Whether the local build recipe still covers everything the cache probes load (TemporalContext, DAL stubs); the build agent's stubs were sized for the scheduler.
