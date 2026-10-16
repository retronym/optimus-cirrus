# Ring 4b: TTrack dependency graph, TweakableTracker, cleanup

Reader notes for Phase 1 of the cache-layer review. Paths are relative to `optimus/platform/projects/core/src/main/scala/optimus/`. `tt/` = `graph/tracking/ttracks/`, `trk/` = `graph/tracking/`. Line numbers are the current worktree.

## Headline

The TTrack graph has essentially no synchronisation of its own against invalidation or cleanup. Graph threads write to it with CAS only (`TTrack.nodes`, `TTrack.ptracks`, `PtracksArray` slots). Invalidation and cleanup then rewrite it with plain stores. The one comment that describes a lock discipline is stale. Correctness rests entirely on one external invariant: **no node that writes to a tracker's TTrack graph is running while that tracker (or an ancestor) runs an update.** `DependencyTrackerQueue` provides that invariant. It makes updates exclusive with evaluations, and `evaluationBarrier` (trk/DependencyTrackerQueue.scala:412) cancels the evaluation cancel scope and waits for running cancelled nodes to stop.

Under that invariant the design is sound and simple: single-writer for invalidation and cleanup, lock-free append for evaluation. When the invariant is broken, the failure mode is the worst one, a **silent lost invalidation**: the node completes onto a detached or `Invalid` TTrack and stays cached with a stale value forever (S1). Nothing in the ring detects it.

New concrete findings:

- **S2 (M):** a deadlock between the cleared-reference watermark (taken from inside `NodeTask.complete`) and the low-priority-action cancel path, which waits for running nodes while holding `queueLock`.
- **S3 (M):** `TweakExpander` invalidates its own running node, which makes the R6 mechanism live in production.
- **S4 (M):** the root-wide traversal-id counter is used from concurrent child-tracker updates, against its documented contract.
- **S6 (M):** toggling `trackForInvalidation` at runtime leaves already-cached nodes invisible to invalidation.
- **S7 (M):** after disposal, evaluating in a disposed tracker's scenario stack kills the process through an NPE inside `complete()`.

## 1. The model

### Objects

- **`TTrack`** (tt/TTrack.java:70). It is a shadow-graph vertex, not a per-key record. One `TTrack` stands for "a set of completed nodes that share the same set of tweakable dependencies". It extends `NodeExtendedInfo`, so it lives in `NodeTask._xinfo` (graph/NodeTask.java:223) and travels with node results through `combineInfo`. Fields:
  - `nodes : TTrackRef` (tt/TTrack.java:120). This is the head of a singly linked list of weak node refs. Sentinels:
    - `null`: the owning node has not completed. It is also the "I own this track" marker used by `merge`.
    - `TTrackRef.Nil`: completed but nothing to invalidate, or all referents GC'd.
    - `TTrackRef.Invalid`: invalidated.

    The field is plain, not volatile. It is written by `weakCompareAndSet` through `nodes_h` (tt/TTrack.java:243) and by `compareAndSet` (247). Invalidation (tt/Invalidators.java:63) and cleanup (tt/CleanupVisitor.java:168, 173, 181) write it with plain stores.
  - `ptracks : Ptracks` (tt/TTrack.java:123) holds the **callers**, i.e. the dependants. It is never null. It has four representations:
    - `Ptracks0` (no callers).
    - A bare `TTrack`: one caller. The track serves as its own 1-element `Ptracks` (tt/TTrack.java:303–339). The header comment at tt/TTrack.java:65–68 calls this "dangerous".
    - `Ptracks2`: two callers, in plain mutable fields (tt/Ptrack.java:149).
    - `PtracksArray`: a volatile `data[]` plus a plain `nextFree`, appended by per-slot CAS (tt/Ptrack.java:227–318).

    Replacing the whole representation goes through `callerCAS` → `ptracks_h.compareAndSet` (tt/TTrack.java:132). Cleanup writes in place with plain stores: `set`, `drop` and `compress` (tt/Ptrack.java:185–215, 338–377; tt/TTrack.java:326–334).
  - `lastUpdateTraversalID : int` (tt/TTrack.java:109) is a plain field used only by traversals for visited marks and the redundant-edge algorithm.
- **`TTrackRoot`** (tt/TTrack.java:342) is a `TTrack` created with `nodes = Nil`. One per tweakable key, held strongly in `TweakableTracker.ttracks: ConcurrentHashMap[TweakableKey, TTrackRoot]` (tt/TweakableTracker.scala:76). The key is `node.trackKey`, which is either a tidy `PropertyNode` copy (strongly retains the entity and args; graph/PropertyNode.scala:95, 122) or a `NodeHash` for `trackTweakableByHash` properties.
- **`TTrackRef`** (tt/TTrackRef.java:25) is a list cell with `nref` (plain) and `final WeakReference<NodeTask> weakRef` (composition, not inheritance; see the TODO at tt/TTrackRef.java:26–29, "still not sure why"). The weak reference is registered with the tracker's `RefCounter` queue when one exists (tt/TTrackRef.java:132–139). TTrackRefs are **never cleared explicitly**. A ref becomes empty only when the node is GC'd, and is unlinked only by `CleanupVisitor.preVisit` (tt/CleanupVisitor.java:154–187). An invalidated node stays referenced (weakly) until cleanup unlinks the ref. Invalidation does not unlink refs; it replaces the whole list with `Invalid`.
- **`Ptracks`** (tt/Ptrack.java:25): see above. `addCaller` is the only concurrent mutator. Dedup is best effort: `TTrack`/`Ptracks2` check identity, and `PtracksArray` rescans the last `PTRACK_DEFAULT_RESCAN = 2` slots (trk/DependencyTrackerSettings.java:23). Duplicate edges are allowed and removed by cleanup.
- **`RefCounter` / `RefQ`** (tt/RefCounter.scala:25; tt/TweakableTracker.scala:507) counts GC-cleared refs for the cleanup heuristic and fires `onWatermarkBreached` every `ttrackClearedNodeRefWatermark = 100_000` (graph/Settings.java:206). It polls only inside `createWeakReference`, i.e. inside the `TTrackRef` constructor on a graph thread.

### Shape of the graph

```
TTrackRoot(k1)      TTrackRoot(k2)        <- one per tweakable key, in ttracks map (strong)
   | ptracks           | ptracks          (edge direction: dependency -> dependant, "callers")
   v                   v
 TTrack T_a  ------>  TTrack r            <- r created by merge() when a node combined two tracks
   nodes: [a, b]       nodes: [p]         <- weak refs to completed cacheable nodes sharing this dep-set
                         |
                         v
                       TTrack r2 ...
```

- Edges point from dependency to dependant: a `TTrack` lists its *callers*. Invalidation walks callers transitively.
- Strong: map → root, root → callers (and transitively up), and every node → its `_xinfo` TTrack. A live cached node therefore pins its own TTrack and the entire dependant TTrack subgraph above it, though not the dependant nodes.
- Weak: TTrack → node (`TTrackRef.weakRef`).
- Graphs of different trackers are interconnected. A child-tracker node that consumes a parent-scenario node merges the parent graph's TTrack, so child TTracks hang as callers off parent TTracks. The overlay listener (graph/TemporaryOverlayTweakableListener.scala:32–64) reports one node to two trackers. Cleanup of a tracker therefore walks into descendant graphs (tt/TweakableTracker.scala:329–331 says so).

### How the graph is built (graph threads, during evaluation)

1. **Tweakable node completes.** `NodeTask.complete` (graph/NodeTask.java:1232). If `isTrackingValue && ss.isTrackingIndividualTweakUsage` and the node is not cancelled, it calls `tweakableListener.onTweakableNodeCompleted(this)` (1250). That is `TweakableTracker.track(node.trackKey, node)` (tt/TweakableTracker.scala:111, 130–158):
   - `ttracks.get` or `putIfAbsent(key.tidyKey, new TTrackRoot)` (138–147).
   - `node.combineXinfo(root)` (156).
   - The `TRACKING_VALUE` flag is cleared at completion (graph/NodeTask.java:1251), so parents do not report it again.
2. **Tweak or tweakable used without being executed in this scenario.** These go through `onTweakUsedBy` (tt/TweakableTracker.scala:118; from graph/TweakNode.scala:114 and platform/ScenarioStack.scala:1236), `onTweakableNodeUsedBy` (113; from graph/PropertyNode.scala:252 via `combineInfoDirectCaller` graph/NodeTask.java:1342–1343, graph/cache/NCProxies.java:250, platform/ScenarioStack.scala:1228) and `onTweakableNodeHashUsedBy` (115). All of them reach `track(key, consumer)` → `consumer.combineXinfo(root)`, so the *consumer* adopts or merges the root.
3. **Every consumed child.** `combineInfo` → `combineXinfo(child._xinfo)` (graph/NodeTask.java:1320–1360) → `NodeExtendedInfo.combine` (graph/NodeExtendedInfo.scala:138) → `TTrack.merge(child, ntsk)` (tt/TTrack.java:154):
   - Parent `_xinfo` is `Null`: the parent just *shares* the child's TTrack (graph/NodeExtendedInfo.scala:186). This is the chain optimisation.
   - Parent `_xinfo` is a TTrack with `nodes == null` (owned, parent in flight): `child.addCaller(this)`.
   - Otherwise (the parent is sharing a completed track): allocate `r = new TTrack(ntsk.isDone() ? Nil : null)`, `this.addCaller(r)`, `child.addCaller(r)`, and the parent's `_xinfo = r`.

   Edges are linked **eagerly at merge time**, before the parent completes.
4. **Node completes.** `_xinfo.nodeCompleted(this, isCancelled)` (graph/NodeTask.java:1255 → tt/TTrack.java:174):
   - Cancelled: no-op.
   - `effectivelyCacheable()`: CAS-prepend a new `TTrackRef(ntsk)` onto `nodes` (230–243). If the head was `null` or `Nil` the ref is installed as the whole list. If the head was `Invalid`, the ref is prepended in front of the `Invalid` sentinel (see S1).
   - Non-cacheable: CAS `null → Nil` (247).

   This happens *before* `tryUpdateState(DONE)` (1270), so a node is in its TTrack before anyone can observe it done.

   Note the comment at tt/TTrack.java:176–178: every *cacheable* node is recorded, cached or not. `nc_scenarioStack` nodes and evicted nodes are tracked too.
5. **User-tracked nodes.** A `UTrackNode` (trk/UserNodeTracker.scala:239) is a `ProxyPropertyNode` with `effectivelyCacheable = true` (278), attached to `tracker.scenarioStack` (243) and registered as a continuation of the real node (229). On completion it `completeFromNode`s (298), which combines the real node's xinfo, so it lands in the same TTrack and is invalidated with it.

### How the graph is read (invalidation, update thread)

`TweakableTracker.invalidateByTweaks` (tt/TweakableTracker.scala:160) dispatches to one of three paths:

- **Instance tweak:** `invalidateByKey` (195, 218). It does `ttracks.get(k)`, then `ttracks.remove(k)`, then `Invalidators.invalidate(root, …)`, then recurses into `children(pkey)`. For the underlay tracker the children are `tweakableTracker` (trk/DependencyTracker.scala:96–103). For the main tracker they are the children's underlay trackers (118–120).
- **Property tweak** (`hashKey == null`): `invalidateByPropertyMap` (262) scans the *whole* `ttracks` map and the `matchOn` parents. It costs O(roots) per property tweak.
- **Untracked property:** `invalidateAllCachedNodes` (288) logs an error and filters the entire global cache through `Caches.clearCaches`, calling `node.invalidateCache` on every node whose listener responds to the consistent-subtree root (301–309). This hits in-flight nodes too. Then `onInvalidateAll` → `doClearTracking` (trk/DependencyTracker.scala:460) clears the maps and invalidates all UTracks.

`Invalidators.invalidate` (tt/Invalidators.java:28) is a `MarkingGraphTraverser` (tt/MarkingGraphTraverser.java:20, tt/SimpleGraphTraverser.java:48). It uses one fresh traversal id and the equality test `lastUpdateTraversalID != id`:

- **preVisit:** for every live ref in `nodes`, `task.invalidateCache(observer)` (36).
- **postVisit:** `ptracks = Ptracks0; nodes = Invalid` (61–64), as plain stores.

The walk is depth-first over callers with an explicit stack (tt/MutableTTrackStack.java). Traversal ends at the first `at(i) == null` (tt/SimpleGraphTraverser.java:62–64): a gap in a `Ptracks` truncates the walk.

### What `invalidateCache` leaves behind

`NodeTask.invalidateCache` (graph/NodeTask.java:1140) does `updateState(INVALID_CACHE | NOT_RUNNABLE, clear DONE_FLAG)`, an unconditional CAS loop.

- **Done node:** becomes `STARTED|NOT_RUNNABLE|INVALID_CACHE` (plus bit 8 if it failed). `isDone()` is false, `isDoneEx()`/`isDoneOrInvalidated()` are true (279–287), and the result field is retained. The cache skips it (graph/cache/NCEntry.java:30, 35, 220). The entry keeps occupying its slot and pinning the result until it is replaced or evicted (trk/DisposalSupport.scala:55–56 acknowledges this).
- **In-flight node:** `NOT_RUNNABLE` blocks `tryRun`. When it later completes it gets `DONE|INVALID`. Sync waiters exit their loop on `isDoneEx()` (graph/OGSchedulerContext.java:762) without being notified. This is R6.

Overrides:

- `UTrackNode.invalidateCache` (trk/UserNodeTracker.scala:334) does **not** call `super`. Under the UTrack write lock it swaps in a fresh `UTrackNode`, invalidates the template only if the template is done (341–343), and posts `notifyNodeInvalidated`.
- The batch-updater debug node (trk/DependencyTrackerBatchUpdater.scala:713) calls `super` and adds an over-invalidation check.
- `TweakExpander` calls `invalidateCache` on itself while running (graph/TweakExpander.scala:74–76). See S3.

**Through TTracks, `invalidateCache` only reaches nodes that have executed `_xinfo.nodeCompleted` (graph/NodeTask.java:1255).** Such a node is done, or sits in the 1255→1270 window of `complete()`. The R6 state can only arise through TTracks if an update overlaps that window, i.e. if the barrier has a hole. `invalidateAllCachedNodes` and `TweakExpander` reach in-flight nodes directly.

## 2. Concurrency model

| Operation | Thread | Mechanism |
|---|---|---|
| `track` → map `get`/`putIfAbsent` (tt/TweakableTracker.scala:138–147) | graph, many | CHM |
| `combineXinfo` / `merge` → `addCaller` (tt/TTrack.java:125–171) | graph, the node's own thread | `ptracks_h` CAS; `PtracksArray` slot CAS + plain `nextFree` + volatile `data` CAS on resize |
| `nodeCompleted` (tt/TTrack.java:174–248) | graph | `nodes_h.weakCompareAndSet` / `compareAndSet`; plain re-read of `nodes` in the loop |
| `trackCount.increment` (tt/TweakableTracker.scala:135) | graph | `LongAdder` |
| `RefCounter.createWeakReference` → poll → `maybeBreachWatermark` (tt/RefCounter.scala:54–107) | graph, inside `complete()` | `AtomicInteger cleared`, `@volatile watermark`, double-checked `synchronized(this)`; `onWatermarkBreached` outside the lock |
| `onWatermarkBreached` → `root.runCleanupNow` → `queue.maybeRunCleanupNow` (tt/TweakableTracker.scala:510–524; trk/DependencyTrackerQueue.scala:601–617) | graph, inside `complete()` | takes the root's **`queueLock`** (608) |
| `invalidateByKey` / `invalidateByPropertyMap` / `Invalidators` (tt/TweakableTracker.scala:195–283; tt/Invalidators.java) | update action thread of the tweaked tracker | **none**: plain stores to `nodes`, `ptracks`, `lastUpdateTraversalID` |
| `invalidateAllCachedNodes` (tt/TweakableTracker.scala:288) | update | none beyond the cache's own |
| `CleanupVisitor.clean` (tt/CleanupVisitor.java:189) via `TrackingGraphCleanupAction` (trk/GarbageCollectionSupport.scala:103) | **root** update action (low priority, interruptible) | none; plain stores; CHM iterator `remove` |
| `withFreshTraversalId` (trk/DependencyTrackerRoot.scala:116–133) | any traversal; documented "only from Update actions on the root" (113–114) | none; `traversalId` and `resetId` are plain `private[this] var` (100–102) |
| `resetAllTraversalIds` (trk/DependencyTrackerRoot.scala:139; trk/DependencyTracker.scala:467) | whoever triggers rollover or a traversal exception | none; walks every tracker's graph |
| `disposeTweakableTracker` (`ttracks = null`, tt/TweakableTracker.scala:106) | dispose update | plain `var` |
| `UTrack.swapIf` / `read` / `doIf` (trk/UserNodeTracker.scala:177–202) | update (invalidate) vs graph (completion callback) | `ReentrantReadWriteLock` per UTrack; `UTrackNode.onChildCompleted` also `synchronized(this)` (289) against double completion |
| `CleanupScheduler` (trk/GarbageCollectionSupport.scala:183) | timer thread | `synchronized` on the scheduler; it only enqueues actions |

**Volatile, Atomic and CAS in the ring, and what each protects:**

- `TTrack.nodes_h` CAS: concurrent prepends from graph threads completing nodes that share a track. The diamond case is documented at tt/TTrack.java:182–213.
- `TTrack.ptracks_h` CAS: concurrent caller additions, plus promotion 0→1→2→array.
- `PtracksArray.data` volatile + `data_h` CAS: array growth. `dataElm_h` CAS: slot claim. `nextFree` is plain (tt/Ptrack.java:250), "safe" by the argument in the comments at 265–316.
- `RefCounter.cleared` (AtomicInteger) and `watermark` (volatile + `synchronized`): breach once per watermark.
- `TweakableTracker.ttracks` (CHM): root lookup/insert vs invalidation/cleanup removal.
- `trackCount` (LongAdder): heuristic only.
- `DependencyTracker.scenarioStackWithCancelScopeCache` (volatile, trk/DependencyTracker.scala:191): publishing the scope-bound scenario stack.
- The UTrack RW lock and `UTrackNode` monitor; `UTrack.memo` is `@volatile` (trk/UserNodeTracker.scala:157).

**How the two sides are kept apart:**

- **Nothing inside the TTrack code keeps them apart.** The "LOCKING STRATEGY" comment (tt/TTrack.java:98–103, "mutable fields are only updated or observed with a sync lock on this", "lock ordering – children first") is **stale**: there is no `synchronized` anywhere in `TTrack`/`Ptracks`. The header's "invalidatePreview" (44) no longer exists.
- Exclusion comes from `DependencyTrackerQueue` (the other reader's ring; summarised only so far as this ring relies on it):
  - `recordStart` refuses an update while work is in progress, and an evaluation while updating (trk/DependencyTrackerQueue.scala:126–145).
  - `evaluationBarrier` cancels the shared evaluation scope and calls `waitForCancelledNodesToStop` (412–422). That waits for nodes that are *running or sync-suspended* in the scope (graph/OGScheduler.java:1627–1670). Async-suspended nodes are not waited for; they cannot run again except to complete as cancelled, and `nodeCompleted` ignores cancelled completion (tt/TTrack.java:175). Their `merge`/`addCaller` can still happen if they resume and `combineInfo` before noticing cancellation (see S8).
  - Child queues register on the parent as an *evaluate* action (trk/DependencyTrackerQueue.scala:194–197, 214–221). A parent update is therefore exclusive over the whole subtree, but a **child update runs concurrently with parent evaluations and with sibling updates and evaluations**.
  - Cleanup is a root update, so it is tree-exclusive.
  - Overlays are restricted to the same consistent subtree, i.e. the same queue (platform/AdvancedUtils.scala:820–823), so an overlay listener writing into two graphs is covered.
- The barrier covers only nodes running in the queue's cancel scope. Nodes whose scenario stack carries a different scope, or none, are not stopped. Examples: anything evaluated directly under `DependencyTracker.scenarioStack` (`private[optimus]`, trk/DependencyTracker.scala:144) or `nc_scenarioStack` (public `final val`, 150) without `withCancellationScope`. Production callers I found all attach the queue scope (trk/DependencyTrackerBatchUpdater.scala:298–299, trk/UpdateMultipleScenarios.scala:36, trk/DependencyTracker.scala:222–224). Nothing enforces this.

## 3. Invalidation end to end

`MutableTweakContainer` (other reader) → `tweakableTracker.invalidateByTweaks` (tt/TweakableTracker.scala:160). For each tweak:

1. **Key → roots.** The root for `trackKey` is removed from the map, then the walk runs over it and recursively over the same key in each child tracker (218–233).
2. **Roots → TTracks.** All TTracks transitively reachable through `ptracks`.
3. **TTracks → nodes.** Every live `TTrackRef` referent.
4. **Each node** gets `invalidateCache`, with the state described above.
5. **Each visited TTrack** is emptied (`ptracks = Ptracks0`, `nodes = Invalid`). Edges *into* those TTracks from other, still-live roots remain until cleanup drops them (tt/CleanupVisitor.java:145, `caller.nodes == Invalid`).

**What prevents re-caching a just-invalidated node.**

- The invalidated *object* is sticky: `INVALID_CACHE` is never cleared except by `reset()` on a clone (graph/NodeTask.java:1177), and the cache never returns it (graph/cache/NCEntry.java:30).
- A new lookup creates a fresh node, which reads the new tweak and registers in a fresh root.
- **What prevents an evaluation from producing a node that read the old value but is recorded only in invalidated TTracks:** only the barrier. If a node P is in flight (P has merged a dependency track `T`, or owns `r` with `nodes == null`) while `invalidateByKey` runs:
  - the walk sets `r.nodes = Invalid` / `T.nodes = Invalid`;
  - P completes and `nodeCompleted` prepends P in front of `Invalid` (tt/TTrack.java:232–240, `initial == Invalid` takes the `else` branch);
  - P is cached, valid, and unreachable from the new root for that key.

  This is S1.

**Torn invalidation.** Within one tracker, invalidation of a batch of tweaks happens inside one update action. Evaluations are excluded for the whole action, so no evaluation observes a half-invalidated state, provided the barrier holds. Across trackers:
- A child update is concurrent with parent evaluations. Parent nodes cannot see child-scenario nodes, so this is harmless as far as I can see.
- Sibling updates are concurrent with each other. They touch disjoint graphs unless graphs overlap; overlap needs overlays, and overlays need the same queue.

The tear that *does* exist is between the **value** change and the **graph**: if the tweak expansion or by-name tweak evaluation for an update evaluates nodes in the tracker's own scenario stack mid-update, those nodes complete into the graph while it is being rewritten. I did not trace `TweakContainer`/`TweakExpander` scenario stacks to confirm or rule this out; flagged for the ring 4a reader.

## 4. Cleanup and GC

**When.** All of these enqueue a `TrackingGraphCleanupAction`, a **root** update action, `isLowPriority = true` (trk/GarbageCollectionSupport.scala:103–137):

- `CleanupScheduler` on queue idle, debounced, run from a `java.util.Timer` thread (trk/GarbageCollectionSupport.scala:216–246).
- GC monitors: graph/LegacyGCMonitor.scala:405, graph/GCNative.java:1262.
- The `RefQ` watermark, at the head of the queue and uninterruptible (tt/TweakableTracker.scala:510–524).
- `doImmediateUninterruptableForcedCleanup` from stats (tt/TTrackStatsSupport.scala:148).

`CleanupIfNecessary` gates the scheduled ones (tt/TweakableTracker.scala:80–99). Cleanup runs when any of these holds:
- GC-cleared refs since the last cleanup > 20 × #roots;
- `track()` calls > 400 × #roots;
- the adaptive timer (5 s..300 s, `recalculateTimedCleanupDelay` 481–504) has expired.

Cleanup is interruptible: `queue.interrupter` = "pending work exists" (trk/DependencyTrackerQueue.scala:48). A `PartiallyComplete` carries a live CHM iterator across actions (tt/TweakableTracker.scala:404–408), and evaluations run between pieces. Each piece starts a fresh traversal id.

**What cleanup removes** (`CleanupVisitor`, an `AdvancedGraphTraverser`, tt/AdvancedGraphTraverser.java:26–59):

- **Dead refs.** `preVisit` unlinks GC'd refs; a fully dead list becomes `Nil` (tt/CleanupVisitor.java:154–187). An `Invalid` sentinel left in the middle of a list by a post-invalidation prepend is unlinked as if dead (181), which "revalidates" the TTrack.
- **Middle-man TTracks.** A caller whose `nodes == Nil` and which has exactly one caller is bypassed: the callee's slot is rewired to the grand-caller (125–134).
- **Edges to dead ends and invalidated callers.** Edges to `Nil` callers with no callers, and to `Invalid` callers, are dropped (139–149, with the `prevMinCallerID == MAX` path for leaves).
- **Redundant duplicate edges.** An edge is dropped when another path from callee to caller was already traversed. This is decided by the id ordering: drop if `callee.id < caller's min id` (142–149; the algorithm is explained at 22–100).
- **Excess capacity.** `postVisit` compresses `Ptracks` (113).
- **Roots.** Removed when `ptracks.at(0) == null && nodes == Nil` after the visit, or when the property is no longer `trackForInvalidation` (199–211).

**How cleanup avoids removing a track a concurrent evaluation is about to write.** It doesn't; it relies on the root update being exclusive. The structural rules are only safe if no in-flight node owns or shares a TTrack:
- Middle-man bypass and dead-end drop treat `Nil` as "nobody will ever attach to this again". Any later consumer of a completed node whose `_xinfo` is that TTrack attaches callers to an orphan (S5).
- The comment at tt/CleanupVisitor.java:157–159 says `nodes == null` cannot happen during cleanup "in principle", but it does "for non-completable nodes". Those TTracks are skipped by the `!= null` guard and are not bypassed (the `== Nil` test excludes `null`).

**Growth model between cleanups (retention foot-gun).** Per evaluation:
- one `TTrackRef` + one `WeakReference` per completed cacheable node per track (including nodes never actually cached);
- one new `TTrack` + two edges per `merge` against a completed shared track;
- duplicate edges whenever a parent combines the same child track outside the 2-slot rescan window;
- `PtracksArray` doubling with no shrink until cleanup;
- one CHM entry per distinct tweakable instance read.

Nothing is reclaimed between cleanups:
- GC'd nodes leave their `TTrackRef`/`WeakReference` cells.
- Invalidated TTracks are kept alive by edges from other roots.
- Root keys strongly retain entities and args (tidy keys).
- A live cached tweakable node pins `root_k` and therefore the whole dependant TTrack DAG (strong `ptracks` upward).
- Cleared `WeakReference`s sit in the `ReferenceQueue` until the next `TTrackRef` is constructed (tt/RefCounter.scala:78–88).
- After disposal only `tweakableTracker.ttracks` is nulled (trk/DisposalSupport.scala:79). `underlayTweakableTracker` is not disposed; its graph lives until cached nodes holding its TTracks are cleared (lazily, `Caches.lazyClearDisposedTrackers`).

**What the weak references buy and cost.**
- Buy: the TTrack graph never keeps nodes, or their results, alive, so cache eviction actually frees memory.
- Cost: one `WeakReference` per tracked completion, i.e. reference-processing load in every GC proportional to live tracked nodes. The TODO at tt/TTrackRef.java:26–29 records a past pathology with this.
- Cost: invalidation must tolerate cleared refs (`visitRef(TTrackRef)` no-op).
- Cost: cleanup is the *only* reclamation, and the GC-pressure trigger is itself counted lazily.
- Cost: the CAS retry loop in `nodeCompleted` allocates a `WeakReference` per attempt (tt/TTrack.java:232, 240).

## 5. Invariants

**Relied on:**

- **I1. Every tweakable dependency of a cached node is in the node's TTrack's ancestry before the node is observable as done.** Provided by eager linking in `merge` (tt/TTrack.java:161, 167–168) and by `nodeCompleted` running before `tryUpdateState` (graph/NodeTask.java:1255 vs 1270). It assumes every consumer calls `combineInfo` (the comment at graph/NodeTask.java:1603). Anything that reads `node.result` without `combineInfo` silently drops the dependency, with no enforcement.
- **I2. No in-flight node of this tracker, or of any descendant, runs during this tracker's update or cleanup.** This is the barrier. Breakers: nodes in a scenario stack without the queue's cancel scope; async-suspended cancelled nodes resuming into `combineInfo` (S8); evaluation in a disposed tracker (S7).
- **I3. `merge` ownership: `nodes == null` ⇒ the TTrack is the private xinfo of exactly one in-flight node** (tt/TTrack.java:156–162). It holds by construction, since a TTrack is created with `null` only for an in-flight owner (166). Invalidation can set it to `Invalid` while the owner is in flight, but only by breaking I2.
- **I4. A `Nil` or `Invalid` TTrack will never again gain callers or nodes that matter.** Cleanup relies on it (tt/CleanupVisitor.java:125–149). It is violated if a completed node whose xinfo is that TTrack is consumed later (S5).
- **I5. Traversal ids are monotonic and unique per traversal across the whole root.** Mark-equality and the redundant-edge rule rely on it (tt/MarkingGraphTraverser.java:31; tt/AdvancedGraphTraverser.java:36; tt/CleanupVisitor.java:145). It is documented as "only from Update actions on the root" (trk/DependencyTrackerRoot.scala:113–114) and violated by child-tracker updates (S4).
- **I6. Within a `Ptracks`, slots `< currentSize` are non-null** ("no gaps"). The traversals stop at the first null (tt/SimpleGraphTraverser.java:62, tt/AdvancedGraphTraverser.java:34–47). `PtracksArray` maintains it by slot CAS, with a formal JMM hole on resize (S9).

**Provided:**

- **P1. Invalidation reaches every node recorded in any TTrack reachable from the key's root,** in the tweaked tracker and its children (tt/TweakableTracker.scala:224–232), assuming I5 and I6.
- **P2. An invalidated node is never served from the cache again** (graph/cache/NCEntry.java:30).
- **P3. Untracked tweakables fall back to clearing every cached node in the consistent subtree** (tt/TweakableTracker.scala:174–176, 288–317). This is a correct but blunt safety net.
- **P4. Cleanup never drops an edge to a TTrack holding a live node ref, unless the redundant-edge rule proves another path.** I could not prove the id algorithm correct by reading; see the "not done" list. It assumes I4 and I5.

## 6. Suspects

Severity: H likely to bite, M needs specific conditions, L hygiene or latent. Lens in brackets.

### S1 (M) [data loss] Lost invalidation when any node writing to a tracker's graph overlaps an update

Mechanism:
1. P is in flight and has merged `T_k`, or owns `r` (a caller of `root_k`).
2. `invalidateByKey(k)` removes `root_k` from the map (tt/TweakableTracker.scala:226) and empties `T_k`/`r` (tt/Invalidators.java:61–64).
3. P completes. `nodeCompleted` sees `nodes == Invalid` and prepends P (tt/TTrack.java:232–243).
4. P is cached with a value computed from the old `k`.

`root_k'`, created later, has no edge to P. The result is a **stale value served until eviction or an unrelated invalidation**, with no error.

A variant without the walk: `track()` reads `root_k` from the map (138), the update then removes and walks it, then the node `combineXinfo`s the dead root.

Today this needs a hole in I2, and I found no production path. Candidates:
- user or framework code evaluating under `tracker.scenarioStack`/`nc_scenarioStack` without the queue scope;
- S7;
- S8.

The ring has no defence in depth. A cheap check would make this loud instead of silent: in `nodeCompleted`, if `initial == Invalid`, mark the node `invalidateCache` (or assert under `schedulerAsserts`). That turns S1 into a recomputation instead of a stale hit. *Do not break:* the lock-free append is the right design for the hot path; the fix is a sentinel check, not a lock.

### S2 (M) [race → deadlock] Watermark-triggered cleanup from inside `complete()` vs low-priority cancellation under `queueLock`

The updater side, thread U:
1. `registerOrEnqueueAction` holds the queue's `queueLock` (trk/DependencyTrackerQueue.scala:212).
2. For a normal update arriving while work is in progress it calls `cancelAndRescheduleLowPriorityActions` (226–228 → 269–283) → `EvaluateNodeKeysLowPriorityAction.cancel()` (trk/DependencyTracker.scala:304–327).
3. That calls `scheduler.waitForCancelledNodesToStop(cancelScope)` (312), which polls until every *running* node in the scope stops (graph/OGScheduler.java:1627–1670). U is still holding `queueLock`.

The graph side, thread W:
1. W is running a node of that low-priority scope, inside `NodeTask.complete` → `TTrack.nodeCompleted` → `new TTrackRef` → `RefCounter.createWeakReference` → poll finds cleared refs → `maybeBreachWatermark`.
2. The 100 000 watermark is crossed → `RefQ.onWatermarkBreached` → `root.runCleanupNow` → `queue.maybeRunCleanupNow` → `queueLock.synchronized` (trk/DependencyTrackerQueue.scala:608).

If the low-priority action is on the root queue, or on a queue that shares it (`shareParentQueue`, trk/DependencyTracker.scala:81), W blocks on the lock U holds, and U waits for W's node to stop: **a permanent deadlock**. The comment at tt/TweakableTracker.scala:515–518 considered deadlock against *waiting on the cleanup* but not against `queueLock`.

Even without the deadlock, every graph thread that crosses a watermark during that window stalls inside `complete()`. Conditions: an update arrives while a low-priority evaluation is running on the root queue, and the 100k-cleared-refs watermark is crossed by a node of that evaluation during the cancel wait. The cancel wait lasts as long as the slowest running low-priority node.

Fix direction: never take `queueLock` from `onWatermarkBreached`. Set a flag and let `afterActionCompleted` or the timer enqueue the cleanup; or hand the enqueue to another executor. Also, `cancel()` should not wait while holding `queueLock`. Probe: Tier 1, with `ttrackClearedNodeRefWatermark=1` and a delay hook in `waitForCancelledNodesToStop`.

### S3 (M) [race / wrong result, R6 made live] `TweakExpander` invalidates its own running node

graph/TweakExpander.scala:69–76. With `tweakTranslatorsMustBePure = false`, the default (graph/Settings.java:96–97), an impure tweak translator triggers `replace(...)` and `invalidateCache(NoOp…)` on the *running* expander node, then `doExpansion()` runs again. Between that call and completion the node is `INVALID_CACHE|NOT_RUNNABLE` with no `DONE_FLAG`.

A concurrent caller sync-waiting on the same (cached) expander node exits `while (!ntsk.isDoneEx())` (graph/OGSchedulerContext.java:762) on its next loop check, for example after running another task while helping. It then `combineInfo`s and reads the result of a node that has not completed. Async waiters are fine: they are notified on the eventual `complete`. The comment's intent, "so that the current caller(s) will get this value", is defeated for sync callers.

This is the first production path I found where `invalidateCache` hits an in-flight node. It ties R6 from "mechanism demonstrated" to "reachable". Probe: Tier 1, an impure translator that throws `GraphFailedTrustException`, with two concurrent sync requesters.

### S4 (M) [race] Root-wide `traversalId` used concurrently by child-tracker updates

`withFreshTraversalId` reads and writes plain `traversalId` with no lock (trk/DependencyTrackerRoot.scala:100, 116–133). Its contract is "only from Update actions on the root" (113–114). But every `TweakableTracker` in the tree uses the root as `idSource` (trk/DependencyTracker.scala:93, 117), and a *child* tracker's update runs while the root is merely "evaluating" (child queues register as evaluations, trk/DependencyTrackerQueue.scala:194–197). Two sibling-tracker updates therefore call `Invalidators.invalidate` concurrently.

Effects:
- **Duplicate ids.** Both marking traversals can get the same id. That is harmless on disjoint graphs. On shared TTracks, each traversal skips what the other marked. Both invalidate everything they visit, so the union is complete, but one update may finish and release evaluations while the other is still emptying a shared region.
- **Lost updates.** `traversalId` can move backwards if a traversal that advances by more than one runs concurrently (`DigraphWriter`, `GraphMaxDepthTraverser`). Today those run only in root updates or tests.
- **Rollover.** When `traversalId > Int.MaxValue/2`, or after any traversal exception, `resetAllTraversalIds` rewrites `lastUpdateTraversalID` on *every* graph in the tree (trk/DependencyTrackerRoot.scala:117–120, 128–131; trk/DependencyTracker.scala:467–471). That includes graphs being traversed by a sibling update at that moment and graphs being appended to by parent evaluations. A concurrent `MarkingGraphTraverser` then re-visits (benign). A concurrent `AdvancedGraphTraverser` cannot run concurrently today, because cleanup is root-exclusive.

Severity M because it violates a documented invariant that P4 (cleanup correctness) depends on. I found no lost invalidation from it under the current traverser mix. Cleanup advances the id by one per edge, so rollover is reachable in long-lived apps: about 1e9 / edges-per-cleanup cleanups. Fix: make `traversalId` an `AtomicInteger` with `getAndAdd` reservations, or take ids from the tracker's own queue.

### S5 (L) [data loss] Cleanup's `Nil`-middle-man bypass and `Invalid`-edge drop orphan TTracks that a held completed node can still feed

tt/CleanupVisitor.java:125–149. A completed **non-cacheable** node N (its track `r` gets `Nil`, tt/TTrack.java:247) whose `_xinfo` is `r`:
1. N is consumed by a parent after a cleanup has bypassed `r` (`root_k → r → r2` rewritten to `root_k → r2`). The new parent attaches as a caller of the orphan `r`.
2. The new parent is unreachable from `root_k`, so a later tweak to `k` does not invalidate it.

The same applies after invalidation, when `Invalid` callers have their in-edges dropped.

This needs a completed node object to be re-consumed across a cleanup or invalidation: a `Node` held in user state, a node passed as an argument, or a stable node, though stable nodes are asserted not tracked (tt/TTrack.java:217). The value it carries is frozen too, so the staleness is arguably the user's RT violation. Recorded because I4 is undocumented and nothing asserts it.

### S6 (M) [foot-gun → stale] Toggling `trackForInvalidation` at runtime

The `DONT_TRACK_FOR_INVALIDATION` flag is set or cleared from external config (`setExternalConfig` → graph/NodeTaskInfo.java:759–765, 1340–1352).
- **Tracked → untracked:** handled. Cleanup drops the roots (tt/CleanupVisitor.java:202–204), and tweaks fall back to clear-all (tt/TweakableTracker.scala:171, 174–176).
- **Untracked → tracked:** nodes cached while untracked have no TTrack edges. A later tweak takes the `invalidateByKey` path (167–169) and misses them. **Stale until eviction.** There is no cache flush on the flag flip.

Whether optconf can be applied after the cache is warm is ring-2 territory; if it can, this is real.

### S7 (M) [foot-gun → process exit] Evaluating under a disposed tracker's scenario stack

`disposeTweakableTracker` sets `ttracks = null` (tt/TweakableTracker.scala:106; trk/DisposalSupport.scala:79). `track()` dereferences `ttracks` with no disposed check (138). If a tweakable node in that scenario stack completes afterwards, the NPE is thrown inside `NodeTask.complete` (graph/NodeTask.java:1250) → `catch Throwable` → `logAndDie` → exit 121 (1281–1285). If the NPE is raised from `onTweakUsedBy` during `run`, it becomes a node exception instead.

`isDisposed` exists on the listener (tt/TweakableTracker.scala:71) but nothing on the evaluation path consults it, and only `Caches.scala:239` uses it, for clearing. The underlay tracker is not disposed at all (asymmetric). Reachability: any code holding the tracker's scenario stack, or a cached node in it, after disposal.

### S8 (L) [race] Async-suspended orphan nodes can `merge` during an update

`waitForCancelledNodesToStop` captures only running and sync-suspended nodes (graph/OGScheduler.java:1645–1665). An orphan suspended on an async child, such as a plugin batch or DAL, can resume during the update. Its `onChildCompleted` → `combineInfo` → `merge` → `addCaller` CASes on a child TTrack while `CleanupVisitor`/`Invalidators` plain-store the same `ptracks` (tt/Ptrack.java:185–215, 338–346; tt/TTrack.java:326–334): a lost update or a torn `Ptracks2`/`PtracksArray`.

The orphan itself completes cancelled and is not recorded. The damage is to the *child's* caller list: a dropped or duplicated edge, or for the array a gap (I6), which truncates later traversals of that TTrack. I did not confirm whether a cancelled node's `combineInfo` runs before the cancellation check. Severity L pending that.

### S9 (L) [race, formal] `PtracksArray` resize can lose a just-CASed caller on weak memory models

tt/Ptrack.java:264–318. Thread A CASes `old[n-1]` (the last slot), then plain-writes `nextFree = n`. Thread B reads `nextFree == n` *plainly*, copies `old` (296) and CASes `data` to the copy. No happens-before edge orders A's slot store before B's copy: the volatile slot CAS is not paired with a volatile read in B, and plain `nextFree` publishes nothing. On AArch64, B can copy `old[n-1] == null`. The result is a gap at `n-1` (I6), so every caller at or after `n-1` is invisible to invalidation. Rare, and severe if it happens: a lost invalidation.

Fix: make `nextFree` volatile, or re-read the copied tail with `getVolatile` before the `data` CAS. A jcstress candidate.

### S10 (L) [perf] Hot-root contention and per-retry allocation in `nodeCompleted`

Widely-shared tracks (a root for a popular tweakable, where every single-dependency consumer shares it) take one CAS-prepend per completing consumer on one field. Each retry allocates a `TTrackRef`, a `WeakReference` and a reference-queue poll (tt/TTrack.java:230–243; tt/RefCounter.scala:105–108). The list grows without bound until cleanup, and invalidation of that root walks it all. A per-root striped list would fix it if it matters. Measure first.

### S11 (L) [perf] Invalidation and cleanup costs scale with the whole map or graph

- A property tweak scans every root (tt/TweakableTracker.scala:267–281).
- An untracked tweak scans the global cache (298–310).
- Cleanup visits every edge of every tracker per complete pass; ids are consumed per edge, which feeds S4's rollover.
- `GraphMaxDepthTraverser` re-traverses a TTrack every time it is found deeper (tt/GraphMaxDepthTraverser.java:25–35): exponential on adversarial DAGs. It is diagnostic only, but runs inside a root update, holding the tree.

### S12 (L) [foot-gun] Stale or misleading comments and test-only hazards

Signals of code added with less care:
- The "LOCKING STRATEGY" comment (tt/TTrack.java:98–103) describes locks that do not exist.
- "invalidatePreview" (44) no longer exists.
- `cleanupRequired` says "reset counters regardless" but resets nothing (tt/TweakableTracker.scala:93–96); the reset is at 357–358.
- `AbstractMultiPtracks.debugString` appends `"["` twice (tt/Ptrack.java:133, 137).
- The TODO "This reference queue is null in some tests, which can cause memory leaks" (tt/TTrack.java:226–227).
- The TODO about `WeakReference` inheritance with "still not sure why" (tt/TTrackRef.java:26–29).
- `MutableTTrackStack`: "be careful if we move to running cleanups in parallel" (tt/MutableTTrackStack.java:19).
- `TTrack` doubles as `Ptracks`, admitted "dangerous" (tt/TTrack.java:65–68). A self-edge is possible via `Ptracks0.addCaller(owner, owner)`, which has no self check (tt/Ptrack.java:97–99); `TTrack.addCaller` has one (317). I found no path that produces it.
- `TTrack.merge` casts `(TTrack) child` unconditionally (tt/TTrack.java:155). Any non-TTrack `info` combined into a TTrack-carrying node throws inside `combineInfo`.

### Properties that hold

- **H1. Single-writer graph maintenance, given the barrier.** Invalidation, cleanup and stats all run as update actions. Cleanup and stats are root updates, so they are tree-exclusive. Evaluations only append with CAS. Simple and cheap on the hot path. *Do not break* by adding locks to `addCaller`/`nodeCompleted`; fix the barrier holes instead.
- **H2. A node is in its TTrack before it is observably done** (graph/NodeTask.java:1255 before 1270). A parent that sees the child done always sees a TTrack that is already linked to the child's dependencies (eager `merge` linking).
- **H3. Invalidated nodes are sticky-invalid in the cache.** The cache predicate is centralised in `NCEntry` (graph/cache/NCEntry.java:30, 35, 220).
- **H4. Untracked tweaks degrade to clear-all, not to silence** (tt/TweakableTracker.scala:174–176).
- **H5. Overlays cannot cross concurrent subtrees** (platform/AdvancedUtils.scala:820–823), so the two-graph overlay listener is covered by one queue.
- **H6. Weak node refs:** the dependency graph never retains results.
- **H7. The diamond case is reasoned out in the code** (tt/TTrack.java:182–213), and `PtracksArray`'s no-gap argument is stated (tt/Ptrack.java:265–316). It is correct on TSO and needs a fence for S9.
- **H8. The `UTrack` swap protocol is properly locked:** an RW lock plus a node monitor prevent double notification and stale callbacks (trk/UserNodeTracker.scala:177–202, 283–332).

## 7. Testability

**Package.** Put whitebox probes in **`optimus.graph.tracking.ttracks`**, not `optimus.graph.tracking`. Scala code there can reach Java package-private members (`TTrack.nodes`, `ptracks`, `lastUpdateTraversalID`, `CleanupVisitor`, `Invalidators.invalidate(…, PNodeInvalidate, …)`, `TTrackRef.unsafeForceClear`, `PtracksArray.LINEAR_GROWTH`, `TTrackRef.size/toList`), and it still sees every `private[tracking]`/`protected[tracking]` Scala member: `TweakableTracker.ttracks`, `cleanupTTracks`, `resetTraversalIds`, `DependencyTrackerRoot.withFreshTraversalId`, `doImmediateUninterruptableForcedCleanup`, `queue.stateManager`.

**Building a tracker.**
- `DependencyTrackerRoot.apply(scheduler: TimedScheduler)` (`private[tracking]`, trk/DependencyTrackerRoot.scala:632) gives a root with a controllable clock and timer. It needs an initialised `EvaluationContext` scenario stack.
- Disable auto-cleanup with `CleanupSchedulerHelper.setAutoCleanupEnabled(false)` (trk/GarbageCollectionSupport.scala:251).
- `root.tweakableTracker` / `underlayTweakableTracker` are `private[optimus]`.
- `TweakableTracker` can also be constructed directly (public constructor, `private[optimus]` class). It still needs an `owner: DependencyTracker` because the `RefQ` constructor calls `owner.root` (tt/TweakableTracker.scala:73).

**Registering tracks.** Evaluate hand-written `PropertyNode`s:
- The `propertyInfo` needs `isDirectlyTweakable` (so `doInitTrackingValue` marks `TRACKING_VALUE`, graph/PropertyNode.scala:166–176), `trackForInvalidation` and cacheable flags.
- Evaluate under `root.scenarioStackWithCancelScope(root.queue.currentCancellationScope)` from inside an evaluate action. Or, to *demonstrate S1*, evaluate deliberately under raw `root.scenarioStack` from a non-queue thread.
- Alternatively bypass evaluation and call `tracker.onTweakableNodeCompleted(node)` / `onTweakableNodeUsedBy(key, node)` directly, then drive `node.combineXinfo`/`combineInfo` by hand.

**Driving invalidation and cleanup.**
- Invalidation: `tracker.invalidateByKey(key, tweak = null, cause, NoOpTrackedNodeInvalidationObserver)` is public. `tweak = null` skips the `PNodeInvalidate` trace.
- Cleanup: `tracker.forceCleanup()` (`protected[graph]`), or `cleanupTTracks(ForceCleanup, flag)` with a controllable `InterruptionFlag` to test partial cleanups.
- Simulate GC with `TTrackRef.unsafeForceClear()`.

**Pure graph probes (no scheduler).** `new TTrack()`, `new TTrackRoot()`, `merge`, `nodeCompleted` and `addCaller` can be driven with stub `NodeTask`s. A stub needs `effectivelyCacheable` overridden and a scenario stack whose listener's `refQ` is null (`ScenarioStack.constant`, `NoOpTweakableListener`). The agent is still required at runtime because it rewrites `NodeTask`. `TraversalIdSource` is a Scala trait with one method, so it can be stubbed for `Invalidators`/`CleanupVisitor` without a root.

**Needs reflection.**
- `DependencyTrackerRoot.traversalId`/`resetId` (`private[this]`), for S4 and to force rollover.
- `TweakableTracker.trackCount`/`collectedReferences`/`nextTimedCleanup` (`private`).
- `RefQ` (private class), or use `refQ.incrementCountManuallyForTestingAndTestWatermark` (tt/RefCounter.scala:90), which is public and enough for S2.
- `PtracksArray.data`/`nextFree` and `Ptracks2.p0/p1` (private; `at()`/`currentSize()` suffice for most checks).
- `DependencyTrackerQueue._currentCancellationScope` (`private[this]`; `currentCancellationScope` is `private[tracking]` and fine).

**Suggested probe targets.**
- **Cleanup model check (P4/I4/I5):** random DAGs built through `merge`/`nodeCompleted` with random `Nil`/`Invalid`/GC. Oracle: for every root, the set of live node refs reachable before cleanup equals the set reachable after.
- **jcstress:** S9 (`addCaller` across a resize), S8 (`addCaller` vs `drop`/`compress`), `nodeCompleted` vs `Invalidators.postVisit` (S1 at the object level).
- **Tier 1:** S2 deadlock (watermark 1 + update during low-priority evaluation), S3 (impure translator, two sync waiters), S7 (evaluate after dispose), S1 via a raw-scenario-stack evaluation racing a tweak.

## Not done (budget)

- The `TweakContainer` → `invalidateByTweaks` call sites and whether tweak expansion evaluates in the tracker's own scenario stack mid-update (the tear question in §3). Ring 4a.
- `UnderlayScenarioSupport`, `TrackDependencyCollector`, and the XS/recording interplay: `RecordingScenario` forwarding to `onTweakableNodeUsedBy`, and NCProxies:250.
- A proof, or a counterexample by hand, of the redundant-edge id algorithm in `CleanupVisitor`, in particular the restamping at tt/AdvancedGraphTraverser.java:46 after a middle-man bypass at tt/CleanupVisitor.java:131–134. It is left to the model-check probe.
- `DigraphReader`/`DigraphWriter` (diagnostic serialisation) skimmed only; `ttrackGraphString` has no production caller.
- Whether a cancelled node's resumption runs `combineInfo` before noticing cancellation (S8 severity).
- Whether optconf `trackForInvalidation` can change after the cache is warm (S6 reachability).
