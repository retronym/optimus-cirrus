# Ring 1 (cache) and ring 2 (key): reader notes

Paths under `optimus/platform/projects/core/src/main/scala/optimus/`. Line numbers from this worktree.

## Entry path

`PropertyNode.lookupAndGet/lookupAndEnqueue` (graph/PropertyNode.scala:196–246) → `ScenarioStack.getNode(key, ec)` (platform/ScenarioStack.scala:1115): asserts (only under `schedulerAsserts`) that the key is fresh and not a `TweakNode`; adjusts the stack (`info.adjustScenarioStack`, `withCacheableTransitively`); if the property is directly tweakable, builds a `TwkResolver` (graph/TwkResolver.scala) that walks the stack's `SSCacheID`s for instance then property tweaks, evaluating `when` clauses asynchronously (the resolver *is* a `PropertyNode` so it can suspend); otherwise `completeGetNode` → `cacheSelector.lookupAndInsert` → `NodeCacheBase.putIfAbsent` (graph/cache/NodeCacheBase.java:30).

`putIfAbsent`: policy switch (`XSFT` downgrades to `Basic` under tracking stacks), optional per-entity local slot (§F1), `alternativeLookup` (XSFT's "up-proxy" probe), then `putIfAbsentCore`: hash = `chash(key.hashCodeForCaching, ss)` mixing the key hash with `_cacheID.hashCode` (= the cache id's sequence number); scan the bucket with `getIfPresentCore` (equality = `value.equalsForCaching(key)`, asymmetric by design; scenario match = `_cacheID` identity; usability = `isUsableWRTCancelScopeAndEnv`); if not directly usable, wrap in a `DelayedCSProxyNode`/XS proxy and insert *that*; CAS the new entry at the bucket head; on CAS failure rescan only the segment added since (`tail = head`).

## Data structures

- `NodeCacheBase.table`: `volatile NCEntry[]`, per-bucket singly linked list, head CASed via `VarHandle`, `next` written only under `updateLock` (unlink) or before publication (insert). No remove in the table proper: entries become *invalid* when `value == null` or `value.isInvalidCache()`, and are unlinked lazily by `remove()` under `updateLock` when a ledger catches up or an eviction walks to them.
- `NCEntry` (graph/cache/NCEntry.java): `value`, `hash`, `next`. `NCEntryLRU` adds `before/after` (LRU list, `updateLock` only), `inLedger`, `hitCount`. Variants: profiling, grouped (children map), not-valid-with-exception, sized, marker.
- `NodeCacheWithLRU`: `ledgers[cacheConcurrency]` (per-thread-ish staging arrays, `synchronized(ledger)`), one `updateLock` (ReentrantLock) for the LRU list, `size`, `header`, eviction, clear, resize. `getLedger()` picks `ledgers[threadId & (n-1)]`. `touchEntry` appends to the ledger; when a ledger is full the toucher catches up *all* ledgers under `updateLock` (`catchupCore` → `catchupEntry`: new entry → add at head and `size += 1`; invalid → `remove`; hit → move to head) and then `removeOverflow`.
- `NodeScopedCache`: same table, no LRU, `size` via VarHandle, resize by tombstoning buckets (`grabEntry`) under `synchronized(this)` while readers spin in `foundTombstone`.
- `SSCacheID` (graph/CacheIDs.scala:188): per-stack tweak map (`JHashMap`), `tweakMask` (`TPDMask`), lazily built `twkHash` (double-checked, volatile). `SSScenarioCacheID` initialises lazily under `synchronized`. `MutableSSCacheID` for tracking scenarios: mutated in place, "synchronized not needed because dup during evaluate never runs concurrently with invalidate on the same TS" (a ring-4 invariant the cache depends on).
- `CacheIDs.global`: `ConcurrentHashMap[SSCacheKey, SSCacheIDRef(WeakReference)]` keyed by (parent cache id, flattened scenario) so sibling `given` blocks share one cache id and therefore one set of cached nodes. Dead refs are reused in place via `compute` + `updateKeyWithValue` (mutating the key inside the map, safe because hash is fixed at construction and equality falls back to the id).
- `TPDMask` (core/TPDMask.java): 2 qwords (128 bits) unless `enablePerNodeTPDMask`; tweakable ids beyond the width fold modulo (`indexListToMask`, `fromIndex`), which only adds false positives.

## Suspects

### S1 (M, data loss) `TPDMask.fromArray` truncates instead of folding

`fromArray` (core/TPDMask.java:44) does `Arrays.copyOf(maskArray, maskQWords)` when the input is longer than the configured width, logging "Compressing mask" at debug. Unlike `indexListToMask`, which folds with `offset % maskQWords`, this drops the high words. Any mask that arrives as an array longer than the process's `tweakUsageQWords` (an optconf `dependsOn` mask written by a process with a wider setting, `stringDecoded` configs, serialised masks) loses dependency bits. A lost bit in `dependsOnTweakMask` is the unsafe direction for XSFT: `minimumScenario`/`subsetOf` then believes the node does not depend on a tweak it does depend on, and a value computed under one scenario is reused under a scenario that tweaks that property. Probe: encode a mask with a bit in word 2, decode under width 2, run the XSFT reuse check.

### S2 (M, wrong result / stale) The per-entity local cache slot ignores the property, invalidation and usability

`NCPolicy.localMatch` (graph/cache/NCPolicy.java:256) returns `EntityImpl._localCache` when `entity`, `args == argsEmpty`, cancel scope and `_cacheID` match. It does not compare `propertyInfo`, does not check `isInvalidCache()`, `isDoneWithExceptionToHide()` or the environment, and the slot is a single plain field per entity (`EntityImpl.java:70`) that `NodeCacheBase.putIfAbsent:49` overwrites with whatever the lookup returned, proxies included. Consequences if two no-arg properties on one entity carry `LOCAL_CACHE`: the lookup for B returns A's node (wrong type/value). Under a tracking scenario the cache id is a `MutableSSCacheID` that keeps its identity across tweak updates, so after `invalidateCache` the slot still matches and hands back the invalidated node (`isDoneEx` true, `DONE_FLAG` cleared → `result` throws). Mitigation in practice: nothing in this export sets the `LOCAL_CACHE` flag (only `localCacheSlot` is read from config, and the slot number is ignored), so this is latent unless the plugin or optconf sets bit 19. Grade M for the mechanism, L for exposure here.

### S3 (L→M, races) `NCEntry.value` is not volatile despite the comments

`NCEntry.getValue/invalid/removed` are commented "volatile read" and `removeValue` "lazySet()", but the field is a plain `protected PropertyNode<?> value` (graph/cache/NCEntry.java:14). Readers on other threads may see a removed entry as live (a stale hit on an evicted node: harmless for correctness, retention only) or, in the other direction, the `remove()` fast path `if (old.removed()) return` can be skipped twice by two threads (both under `updateLock`, so no corruption). The entityagent does not touch these classes. Hygiene, but the comments are wrong and the design argument ("no remove, CAS all the time") rests on them.

### S4 (M, known, quantify) Table resize drops entries and desynchronises LRU and table

`setMaxSize` (graph/cache/NodeCacheWithLRU.java:…) swaps `table`, moves buckets with `moveEntries` until the old table reads empty, and the code admits (OPTIMUS-57931) a slow inserter can still CAS into the old table afterwards. Consequences they list: nodes in the LRU absent from the table (fixed by null checks in `remove`), and the opposite ("should never happen, because … it will never get cleared"). `verifyInvariants` downgrades the fatal checks to warnings for this reason. Probe: resize concurrently with inserts, then `verifyInvariants`; count both directions; look for the "table but not LRU" case, which is a leak.

### S5 (L, foot-gun) Scope path ids above 62 alias silently

`NCPolicy.scopeIDFromPath` (graph/cache/NCPolicy.java:~120) assigns sequential ids and only logs when one exceeds 62; `scopeMaskFromPath` does `1L << id`, which wraps at 64, so the 65th scoped path gets the mask of the 1st and composite policies apply to the wrong scope.

### S6 (L, foot-gun) Cache ids are shared across stacks that differ in anything but tweaks

`createChildInternal` (platform/ScenarioStack.scala:499) keys the shared cache id on (parent id, expanded scenario) only, and `initChild` copies it into a stack with possibly different `tweakableListener`, `siParams` (plugin tags, node inputs, cancel scope, scheduler), and flags. The cache's scenario match is `_cacheID` identity, so cached values flow between such stacks. This is the intended contract: `PluginTagKey.affectsNodeResult` is `false` by definition and `NotTransparentForCaching` only raises a barrier flag for XSFT's minimum-scenario search. A cacheable node that reads a plugin tag or node input is a silent wrong-reuse. Nothing checks it; `schedulerAsserts` has no hook here. Candidate for an RT-verifier-style probe (evaluate under two sibling stacks with different tags, compare).

### S7 (L) `Scenario.equals` is asymmetric across ordered/unordered

platform/Scenario.scala:185 says so. `SSCacheKey.equals` uses it, so cache-id sharing between an ordered scenario `[k := 1, k := 2]` and an unordered one would resolve the duplicate differently. The hash codes differ (`orderedHash` vs `unorderedHash`), so the CHM will almost never compare them; moot in practice.

### S8 (L, latent) `graph.cache.Interner` spins forever once an interned referent is collected

`Holder.internedValueOr` (graph/cache/Interner.scala:~20) does `interned.compareAndSet(null, wrapped)` but `interned` is never null (it holds a `WeakReference`), so when the referent has been collected and the cleaner thread has not yet removed the `Holder` from `hashTable`, the tail-recursive retry never terminates. Nothing in this export calls `optimus.graph.cache.Interner` (the live interners are Guava's and `core/WeakInterner`), so no exposure here; a two-line probe would confirm.

### S9 (L, perf) Global locks on the key side

`NodeTaskInfo.ensureTweakMaskIsSet` takes `synchronized (NodeTaskInfo.class)` (graph/NodeTaskInfo.java:606) on first use of every tweakable property; `updateDependsOnMask` takes `synchronized (this)` per widening (fine, monotone, new instance each time). `NCPolicy.scopeIDFromPath` synchronises on a static map per call with a non-empty path.

### S10 (L, perf) `getIfPresentCore` keeps scanning for a "better" match

[SEE_PROXY_CHAINING]: a tentative (not directly usable) hit does not break the scan; the whole bucket is walked on every lookup that lands on a running node with a different cancel scope. With deep buckets (small caches, `maxSize` 16 minimum, power-of-two tables) and many cancel scopes this is quadratic in the number of concurrent scopes requesting the same key.

### S11 (L, foot-gun) `getNode` checks that matter are assert-only

`ScenarioStack.getNode:1120`: "Re-used node as key", "TweakNode as key", "Tweakable node is not tracking value" are `GraphInInvalidState` only under `schedulerAsserts`. A hand-written node that reuses a key object (the plugin never does) silently caches the wrong thing.

### S12 (L) `NCEntryGrouped.removeValue` clears children racily

Acknowledged in the comment ("We clearly don't care all that much"); affects group-cached requery only.

## Properties that hold (candidates, with why)

- **Insert is lock-free and never loses a concurrent insert of a different key**: head CAS plus "rescan only the new segment" (`tail = head`) is the standard CHM-v7 bucket protocol. Two inserts of the *same* key race to the head; the loser rescans and finds the winner. Probe: jcstress insert-vs-insert same key, count distinct live nodes for the key (expect ≤ 1 plus proxies).
- **At-least-once semantics are explicit**: every path that can miss (resize, concurrent unlink) is documented as "ok to execute a node twice (even concurrently)", and that is sound because `tryRun` guards `run()` and both copies compute the same RT value. Duplicate computation is the cost.
- **Usability is centralised**: `isUsableWRTCancelScopeAndEnv` is the single predicate for cancel scope and environment, with a three-valued answer (usable / not / not yet) and the "not yet" case turned into a proxy rather than a block or a miss. Clean.
- **Packed ids are checked**: `CancelScopeAndEnvironmentIdPacker` reserves 40 bits for cancel scopes and 24 for environments and panics on overflow (contrast with the scheduler's unchecked 16-bit thread id, R1).
- **Mask compression is conservative in the folding paths**: `indexListToMask` and `fromIndex` fold modulo the width, which only adds false positives (less reuse, never wrong reuse). `fromArray` is the exception (S1).
- **Monotone widening of `dependsOnTweakMask`** with a fresh instance per update and a volatile read, so readers see either the old or the new complete mask.
- **Scenario-stack immutability with structural sharing**: `ScenarioStack` is a case class copied per change; `_stableSS` is the only mutable pointer and is double-checked under `this.synchronized` with a volatile read. Child stacks share the parent's `SSCacheID` object when tweaks are equal, so memo sharing across `given` blocks is free.
- **Hash stored on the node, not the entry** (`$hashCodeArgsAndInfo`), computed once per node, so bucket scans compare an int before calling `equalsForCaching`.
- **Ledger batching keeps the LRU lock off the hot path**: a hit costs one `synchronized(ledger)` append; `updateLock` is taken once per `cacheBatchSize` hits per ledger. `tryLock` first, blocking only when the ledger is full.

## Plugin contract, cache side

A hand-written `PropertyNode` must: have a singleton `NodeTaskInfo` (the cache keys policies, masks and tweak ids on its identity); implement `argsEquals`/`argsHash` consistently with `entity.equalsForCachingInternal`; never be reused as a key after it has been attached (`info() ne null`); be `isTrackingValue` if directly tweakable; return a stable `entity` (its hash is part of the cache hash); not read plugin tags, node inputs or the environment without `markAccessedRuntimeEnv` (only `ScenarioStackShared.environment` marks it). Violations of the first three are caught only under `schedulerAsserts`; the rest are silent.

## Testability

`UNodeCache(name, size)` is constructible directly and registers itself; `NodeScopedCache(size)` too. `ScenarioStack.constant` / `EvaluationContext.initializeWithoutRuntime()` give a usable stack; `createChild(Scenario)` with hand-built `Tweak`s needs a `TweakTarget`, which needs a `PropertyNode` key and `NodeTaskInfo` flagged tweakable. `setMaxSize`, `clear`, `verifyInvariants`, `getSize` are public. Private: `putIfAbsentCore` internals (fine, the public `putIfAbsent` is the probe surface), ledgers (reflection), `remove`.

## Not read in depth

`NCProxies` beyond the CS proxy (XSFT agent), `TweakNode`/`TweakExpander` (ring 3 agent), `RecordingScenario` (parked), `CacheGroupKey` beyond its race comment, the native heap-explorer glue in `NCSupport`.
