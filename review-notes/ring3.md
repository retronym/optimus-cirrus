# Ring 3 (evaluation context): reader notes

All paths are relative to `optimus/platform/projects/core/src/main/scala/optimus/`. Line numbers are for the current worktree. Read-only; nothing under `optimus/` was modified. `SSCacheID`/`CacheIDs` live in `graph/CacheIDs.scala`, outside the listed files, but they are the heart of question 1, so I read them in full. `TwkResolver.scala` (the resolver behind `TwkResolution`) was read in full too.

## 1. Data model

### ScenarioStack (`platform/ScenarioStack.scala:138-166`)

A `final case class` with these constructor fields:

| Field | Mutability | Meaning |
|---|---|---|
| `trackingDepth: Int` | val | depth in tracker/recording terms; children get +1, tracker stacks +2 (`:559`, `:589`) |
| `scenario: Scenario` (@transient) | val | the caller's **unexpanded** top scenario (`initChild` stores the caller's scenario, even on a cacheID hit, `:533`) |
| `name` | val | debug |
| `_parent` | **var** | parent stack. Mutated after publication by `setParent` (`:1148`), called only from `tracking/UnderlayScenarioSupport.scala:36-37` |
| `_cacheID: SSCacheID` | val | tweak identity, and the cache key component (below) |
| `flags: Int` | **var** | `EvaluationState` bits. Written only in the constructor (`:182`, `flags |= tweakableListener.extraFlags`) |
| `tweakableListener` | val | tracking/recording sink (the comment at `:262` calls it a var; it is a val now) |
| `siParams: SIParams` | val | "scenario-independent" params that are **not** in the cache key: node inputs (plugin tags), cancel scope, batch scope, progress, tracking ids, scheduler, pathMask (`graph/SIParams.scala:30-46`) |
| `ssShared` | val | per-root shared state (env, root/SI stacks) |
| `cacheSelector` | val | which UNodeCache to use (private/scoped caches) |
| `_stableSS` | **@volatile var** | memo for `withCacheableTransitively` (`:771-790`) |
| `pss` | **var** | profiler data, written by `Profiler.t_sstack_*` (`graph/diagnostics/pgo/Profiler.scala:329,344`) |

Derived in the constructor: `cancelScopeAndEnvId` (`:170-174`), which packs `cancelScope.sequenceId` (40 bits) and `env.sequenceId` (24 bits) for the cache's `isUsableWRTCancelScopeAndEnv` (`graph/cache/NCSupport.java:196-232`). `_hashCode` is a private memo var (`:283-297`).

Structural sharing: a child is `copy(...)` of the parent with `_parent = this` (`initChild`, `:578-602`). Sibling variants (`withCancellationScope`, `withPluginTag`, `withFlag`, etc.) are `copy(...)` with the same `_parent`. Case-class `copy` carries **every** field it is not told to replace, including `_stableSS`. That fact is behind suspect S4.

### SSCacheID and how stacks share one (`graph/CacheIDs.scala`)

- `SSCacheID` (`:188-382`) holds `tweaks: JHashMap[AnyRef, AnyRef]`, keyed by instance key (`PropertyNode`), by `NodeTaskInfo` (a property tweak or a `TweakKeyExtractor`), or by `ExtractorTweakableKey`. It also holds a 128-bit bloom `tweakMask: TPDMask`, a lazily built `twkHash` (for XS, `:283-318`), and `id: Long` from a global `AtomicLong` (`:162`, `:189`).
- **Derivation.** `createChildInternal` (`:499-543`) keys `CacheIDs.global` by `SSCacheKey(parent._cacheID.id, expandedScenario)` (`:522-528`). Equal parent cacheID + equal expanded scenario (`Scenario.equals`) means the same SSCacheID. By induction, two stacks share a cacheID iff they have the same root cacheID and level-for-level equal expanded scenarios, **unless** some level got its cacheID by another route (table below). Roots: `RuntimeEnvironmentCacheKey(env)` (identity of env), so one root cacheID per env; it is `SSCacheID.commonRoot` if `compatAllowCrossRuntimeCacheHits` (default false), and fresh if `uniqueID` (`graph/ScenarioStackShared.scala:63-66`).
- `CacheIDs.global` is a CHM of weak refs to SSCacheID values. Stale refs are polled on the next `putIfAbsent` (`CacheIDs.scala:79-83`). `SSCacheKey` drops its scenario once inserted and compares by `cacheIDID` once the referent is gone (`:118-158`).
- **Initialisation.** `SSScenarioCacheID` is lazily initialised (`ensureInitialized`, DCL on a volatile `isInitialized`, `:394-402`). `putIfAbsent` calls `ensureInitialized` **after** leaving `compute` (`:87`), so a thread that hits another thread's not-yet-initialised entry blocks on the monitor rather than reading half-built maps. Initialising also sets the global `NodeTaskInfo` flags `WAS_*_TWEAKED`/`TWEAKABLE` and assigns tweak masks (`:321-333`).
- Other routes to a cacheID:

| Constructor | cacheID | Line |
|---|---|---|
| `createChildInternal`, empty scenario + node inputs only | **parent's** (a "synthetic" level, skipped by the resolver) | `:512-514`, `:74-76` |
| `createChildInternal`, `Scenario.dontCache` | fresh `SSScenarioCacheID` (not interned) | `:524-528` |
| `createChildForTrackingScenario` | tracker's `MutableSSCacheID` (`tweakContainer.cacheId`) | `:545-562`, `tracking/DependencyTracker.scala:139-141` |
| `withOverlay(ref, overlaySS)` (TOSS) | interned `SSForwardingCacheID(overlaySS._cacheID)` keyed `TOSSCacheKey(parent.id, overlay.id)` | `:689-756` |
| `withTrackDependencies`, `withRecording` | `newUnique` ("do NOT share cache") | `:638-647`, `:665-675` |
| `withAuditorCallbacksDisabled` | `_cacheID.dup` unless `allowCachingWithAuditorCallbackDisabled` | `:629-636` |
| `asBasicScenarioStackWithParent` | `_cacheID.dup` at every tracking level | `:1188-1197` |
| `withRecorded` | **same** cacheID, but `_parent = root` | `:657-663` |
| `ScenarioStackMoniker.restoreOnEnv` | re-interned per level on the engine if `reuseCacheOnRemoteCalls` (default true) and not recording | `graph/ScenarioStackMoniker.scala:51-70` |
| all `with*` sibling copies (CS, plugin tags, batch scope, flags, siParams, cacheSelector) | **same** cacheID | `:600-1000` |
| `ssShared.siStack` | **same** cacheID as root | `graph/ScenarioStackShared.scala:151-160` |

### ssShared (`graph/ScenarioStackShared.scala:99-238`)

`_environment` (constructor), `scenarioStack` (root), `siStack` (root copy with `ALL_SI_FLAGS|NOT_TRANSPARENT_FOR_CACHING`, `_parent = root`), `siStackWithIgnoreSyncStack`, `siRootCache` (Caffeine weak-values map keyed by `SIParams`, used only if `reuseSIStacks`, default false), and `@volatile _scenarioStackWithInitialTime`. `environment` is the tracking accessor: it marks `accessedRuntimeEnv` on the current node (`:102-117`). `environmentWithoutTrackingAccess` is the escape hatch (`:131`).

### EvaluationState flags (`graph/EvaluationState.java`)

| Flag | Effect on caching / tweak resolution |
|---|---|
| `CACHED_TRANSITIVELY` 0x2 | "this stack sits under a cacheable node". `getNode` upgrades the stack with `withCacheableTransitively` for cacheable nodes (`ScenarioStack.scala:1126-1127`). Not in the cache key. `Scenario.dontCache`-like behaviour comes from clearing it (`withoutCacheableTransitively`, `:761-765`) |
| `SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR` 0x4 | an inherited marker. A tweakable that was never tweaked throws `IllegalScenarioDependenceException` immediately (`:1044-1055`) |
| `SCENARIO_INDEPENDENT_STACK` 0x8 | the SI barrier: `TwkResolver.next` throws on reaching it (`graph/TwkResolver.scala:199`), and `findInstanceTweak` stops silently (`:28`). Cleared for children (`CLEAR_FOR_CHILDREN`) |
| `RECORD_TWEAK_USAGE` 0x10 | XS recording stack. Changes TweakNode compute-SS wrapping (`graph/TweakNode.scala:122-134`) |
| `TRACK_TWEAK_USAGE_PER_NODE` 0x20 | tracker stack. Cannot be serialised (`ScenarioStackMoniker.scala:113`). `asBasicScenarioStack` strips it |
| `RECORD_WHEN_DEPENDENCIES` 0x40 | when-clause recording in XS (`TwkResolver.scala:100-104`, `160-162`) |
| `FAIL_ON_SYNC_STACKS`, `IGNORE_SYNC_STACKS` | scheduler diagnostics only |
| `IN_NON_CONCURRENT_SEQ`, `IN_WITHOUT_DAL_BLOCK` | set together with a plugin tag, so siParams change too |
| `CONSTANT` 0x200 | `getNode` throws (`:1116-1117`) |
| `AUDITOR_CALLBACKS_DISABLED` | also dups the cacheID (see the table above) |
| `NOT_TRANSPARENT_FOR_CACHING` (+`_INHERIT`) | XSFT search barrier. Set on the root and SI root, and on plugin tags that are `NotTransparentForCaching` plus their immediate child (`clearForChild`, `EvaluationState.java:90-93`) |
| `DISABLE_PICKLING_INTERNING` | read by `StatsBasedInterner` |

No flag is part of the cacheID. Flags change only who may resolve tweaks, and how.

### Stack kinds

| Kind | Built by | cacheID | Tweaks seen | May cache |
|---|---|---|---|---|
| root | `ScenarioStackShared.apply` | per env (interned) | none | yes (flagged NTFC: XSFT barrier) |
| SI root | `ssShared.siStack`, `asSiStack` | **same as root** | none; walking past it throws | yes (SI nodes use `UNodeCache.siGlobal`, `cache/CacheSelector.java:181`) |
| constant / constantNC | `createConstantStack` (`:78-83`) | `newUnique` | n/a: `getNode` throws | n/a; used for `tidyKey` and completed nodes |
| given child | `createChildInternal` | interned (parent id, expanded scenario) | own + parents | yes |
| synthetic child | node inputs only | parent's | skipped | yes |
| tracker (TS) | `createChildForTrackingScenario` | tracker's `MutableSSCacheID` (mutates) | own (mutable) + underlay + parents | yes if `cached`; `nc_scenarioStack` variant has CT cleared |
| TOSS overlay | `withOverlay(ref, ss)` | forwarding to overlay's (mutable) | via forwarding cacheID | yes; relies on invalidation |
| recording (XS) | `withRecording` | `newUnique` | via parent | own scoped cache if `scopeXSCache` (default true) |
| recorded (XS owner done) | `withRecorded` | same as recording | parent reset to root, so must not be used for lookups | — |
| track-deps | `withTrackDependencies` | `newUnique` | via parent | yes, unshared |
| auditor | `withAuditorCallbacksDisabled` | dup (unshared) | same | yes, unshared |
| basic copy | `asBasicScenarioStack` (moniker, snapshot) | dup at each tracking level | frozen copy of tracker tweaks | yes |

## 2. Tweak resolution path

From the plugin's `lookupAndGet` → `ScenarioStack.getNode(key, ec)` (`:1115-1146`):

1. `isConstant` → throw. Under `schedulerAsserts`, reject a `TweakNode` key, a re-used node, or a tweakable without `isTrackingValue`.
2. `ssAdjusted = info.adjustScenarioStack(key, this)` (`graph/NodeTaskInfo.java:878-898`): an SI property → `asSiStack`; `@givenRuntimeEnv` → initial-runtime stack plus the caller's CS/cache; otherwise `this`. Then `withCacheableTransitively` if cacheable and the stack is not yet CT.
3. If `info.isDirectlyTweakable` (`TWEAKABLE && !TWEAKHANDLER`, `NodeTaskInfo.java:1366-1372`): `requestingNode.markScenarioDependent()` unconditionally, then `getTweakResolver` (`:1044-1055`):
   - `!propertyInfo.wasTweakedAtLeastOnce` → null (fast path), or throw `IllegalScenarioDependenceException` if self or an ancestor is SI.
   - else → `new TwkResolver(requestingNode, key, startSS = this, trackWhenClause = true)`. Note that the resolver starts at **`this`**, not at `ssAdjusted`, so an SI property's tweaks are resolved in the caller's stack.
4. `TwkResolver.startAsyncResolve(ec, ss)` (`TwkResolver.scala:244-272`) walks `cur` up `parent` links (`next`, `:197-212`):
   - root → resolved (no tweak).
   - `cur.isScenarioIndependent` → throw ISDE.
   - synthetic level (`parent._cacheID eq _cacheID`) → skip (`:177`).
   - instance tweak: `info.wasTweakedByInstance && cacheID.mayContainTweak(info)` → `cacheID.get(key)`. This is a HashMap lookup using `PropertyNode.hashCode/equals` (entity `==`, `propertyInfo eq`, `argsEquals`) (`graph/PropertyNode.scala:300-330`).
   - property tweak on `info`, then on each `info.matchOn()` parent (`:184-193`): `cacheID.get(pinfo)` → `Tweak` or `TweakKeyExtractor`. An extractor computes its key under `asIfCalledFrom(ScenarioStack.nullNode)` (`:139-142`) and looks up `ExtractorTweakableKey`.
   - when-clause (`PredicatedPropertyTweakTarget`): `whenNode = target.whenClauseNode(key, cur.parent)`, re-homed into a recording or tracking variant for XS (`:157-169`). The sync path (`syncResolve`, or a non-FSM when node) does `ec.runAndWait(whenNode)`, a **sync stack**. The async path does `continueWithIfEverRuns` + `enqueueAttached` and returns the resolver itself as the node to wait on. `valueOfWhenClause` (`:74-108`) merges TPD/xinfo into the requester, the tracking proxy (under `trackingProxy.synchronized`), or a `WhenTargetTweakableListener`. A when clause that threw becomes a by-value failed tweak.
5. `getValueNode` (`:111-136`): no tweak → `completeGetNode` → `cacheSelector.lookupAndInsert` if cacheable and not stable. Tweak found → `tweak.createNode(startSS, cur, key)` → `TweakTemplate.createTweakNode`. A constant or exception template completes immediately. Otherwise `TweakNode.initialize` (`graph/TweakNode.scala:165-194`) decides by `evaluateIn`:
   - by-name: TweakNode attached to `givenSS`, RHS in `givenSS.parent.withSIParamsFrom(requestingSS)`.
   - bindOnce: attached to `givenSS`, RHS in `givenSS`.
   - bind: attached to and evaluated in `requestingSS`.
   - If the given and requesting listeners differ, the TweakNode is attached to `requestingSS`.

   An unstable, cacheable TweakNode is cached via `startSS.cacheSelector.lookupAndInsert(tweakInfo, tweakNode)` (`:126-133`).

Expansion happens once, at stack creation. `createChildInternal` → `TweakExpander.expandTweaks(scenario, this)` (`graph/TweakExpander.scala:40-46`) runs an `ExpandTweaksNode` (a hand-written `PropertyNode`, identity = scenario) **synchronously** (`.get`) under `evaluateIn.withIgnoreSyncStack`. That is a sync stack from inside whatever node ran `given`. Markers are resolved through `environmentWithoutTrackingAccess.entityResolver` (`:94`).

What is cached, and where:

| Step | Cached? | Shared state read | Lock |
|---|---|---|---|
| child cacheID | `CacheIDs.global` CHM | `Scenario.equals` → `Tweak.equals` → `TweakTemplate.equals` (user `equals` on captured values; **hydrates `TweakTemplateLazyVal`**) | CHM bin lock (`compute`) |
| SSCacheID tweak map | built once per cacheID | `SSScenarioCacheID.ensureInitialized` | `synchronized(cacheID)`, DCL on volatile |
| `twkHash` (XS) | lazily, once | `allTweaksAsIterable` | `synchronized(cacheID)` DCL; `MutableSSCacheID` nulls it **without** the lock (`CacheIDs.scala:445`) |
| global "was tweaked" / tweak masks | process-wide on `NodeTaskInfo` | volatile `_flags`, volatile `tweakMask` | `synchronized(NodeTaskInfo.class)` for masks and subclass marking |
| tweak lookup per level | not memoised (a walk every time; masks make misses cheap) | `cacheID.tweaks` plain HashMap, `tweakMask` plain mutable `TPDMask` | none |
| when-clause node | via `whenClauseNode` (plugin-generated; caching depends on it) | — | — |
| TweakNode | node cache, keyed at `givenSS`/`requestingSS` | — | ring 1 |
| expanded scenario | node cache (`TweakExpansion` info), keyed at the parent stack | — | ring 1 |
| CT-upgraded sibling | `_stableSS` | volatile | `synchronized(this ScenarioStack)` |

## 3. Shared mutable state in the ring

| State | Writer(s) and thread | Protection | Notes |
|---|---|---|---|
| `ScenarioStack._parent` (`:145`) | `UnderlayScenarioSupport.doSetUnderlay` (tracker update) | none, not volatile | Breaks immutability: S6 |
| `ScenarioStack._stableSS` (`:165`) | `withCacheableTransitively`, any thread | volatile + `this.synchronized` | Lock is on the stack object; nothing else in the ring syncs on stacks (grep), so no nesting found. Correctness: S4 |
| `ScenarioStack._hashCode` | any reader | racy idempotent memo | Includes `_parent.hashCode`, so it goes stale after `setParent` |
| `ScenarioStack.pss` | profiler | none | diagnostics only |
| `ScenarioStack.flags` | constructor only | n/a | var only for `|=` in the constructor |
| `ScenarioStackShared._scenarioStackWithInitialTime` | `scenarioStackWithInitialTime` | volatile + `synchronized(ssShared)` **without a re-check** | S7. Holds the monitor while calling `createChild` (CHM bin lock, maybe tweak expansion → graph evaluation) |
| `ssShared.scenarioStack/siStack/siStackWithIgnoreSyncStack/siRootCache` | `setScenarioStack`/`copy` before publication | publication via node attach | fine |
| `ssShared.siRootCache` | `asSiStack` (only if `reuseSIStacks`) | Caffeine weak values | — |
| `CacheIDs.global` | `putIfAbsent`, poll-cleanup, `debugClear` | CHM | Keys and equality run arbitrary user `equals` under the bin lock |
| `SSCacheKey.cacheIDRef/_scenario/cacheIDID` | inside `compute` | bin lock; `cacheIDID` volatile | fine |
| `SSCacheID.tweaks/tweakMask` (immutable kinds) | `ensureInitialized` | monitor + volatile flag | fine |
| `MutableSSCacheID.tweaks/tweakMask/modifyOnWrite/twkHash` | tracker update (`putAll`, `removeTweaks`, `clearTweaks`) | **none**. The class comment says "dup is called during an evaluate cycle … never runs concurrently with an invalidate cycle" (`CacheIDs.scala:407-412`) | Readers are every `TwkResolver` walking a tracker stack, plus `dup`. S8 |
| `SSForwardingCacheID` | none (forwards to a mutable one) | — | S10 |
| `MutableNodeInputMap.tagsThisLevelOnly`, `cache` (`ScenarioStack.scala:1417-1475`) | `DependencyTracker.setPluginTag`/`clearPluginTag` (public API, any thread) and `DependencyTrackerRoot` constructor | `cache` volatile; `freeze` DCL under `this`; **writes unsynchronized** | S2 |
| `SIParams.hashCodeCache` | any | racy idempotent memo | `scheduler`/`pathMask` are in `equals` but not in the hash (fine) |
| `Scenario._hashCode`, `_withoutNested`, `_lastEqualPartner` | any | racy memos; LEP is a `WeakReference` | `_withoutNested` race only costs identity, i.e. a slower `equals` |
| `Tweak._hash` | any | racy memo | — |
| `NodeTaskInfo._flags`, `tweakMask` | `SSCacheID.put` from any thread | volatile CAS / class lock | monotonic |
| `CancellationScope.children/listeners` | `childScope`, `union`, cancel | `this.synchronized` | `union` may add a child to two parents |
| `UntweakedScenarioState.scenarioStack` lazy val | first use | lazy-val monitor on the state | `ScenarioState.minimal` is a global singleton; the init only touches the CHM, so no nesting found |
| `RuntimeEnvironment.id` lazy val | first use | lazy-val monitor | trivial |
| `EvaluationContext.lastDefaultScenarioStack` | `initCoreUnsafe` | none | debugger only |

Lock nesting found:

- `ssShared` monitor → CHM bin lock (`scenarioStackWithInitialTime` → `createChild` → `putIfAbsent`).
- CHM bin lock → `TweakTemplateLazyVal.provider.value` and user `equals` (via `Scenario.equals`).
- `synchronized(cacheID)` in `ensureInitialized` → `NodeTaskInfo.class` (mark tweaked / masks).

I found no cycle between these. The CHM-bin → user-code edge is the open-ended one (S11).

## 4. Invariants

| Invariant | Assumed where | Holds? |
|---|---|---|
| I1. A stack's `_cacheID` reference never changes | everywhere | **Yes** (val). But the *contents* change for `MutableSSCacheID` and anything forwarding to it, by design. Cache coherence there rests on tracker invalidation, not on identity |
| I2. Equal cacheID ⇒ every tweak resolves identically | `PropertyNode` cache key = (entity, args, info, cacheID); TweakNode caching | **Mostly; violated by:** (a) `Tweak.equals` ignoring `evaluateIn`/`readsTweakables`/`flags` (S5); (b) SI and non-SI stacks sharing the root cacheID, where the walk *throws* in one and succeeds in the other (S1); (c) tracker/TOSS stacks with a live `_parent` (S6) and mutable maps (resolution may legitimately differ over time; invalidation must cover it); (d) `withRecorded` keeping the cacheID but cutting the parents (documented: only proxies see that stack) |
| I3. Ancestors with equal cacheIDs resolve identically, so a child keyed on (parent id, scenario) is sound | `createChildInternal` | Holds for interned chains. Tracker levels are keyed by the tracker's constant id while their contents change (`TOSSCacheKey` comment, `:691-693`) |
| I4. A synthetic level (cacheID eq parent's) carries no tweaks of its own | `TwkResolver.nextInCurrent` `:174-177` | Holds where created; broken by `asBasicScenarioStack`, which dups each tracking level separately (S9) |
| I5. SI stacks see no tweaks above them; reading a tweakable from an SI context throws | `getTweakResolver`, `TwkResolver.next` | Holds for the walk; `findInstanceTweak` stops silently instead of throwing (`TwkResolver.scala:28`). The thrown ISDE is RT and is cached (S1) |
| I6. `tweakableListener` is fixed for the stack's lifetime | `ScenarioStack.equals` comment, TweakNode `initialize` | Holds (val) |
| I7. A stack's flags are fixed | readers | Holds (only the constructor writes). But flags are **not** reliably inherited across `withCacheableTransitively` (S4) or `asSiStack` (S3) |
| I8. `NodeTaskInfo.wasTweaked*` and the mask are visible before any lookup on a stack holding the tweak | `getTweakResolver` fast path, `mayContainTweak` | Holds for interned/immutable cacheIDs (set before the volatile `isInitialized` write, and stacks reach other threads after that). For `MutableSSCacheID` it relies on tracker-queue happens-before |
| I9. No tweak lookup on a stack while its tracker mutates it | `MutableSSCacheID` class comment | Assumed, not enforced here. Ring 4 must confirm the queue gives mutual exclusion **and** happens-before for every evaluator thread, including child trackers that walk through it |
| I10. Plugin tags / node inputs never affect a cached value without being tracked | they are excluded from the cacheID (`SIParams` doc, `:27-28`) | Assumption on user code and plugins; nothing in the ring checks it |
| I11. A key's `hashCode/equals` are stable and value-based | `SSCacheID.tweaks` HashMap, `CacheIDs` | Assumed; never checked |

What the ring provides to ring 1 (cache): `_cacheID` identity, `cancelScopeAndEnvId` (computed once in the constructor), `cacheSelector`, `cachedTransitively`, and the guarantee that `getNode` hands over a key already attached to the CT-upgraded, adjusted stack.

What it provides to ring 4 (tracker): `tweakableListener` (fixed per stack), `trackingDepth` ordering, `combineTrackData`/`combineWhenClauseData` reporting walks (`:1206-1277`), and `setParent` as the underlay hook.

## 5. Suspects and properties

Grades: H likely to bite; M real, needs specific conditions; L hygiene or latent.

### S1 (M, wrong result, probe-ready) An `IllegalScenarioDependenceException` raised under an SI stack is cached under the **root** cacheID and served to legal non-SI callers

- `siStack` is `root.copy(...)` with the same `_cacheID` (`ScenarioStackShared.scala:153-158`), and `asSiStack` keeps it (`ScenarioStack.scala:224-236`). A non-SI node N called from an SI node runs on the SI stack (`adjustScenarioStack` returns `requestingSS` for non-SI info). Its cache key is therefore identical to N requested from the plain root.
- If N reads a tweakable, `getTweakResolver` throws (never tweaked, `:1050-1054`) or `TwkResolver.next` throws (`TwkResolver.scala:199`). `IllegalScenarioDependenceException` is an `RTExceptionTrait` (`graph/GraphExceptions.scala:138-140`), so the failed N is cacheable and `isDoneWithUsableResult`.
- The same holds for `given(S)` inside an SI node: the child key is `SSCacheKey(root.id, S)`, the same as top-level `given(S)`.
- Interleaving: SI node M calls `e.n` (non-SI, reads tweakable `e.p`). It fails with ISDE and is cached. Later, top-level code under the root (or under `given(S)` in the second variant) asks for `e.n` and gets a hit with ISDE, in a context where reading `e.p` is legal.
- The reverse order masks the SI violation: M gets N's value without error. Values agree because neither path sees tweaks above the root, so that direction is "error not raised", not a wrong value.
- I found no SI check in `graph/cache` (grep). Ring 1 should confirm the key really is (entity, args, info, cacheID) with nothing SI-related.
- Probe: SI node → non-SI cacheable node → tweakable property that is tweaked elsewhere; then call the non-SI node from root.

### S2 (M, race → stale plugin tags, lost invalidation) `MutableNodeInputMap.freeze` can republish a stale cache after `addInput`/`removeInput`

- `ScenarioStack.scala:1425-1475`. `freeze` does a DCL: inside `synchronized(this)` it reads `tagsThisLevelOnly`, builds `n`, then writes `cache = n`. `addInput` and `removeInput` mutate `tagsThisLevelOnly` and set `cache = null` **without** taking the monitor.
- Interleaving:
  1. T1 (evaluator): `freeze` sees `cache == null`, enters the lock, reads the old map, builds `n_old`.
  2. T2 (`DependencyTracker.setPluginTag`, a public non-queued API, `tracking/PluginTagSupport.scala:18-29`): `tagsThisLevelOnly += tag`; `cache = null`; children invalidated.
  3. T1: `cache = n_old`.
- Every later `findPluginTag`/`freeze` on the tracker stack returns the old tags until the next invalidation. The same pattern applies one level down: a child's `freeze` calls `parentTags.freeze` and can cache the parent's stale result after the child was invalidated.
- Fix shape: do the write and the `cache = null` under the same monitor, or version-stamp.

### S2b (M, foot-gun) `MutableNodeInputMap.contains` looks at this level only, while `FrozenNodeInputMap.contains` sees the merged set

- `ScenarioStack.scala:1423-1424` vs `platform/inputs/loaders/NodeInputMap.scala:277`.
- On a tracker stack (whose `siParams.nodeInputs` is the mutable map), `hasPluginTag(k)` is false for a tag inherited from a parent tracker or the root, while `findPluginTag(k)` is `Some`.
- `withoutPluginTag` (`:943`) then returns `this`, leaving the inherited tag in place.
- `SIParams.resolvePlugin` (`graph/SIParams.scala:52-62`) ignores inherited `enablePluginTagKey`s, so scoped plugins silently do not apply to nodes run directly on a tracker stack.

### S3 (M, foot-gun with behavioural effect) `equivSIParams` tests disjointness of flags, not equality, so `asSiStack` drops propagatable flags

- `ScenarioStack.scala:67-71`: `(lhs.flags & rhs.flags & ~ALL_SI_FLAGS) == 0`. With `rhs = siRoot` (flags `SI | NTFC`) this reduces to "lhs lacks `NOT_TRANSPARENT_FOR_CACHING`", which holds for almost every non-root stack.
- When `siParams` also match, `asSiStack` returns `siRoot` and discards `flagsToPropagate` (`IGNORE_SYNC_STACKS`, `AUDITOR_CALLBACKS_DISABLED`, `FAIL_ON_SYNC_STACKS`, `DISABLE_PICKLING_INTERNING`, CT). The non-equivalent branch propagates them (`:229-233`).
- Concrete case: `withAuditorCallbacksDisabled` changes only flags and cacheID, not siParams. An SI node called by the auditor therefore runs **with auditor callbacks enabled**, the self-audit the flag exists to prevent.
- `AdvancedUtils.disableFeatures` (`platform/AdvancedUtils.scala:1355-1371`) uses `withFlag` only, so SI nodes inside it lose both pickling-interning suppression and sync-stack suppression.
- The intended test is probably `((lhs.flags ^ rhs.flags) & ~ALL_SI_FLAGS) == 0`; that is my reading, not confirmed with authors.

### S4 (M, foot-gun with behavioural effect) `withCacheableTransitively` can return a sibling with **different flags or listener**, depending on history

- `ScenarioStack.scala:771-790`. `canUseCached` checks only `cachedTransitively`, `siParams eq`, and `cacheSelector eq` on `_stableSS`. `copy(...)` carries `_stableSS` unless it is explicitly nulled, and `withFlag` (`:607`), `withoutWhenTarget` (`:1199-1202`) and `withNonScenarioPropertiesFrom` (when siParams are eq) do not null it.
- Sequence:
  1. A (not CT) is upgraded; `A._stableSS = A'`.
  2. `B = A.withFlag(IGNORE_SYNC_STACKS)`; `B._stableSS = A'`.
  3. `B.withCacheableTransitively` returns `A'`, which lacks the flag.
- If A had not been upgraded before B was copied, B's upgrade keeps the flag. Behaviour therefore depends on history.
- Hits:
  - `TweakExpander.expandTweaks`: `evaluateIn.withIgnoreSyncStack.getNode(node)` (`graph/TweakExpander.scala:45`) on a non-CT parent loses the very flag it sets.
  - `disableFeatures(disablePicklingInterning = true)` inside a non-cacheable node: cacheable callees lose `DISABLE_PICKLING_INTERNING` (a TODO-tracked workaround).
  - `FullySpecifiedScenarioState` (`platform/ScenarioState.scala:40-42`): `withoutWhenTarget` keeps a `_stableSS` that may still carry the `WhenTargetTweakableListener`.
- Fix shape: null `_stableSS` in `withFlag`/`withoutWhenTarget`, or compare flags and listener in `canUseCached`.

### S5 (L–M, wrong result in narrow cases) `Tweak.equals` ignores `evaluateIn` (bind / bindOnce / by-name), `readsTweakables` and `flags`, so equal scenarios share a cacheID while resolving differently

- `platform/Tweak.scala:142-147`. `SSCacheKey` equality is `Scenario.equals` → `Tweak.equals`. The first creator's `SSScenarioCacheID` holds *its* tweak instances; later stacks with an "equal" scenario resolve through those (`TwkResolver` reads `cacheID.get`; `TweakNode.initialize` switches on `tweak.evaluateIn`).
- Input: under the same parent, `given(Tweaks.bind(t))` then `given(Tweaks.bindOnce(t))` (or `given(t)` with `keepLazyEvaluation`, a `TweakTemplateByKey` RHS, or `convertByNameToByValue` off). The second given silently gets the first's binding: the RHS is evaluated in the requesting stack vs the given stack vs the parent of the given.
- By default, by-name `TweakTemplateByNode` tweaks are reduced to by-value at `given` entry (`EvaluationContext.scala:281-282`), which removes the common collision.
- The same applies to `onlyReads` (`TweakNode.onChildCompleted`'s mask check uses the cached tweak's `readsTweakables`) and `alwaysInvalidateTracking`.

### S6 (M, needs ring-4 confirmation) `setParent` mutates a published stack; every `copy` of a tracker stack keeps the old underlay

- `ScenarioStack.scala:1148-1150`, `tracking/UnderlayScenarioSupport.scala:31-38`. Only `scenarioStack` and `nc_scenarioStack` are re-pointed.
- Siblings made by `copy(...)` keep the old `_parent`. These include per-action `modifySS`/`withCancellationScope` copies (`ScenarioStack.scala:94-121`), `withPluginTag` (e.g. `OGSchedulerLostConcurrency.java:185`), `withTrackingNode`, `withBatchScope`, and the snapshot's cached `_asBasicScenarioStack` (`tracking/SnapshotSupport.scala:38-58`).
- Any such copy used after `doSetUnderlay` resolves tweaks against the **old** underlay. Children of the live tracker stack are fine: their `_parent` is the stack object itself.
- `_parent` is not volatile, so an evaluator on another thread has no happens-before with the write unless the tracker queue provides it.
- `_hashCode` memoises `_parent.hashCode` and goes stale.
- Question for ring 4: does any copy of a tracker stack outlive an underlay update (queued evaluate actions, the snapshot cache, `scenarioState`)?

### S7 (L–M, race) `ScenarioStackShared.scenarioStackWithInitialTime` is a DCL with no re-check

- `graph/ScenarioStackShared.scala:190-204`.
- Interleaving:
  1. Two threads see `_scenarioStackWithInitialTime == null`.
  2. Both enter the monitor in turn.
  3. Each calls `config.newTopScenario` (a fresh initial-time tweak) and `createChild`.
  4. The second overwrites the field.
- The callers get stacks with different initial times. Later `initTimePresentWithCheck(S_A.topScenario)` (`:163-183`) compares against `S_B`, sends `sendEvilScenarioStackInitTimeRandom`, or throws if `throwOnInvalidInitRuntimeEnvUsage`.
- The monitor is also held across `createChild` (CHM, possible tweak expansion).

### S8 (latent; M if ring 4 refutes I9) `MutableSSCacheID` is read lock-free while being mutated

- `putAll`/`put` mutate a `JHashMap` and `merge` into a published `TPDMask` (two longs).
- `removeTweaks` replaces `tweakMask` with a fresh mask and then merges into it after publishing it (`recomputeTweakMask`, `CacheIDs.scala:361-375`). A concurrent `mayContainTweak` can see an empty or partial mask and **skip an existing tweak**.
- `copyOnWrite` nulls `twkHash` outside the lock that builds it, so a concurrent `mayContainTweakAffecting` can store a twkHash built from the old map *after* the null, giving permanent XS false negatives for that cacheID (`:283-298` vs `:445`).
- `modifyOnWrite` is a plain field written by `dup` on an evaluator thread and read by the tracker thread.
- All of this is safe only under I9.

### S9 (L) `asBasicScenarioStack` dups each tracking level independently

- It turns a synthetic (cacheID-sharing) tracking child into a level that *contains* its parent's tweaks (`ScenarioStack.scala:1177-1197`, `CacheIDs.scala:339-343`).
- A by-name `x := x + 1` would then resolve in the child and re-resolve in the parent (the very case `TwkResolver.scala:174-176` guards against).
- This only matters if a tracking level ever shares its parent's cacheID. I did not find a constructor that does, so it is latent.
- `recomputeTweakMask` also only recognises `NodeKey` and `PropertyInfo` keys (`:366-372`). A property tweak whose info is a bare `NodeTaskInfo` would vanish from the mask after a removal. I found no such tweakable, so this is latent too.

### S10 (L) `SSForwardingCacheID` is incomplete

- It sets `tweaks = null` (`CacheIDs.scala:490`) but does not override `toScenario`/`inputScenario`, `size`, `tweakMaskString`, `allInstanceTweaksInfos`, `allPropertyTweaksInfos`, or `mayContainTweak(mask: TPDMask)` (the latter is flagged by its own comment at `:275`).
- `nestScenariosUpTo` (`ScenarioStack.scala:1307-1315`) and `buildSSCacheIDArray` (`ScenarioStackMoniker.scala:148`) would NPE on a TOSS level.
- `mayContainTweak(mask)` returns false for any TOSS, a false negative for XSFT if TOSS ever reaches it.

### S11 (L, foot-gun) User code runs under a CHM bin lock

- `CacheIDs.putIfAbsent` → `compute` → `SSCacheKey.equals` → `Scenario.equals` → `TweakTemplate.equals`, which is `NodeClsIDSupport.equals` on captured values, and `TweakTemplateLazyVal.equals` → `provider.value` (`graph/TweakTemplate.scala:140-143`).
- A slow `equals`, or a lazy-val hydration that does I/O, stalls every `createChild` hashing to that bin.
- A user `equals` that creates a scenario stack re-enters `compute` on the same map. CHM throws "Recursive update" for the same bin and can deadlock across bins.
- `TweakTemplateLazyVal.hashCode == 1` makes every such tweak hash alike (the initial-time stack).

### S12 (M, wrong result, out-of-ring caller) `Scenario.nestWithFlatten` drops `s.nestedScenarios` and inverts priority when `this` is already nested

- `platform/Scenario.scala:236-271`, flatten branch: `new Scenario(combined, nestedScenarios, flags)`.
- (a) `s.nestedScenarios` is discarded.
- (b) `s`'s tweaks are merged into `this`'s **top** level, under `this.nestedScenarios`, so a nested scenario's tweak to the same target now beats `s`'s.
- (c) Only `this`'s flags are kept.
- Caller: `platform/.../graph/JobTweakExtractor.scala:39` folds a list. After one conflict forces nesting, the next non-conflicting scenario is flattened beneath the nested one.
- Example: `S1 = (a := 1)`, `S2 = (b := a+1 onlyReads a)` (conflict, so nested), `S3 = (b := 5)`. The fold yields top `{a:=1, b:=5}`, nested `[S2]`, so `b = 2`. Proper nesting gives `5`.

### S13 (L, perf / sync stacks) Sync waits inside scenario-stack creation and tweak lookup

- `TweakExpander.expandTweaks` uses `.get` (`TweakExpander.scala:45`). `getTweakAndScenarioStackNoWhenClauseTracking` and `findParentByScenarioReferenceId` use `syncResolve` → `ec.runAndWait(whenNode)` (`TwkResolver.scala:205-206`, `214-220`).
- Every `given` with also-set or marker tweaks, every when clause on a non-FSM node, and XS `hasTweaksForTweakables` therefore create sync stacks, the scheduler review's R9 territory.

### S14 (L) Smaller items

- `ScenarioStack.equals` NPEs comparing `constant` with `constantNC` (`ssShared` null, `:275`).
- Stale `RuntimeEnvironmentCacheKey` entries keep a disposed env (and its `EntityResolver`) reachable until some later `putIfAbsent` polls (`CacheIDs.scala:79-83`).
- `ExpandTweaksNode`'s impure-retry path calls `invalidateCache` on itself while running (`TweakExpander.scala:72-76`), the R6/R8 state-word territory.
- `TwkResolver.findInstanceTweak` stops at SI silently while the main path throws (`TwkResolver.scala:28`).
- `createChildInternal` computes `tl` (the when-target listener) from the **unexpanded** scenario's `hasWhenPredicatedTweaks`. The expanded scenario keeps the input's flags (`TweakExpander.scala:97`, `flagsWithoutUnresolved`), so predicated tweaks produced by expansion are not flagged.

### Properties that hold (with the reason)

- **P1. Interned child cacheIDs are race-free.** `CacheIDs.putIfAbsent` uses one `compute` per key, so exactly one SSCacheID wins. Initialisation happens after `compute` under the cacheID monitor with a volatile flag, so losers block until the winner's maps are complete (`CacheIDs.scala:55-89`, `394-402`). Weak values plus poll-cleanup bound growth. Do not break: keep `ensureInitialized` outside `compute`, and keep it before the cacheID is used.
- **P2. Global "was tweaked" flags are published before use.** `put` marks `NodeTaskInfo` (volatile CAS) before the volatile `isInitialized` write. `markPropertyTweaked` walks subclasses under the class lock, with `internalSetOn` handling late-loaded subclasses (`NodeTaskInfo.java:1257-1320`). So the `wasTweakedAtLeastOnce` fast path in `getTweakResolver` cannot miss a tweak on an interned cacheID.
- **P3. Masks only ever cause false positives for immutable cacheIDs.** `put` merges `info.tweakMask` and `TPDMask.intersects` is conservative; masks shared across `_matchOn` parents are documented (`NodeTaskInfo.java:603-645`).
- **P4. Sibling copies keep the cacheID, so cancel scopes, plugin tags, progress and batch scopes do not fragment the cache.** Reuse across cancel scopes and environments is mediated in one place (`NCSupport.isUsableWRTCancelScopeAndEnv`) by a precomputed packed id.
- **P5. Synthetic stacks** (node-input-only children share the parent's cacheID) **cannot double-apply by-name tweaks**, because the resolver skips them (`TwkResolver.scala:174-177`). Holds for every constructor I found (but see S9).
- **P6. Expansion is keyed on the result, not the input.** The child cacheID uses the *expanded* scenario, and the expansion is itself a cached node keyed by the parent stack. Two also-sets that expand to the same tweaks share a cacheID, and differently-expanding contexts do not.
- **P7. Unordered scenarios reject conflicting duplicates** (`Scenario.validate`, `Scenario.scala:467-484`). So the set-equality used for unordered scenarios cannot equate two scenarios whose "last wins" maps differ, with one exception: `resolveDupsByTakingLast` (`dupAllowed`) tweaks under `unordered`, which I did not chase.
- **P8. `Scenario._lastEqualPartner`** is a racy cache whose only failure mode is a null referent, which falls back to full comparison; there is no out-of-thin-air "equal". This rests on tweaks being immutable and on `TweakTemplate` equality being stable, so a `TweakTemplateByNode` capturing a mutable value would break it, and the cache is off by default (`cacheScenarioEqualities=false`).
- **P9. The TweakNode cache location is principled.** By-name and bindOnce tweak nodes attach to `givenSS` (XSFT-like reuse across inner givens). Bind nodes and listener mismatches attach to the requester (`TweakNode.scala:165-194`). A tracking or recording requester therefore never shares a TweakNode with an untracked given.

## 6. The plugin contract, from this ring's side

A hand-written `PropertyNode[T]` participates in tweak resolution if:

- `propertyInfo` is a `NodeTaskInfo`/`PropertyInfo` with `TWEAKABLE` set and `TWEAKHANDLER` clear (`isDirectlyTweakable`). Otherwise `getNode` never builds a resolver and **tweaks to it are silently ignored**. An instance tweak's construction checks `isTweakable` (`TweakTargetImpl.scala:33-35`); a property tweak uses the info itself (`TwkPropertyInfo0/1/...`, `PropertyTarget`, `PropertyInfo.scala:353+`).
  - A node whose info gets `TWEAKABLE` *after* a resolution miss is fine: `wasTweakedAtLeastOnce` is consulted every time.
  - `getCacheable` decides `completeGetNode` caching; `isScenarioIndependent` routes to `asSiStack`; `isGivenRuntimeEnv` routes to the initial-runtime stack.
- `entity`, `argsHash` and `argsEquals` define instance-tweak identity. `PropertyNode.equals` = `propertyInfo eq` + `entity ==` + `argsEquals` (`PropertyNode.scala:300-306`). This is **entity `==`**, while caching uses `equalsForCachingInternal` (`:318-324`). An entity class where the two disagree gets tweaks applied to entities the cache treats as distinct; that is by design, but it is a contract. `argsEquals` may assume the same node class. `hashCode` must be consistent with `argsHash` (`$hashCodeArgsAndInfo`).
  - Mutable args or entity hash change the tweak map silently: `SSCacheID.tweaks` is a plain `HashMap`, nothing detects it, and the result is a missed tweak.
  - An instance tweak needs `entity ne null` (`TweakTargetImpl.scala:33`). A null-entity node can only be property-tweaked.
- `isTrackingValue` (from `initTrackingValue`): asserted only under `schedulerAsserts` (`ScenarioStack.scala:1123-1124`).
- The key must be fresh: `info() eq null` and not a `TweakNode`, asserted only under `schedulerAsserts`. Otherwise `prepareForExecutionIn` clones or attaches.
- `argsCopy(generator)` is needed for `TweakTemplateByKey` and when clauses (`TweakTemplate.scala:160`). The default handles only zero-argument properties and casts the generator to `Entity => Node`, so a hand-written node with arguments that is tweaked by key or `when` gets a `ClassCastException` at resolution time.
- `transformTweak(value)` is required iff `hasTweakHandler`. Otherwise `UnsupportedOperationException` at expansion.
- `tidyKey` attaches to `ScenarioStack.constant`. `trackKey` uses `NodeHash` if `trackTweakableByHash`.

Detected:

- unknown or conflicting tweaks within one scenario (`Scenario.validate`, property-level always; instance-level only warns unless `throwOnDuplicate`);
- an invalid instance-tweak key (null entity, not tweakable);
- a tweak handler on a `PropertyTarget`;
- SI violations (throw; see S1 for what happens next);
- an RHS reading outside `onlyReads` (`TweakNode.scala:78-83`);
- the asserts above, but only with `schedulerAsserts`.

Silently mishandled:

- missing `TWEAKABLE` (tweak ignored);
- unstable `hashCode`/`equals`/mutable args (missed tweak);
- by-key or `when` tweaks on multi-argument hand-written nodes without `argsCopy`;
- an `equals` that differs between `==` and `equalsForCachingInternal`;
- a cacheable node reading node inputs or plugin tags (not in the cacheID; I10);
- a node reading `ssShared.environmentWithoutTrackingAccess` for anything value-affecting.

## 7. Testability

Reachable from a probe in `optimus.graph` or `optimus.platform` without the DAL or the entity plugin:

- `EvaluationContext.initializeWithoutRuntime()` (public, `EvaluationContext.scala:110-111`) uses `ScenarioState.minimal` → `RuntimeEnvironment.minimal` (config and resolver null). The probes already use it (`probes/src/optimus/graph/Probes.java:354`). Avoid `initialRuntimeScenarioStack`, `scenarioStackWithInitialTime` and `@givenRuntimeEnv`: they dereference `config`.
- `ScenarioStack.newRoot(env, uniqueID = true)` (public) gives an isolated root cacheID per test, which avoids cross-test interning through `RuntimeEnvironmentCacheKey(minimal)`. `CacheIDs.debugClear()` is a global reset. `ScenarioStack.constant`/`constantNC` are public vals.
- `ScenarioStack(...)` constructor, `initChild`, `createChildInternal`, `withFlag`, `withCacheableTransitively`, `withAuditorCallbacksDisabled`, `asSiStack`, `setParent`, `asBasicScenarioStack` and `getTweakAndScenarioStackNoWhenClauseTracking` are all `private[optimus]` or public. `createChild(scenario, marker)` is public.
- `SSCacheID.newUnique/newUninitialized/newForwarding` and `MutableSSCacheID` are `private[optimus]`/`final private[optimus]`. `SSScenarioCacheID` is `private` to package `optimus.graph`, so it is reachable only via `newUninitialized`. `ScenarioStackShared.apply` is `private[optimus]`.
- `Scenario(tweaks: Tweak*)` and `Scenario.unordered` are public (annotations only steer the plugin). `new Scenario(...)` is `private[optimus]`.
- `new Tweak(target, TweakTemplate(v))` is `private[optimus]`. `TweakTemplate.apply(any)` is public (`TweakTemplate.scala:246`): a plain value gives a constant template, and a `Node` (by-name) gives the by-node template. `Tweak.bind(evaluateIn)` is `private[optimus]`, and `Tweaks.bind/bindOnce` are public. `SimpleValueTweak(nodeKey)(value)` is public.
- Property-level tweaks need no entity: `new TwkPropertyInfo0[E <: Storable, R](name, flags)` with `NodeTaskInfo.TWEAKABLE` (plus `DONT_CACHE` or not), a hand-written `PropertyNode` with `entity = null`, `propertyInfo = thatInfo`, and `run` completing. `TwkPropertyInfo0` is itself a `PropertyTarget`, so `info := value` builds a tweak directly (`:=` is a plugin-lifted by-name method, so in Java/plugin-less Scala construct `new PropertyTweak(info, TweakTemplate(v))` or `new Tweak(info, TweakTemplate(v))`).
- Instance tweaks need a non-null `Entity`. `EntityImpl` is abstract and needs an `EntityInfo`. The cheapest route is a probe class in package `optimus.platform.storable` implementing `Entity` directly (the abstract `entityFlavorInternal` is `private[storable]`; mimic `intellij/only/DoNotUseIJCompileTimeEntityInjectedImplementation.scala` but with real `equals`/`hashCode`). I have not tried this.
- An SI context for S1: a `NodeTaskInfo` with the SI flag on a hand-written node, or `ss.asSiStack` plus `EvaluationContext.given(ss, node)`.

Probe suggestions, in order of value:

1. **S1:** SI-poisoned cache.
2. **S3/S4:** flag loss. Assert `ss.withFlag(X).withCacheableTransitively.hasFlag(X)` after upgrading `ss` first, and `ss.withAuditorCallbacksDisabled.asSiStack.auditorCallbacksDisabled`. These are pure, deterministic unit checks.
3. **S5:** `bind` vs `bindOnce` of the same tweak under one parent.
4. **S2:** `MutableNodeInputMap` lost update. Hook between "build n" and "cache = n", or brute-force with jcstress.
5. **S7:** the initial-time DCL (needs a config stub).
6. **S12:** `nestWithFlatten` (pure).

## Not covered (budget)

- `CacheSelector` and private or scoped caches beyond their flags.
- `RecordingScenario` / `RecordingTweakableListener` / `WhenNodeRecordingTweakableListener`: only the parts called from the ring.
- `ConvertByNameToByValueNode` and `removeRedundantTweaks`: only the entry conditions.
- `CancellationScope` beyond `union`/`childScope` and its id packing.
- `TemporalContext`: it does not key `ScenarioStack` directly. It enters only via initial-time tweaks (`TweakTemplateLazyVal`) and `TweakExpander.processMarkers`.
- `ScenarioReference`/`SnapshotScenarioStack` overlay mechanics beyond `withOverlay`.
- Deserialisation (`readResolve` paths) beyond `ScenarioStackMoniker.restoreOnEnv`.
- Whether `JobTweakExtractor` inputs can contain nested scenarios (S12a).
- S6 and S8 hinge on ring 4's queue discipline; I did not read `DependencyTrackerQueue`.
