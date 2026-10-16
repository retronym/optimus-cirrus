# Cache campaign P0 fixtures

Shared fixtures for the Phase 2 probes in [CACHE_CAMPAIGN.md](../../CACHE_CAMPAIGN.md). Scala for everything that touches Scala-only API (default args, by-name, `private[tracking]`), Java for the model/oracles and the agent.

```
probes/cache/build.sh                      # scalac (mixed, all .scala + .java on the source list) then javac; builds classes/cacheagent.jar
probes/cache/run.sh <main-class> [args]    # entity agent + CacheDelayAgent + the usual --add-exports/opens
probes/cache/run.sh optimus.graph.CacheFixturesSmoke [nodes entities scenarios tracker priming child hooks]
```

Env: `JVM_EXTRA` (extra JVM flags), `CACHE_AGENT=0` (no delay agent), `CACHE_EXCLUDE="Foo.java Bar.scala"` (leave sources out of the build), `CIRRUS_BUILD`, `CACHE_CLASSES`. `run.sh` exports `CACHE_JAVA_CMD` for child JVMs.

| File | What |
|---|---|
| `src/optimus/graph/ProbeDag.java` | The probe's own model (props, keys, tweaks, levels, stacks) and both oracles |
| `src/optimus/graph/CacheFixtures.scala` | Nodes, entities, `ProbeSystem` (model to runtime), builders, evaluation, priming, child JVM |
| `src/optimus/graph/tracking/CacheTrackerAccess.scala` | Tracker driver and internals (`private[tracking]` + reflection) |
| `src/probes/CacheDelayAgent.java`, `CacheDelay.java` | ASM hooks and their runtime (counting, cross-thread handlers, forwarding to `probes.Delay`) |
| `src/optimus/graph/CacheFixturesSmoke.scala` | Smoke main: exercises every fixture against the oracles |

## Model (`ProbeDag`)

A key is `(Prop, entity, arg)`: entity `MODULE` (-1) is the shared `ProbeModule` (a `FakeModuleEntity`), entity `k >= 0` is `ProbeEntity(k)`. Values are longs.

- `dag.prop(name)` returns a `Prop` with fluent setters: `.base(n)`, `.reads(p)` (same entity/arg) or `.reads(p, entity, arg)`, `.notTweakable()`, `.dontCache()`, `.si()`, `.kind(TAG_READER | ENV_READER)`, `.badKey(MUTABLE_ARG | ASYMMETRIC_EQUALS | UNSTABLE_HASH)`, `.combine((key, childValues) -> long)`. Public fields `atMostOneWaiter`, `localCache`, `dontTrack`, `extraFlags`, `mutableArg[0]`. Default: tweakable, cacheable, `combine = base + arg + sum(children)`.
- `Ref(prop, entity, arg)` is a declared read; `SAME` (-2) for entity/arg means the reader's own.
- `Tw.instance(key, v)`, `Tw.instance(key, rhs, mode)`, `Tw.property(prop, v)`, `Tw.property(prop, rhs, mode)`, `Tw.propertyWhen(prop, when, v | rhs, mode)`. `Rhs(reads, combine)` is a by-name RHS (refs resolve against the tweaked key). `Mode.BYNAME` (default, `evaluateInParentOfGiven`), `BIND` (`evaluateInCurrent`, what `Tweaks.bind` gives), `BIND_ONCE` (`evaluateInGiven`). `When(argPred, readRefOrNull, valuePred)`.
- `Level(tweaks, tagOrNull)` is one flat scenario (optionally setting `ProbeTagKey`); `Stack(levels, envToken)` lists levels from just above the root.

## Oracles

- `new SequentialEvaluator(stack).eval(key)` (or `.evalAt(key, top)`): no cache. Per level from the top: instance tweak (last wins), then property tweak (last wins) whose `when` holds, with the `when` read evaluated one level down (TwkResolver's `cur.parent`). A by-name RHS is evaluated at `lvl-1` (BYNAME), `lvl` (BIND_ONCE) or the requesting position (BIND). SI properties evaluate in an SI position where any tweakable read throws `OracleISD`. Tag readers see the nearest level tag; env readers see `stack.envToken`. It records, per visited `key@position`, the tweakable keys looked up transitively (`reads`, `visited`).
- `InvalidationOracle.dependents(ev, changedKey | changedProp)`: the exact set of visited keys whose evaluation looked up the changed tweakable (a key that is itself tweakable counts as reading itself). This uses the dynamic reads, so a tweaked node does not depend on its declared children, and `when` and RHS reads count. `staticDependents(universe, key)` is the declared-DAG over-approximation.

## Runtime binding (`ProbeSystem`, CacheFixtures.scala)

- `val sys = new ProbeSystem(dag)`: one `ProbeProperty` per `Prop`. `pp.info` is the singleton `TwkPropertyInfo0` (a `PropertyTarget`, so property tweaks target it directly) with flags from the Prop (`ProbeProperty.LOCAL_CACHE = 1L<<19`). `pp.runs`, `pp.runsOf(key)` and `pp.lastRun.get(key)` (the last node instance that ran: use it for `isInvalidCache` and identity checks). `sys.rhsRuns`, `sys.whenRuns`, `sys.runHook`.
- `sys.newKey(key)` returns a fresh key node (the `$newNode` convention: `scenarioStack == null`, never reused). Its class follows the Prop: `TweakableLeaf` (no reads), `Combinator`, `SINode`, `TagReader` (`EvaluationContext.findPluginTag(ProbeTagKey)`), `EnvReader` (`scenarioStack().env`, unmarked, mapped through `sys.registerEnv(env, token)`), `BadKeyNode`. `run` reads children with `sys.newKey(ref).lookupAndGet`, so tweak resolution, caching, recording and TTrack tracking all fire. `argsCopy(gen)` serves `when` clauses and `TweakTemplate.byKey`.
- `sys.tweak(tw)`: an instance constant uses `SimpleValueTweak(key)(v)`; an instance by-name uses `new Tweak(InstancePropertyTarget(key), TweakTemplate(ProbeRhsNode))`; a property tweak uses `new Tweak(info | PredicatedPropertyTweakTarget(info, WhenGen), TweakTemplate(v) | TweakTemplate.byKey(RhsGen))`; then `.bind(evaluateIn)` per mode.
- `sys.scenario(level, unordered = false)`, `sys.stack(root, stack, unordered = false)` (one `createChild` per level, adding the tag when set), `sys.eval(ss, key)`, `sys.lookupNode(ss, key)` (the node getNode returned, completed).
- Entities: `ProbeEntity(id)` is a Scala `EntityImpl` subclass in `optimus.graph` with `argsEquals`/`argsHash` on the id (applied flavour, so `==`/hash follow the id). No class in `optimus.platform.storable` is needed, because `EntityImpl` already implements `entityFlavorInternal`. `ProbeModule` is the `FakeModuleEntity`.

## CacheFixtures (object)

- `init()` must be called first on each driver thread. It runs `initializeWithoutRuntime()`, and it also forces `NodeCacheConfigs`, which creates `UNodeCache.global`/`siGlobal`; without that, the first cacheable lookup throws an NPE.
- `newRoot(env = minimal)` is `ScenarioStack.newRoot(env, uniqueID = true)`. `newEnv()` gives a distinct `RuntimeEnvironment(null, null)`. `child(ss, scenario[, tag])`, `withTag(ss, v)`.
- `evalIn(ss)(body)` runs body in a node attached to ss. `eval(ss, key)` is `lookupAndGet` from such a node. `lookupNode(ss, key)`.
- Low-level builders: `valueTweak`, `instanceTweak`, `propertyTweak(info, v)` (`new Tweak(info, TweakTemplate(v))`), `byNameInstanceTweak`, `bind`/`bindOnce` (`Tweaks.*`), `withMode`, `scenario(...)`, `unordered(...)`, `nested(outer, inner)`.
- Priming:
  - `installGlobalCache(16)` installs a new `UNodeCache` as `UNodeCache.global`. The constructor registers it with `Caches`, which is all the `schedulerAsserts` check in `global_=` requires. In a child JVM, use `-Doptimus.scheduler.cacheSize=16` instead (`-Doptimus.cache.concurrency=N` sets concurrency).
  - `privateCache(info, 16)` (`setCustomCache`), `setTweakableID(info, id)`, `setAssumeDependsOn(info, ids*)` (which also sets the policy to XSFT), `setPolicy(info, NCPolicy)`.
  - `setTTrackWatermark(n)`, or `-Doptimus.tracking.cleanup.ttrackClearedNodeRefWatermark=1` (`watermarkProperty`).
  - `xsftEnabledByDefault`.
- `runChild(jvmFlags, mainClass, args, timeoutMs)` returns `(exit, output)` and uses `CACHE_JAVA_CMD`.

## Tracker (`optimus.graph.tracking.CacheTrackerAccess`)

- Construction and teardown: `newRoot()` / `newRoot(timedScheduler)`, `consistentChild(t, name)` (shared queue), `concurrentChild(t, name)` (own queue), `dispose`, `disposeAllRoots` (`TEST_ONLY_disposeAllRoots`), `setAutoCleanup`, `cause(name)` (`TestEventCause`).
- Updates: `addTweaks(t, tw*)`, `addTweaksAsync`, `removeTweak`, `removeAllTweaks`, `batch(t)(u => u.addTweaksImmediate(...))`, `batchAsync`.
- Evaluations: `evaluate(t, key[, scope])` (`evaluateNodeKey`, `NullScope`), `evaluateKeys`, `evaluateKeysAsync`, `evaluateKeysLowPriorityAsync` (each returns a `CompletableFuture[(nodes, keys)]`), `executeEvaluate`, `executeEvaluateAsync(t, lowPriority)`, `await(f, ms)`. `evaluationStack(t)` is the stack evaluate actions use; `rawStack(t)` bypasses the queue.
- Callbacks: `newScope(name)`, `track(t, key, scope)`, `registerNodesInvalidated(t, scope)(cb)`.
- Internals:
  - `queue`, `currentCancellationScope`, `currentCancellationScopeRaw` (`_currentCancellationScope`), `workQueue`/`lowPriorityQueue` (+`Size`), `inFlightWork`, `isUpdating`, `queueState`, `outstandingWork`.
  - `traversalId(root)` / `setTraversalId` (a `private[this]` field, found by name or `$$name` suffix), `withFreshTraversalId`.
  - `ttracks(t)` (`TweakableTracker.ttracks`), `tweakableTracker`, `forceCleanup`.
  - `findField`/`field`/`setField` for anything else.

## Delay hooks (`CacheDelayAgent` + `CacheDelay`)

Each hook is a stack-neutral `ldc id; invokestatic probes/CacheDelay.point`. The agent prints the inserted hooks per class at load time, and `CacheDelay.instrumented` lists them. `CacheDelay.point` counts hits (`startCounting`/`counts`/`count(id)`/`stopCounting`), runs a global handler on whichever thread hit the point (`onHit(id, h)`/`clearHandler`/`clearAll`; a handler may block to force a cross-thread interleaving), and then calls `probes.Delay.point(id)`, so `Delay.arm(ctl, ids)` (thread-local step schedules) and `-Dprobes.delay.maxNanos` work as before.

| id | where |
|---|---|
| `nc.put.head` / `nc.put.cas` | `NodeCacheBase.putIfAbsentCore`: after the bucket-head `getVolatile` / just before the bucket CAS |
| `lru.remove`, `lru.catchup` | `NodeCacheWithLRU.remove(NCEntryLRU)`, `catchupEntry`: entry |
| `lru.setMaxSize.swapped` | `NodeCacheWithLRU.setMaxSize`: after `table = new ...`, before `moveEntries` |
| `ttrack.completed.cas` | `TTrack.nodeCompleted`: before `nodes_h.weakCompareAndSet` (each retry) |
| `ttrack.addCaller`, `ttrack.merge` | `TTrack.addCaller(TTrack,int)`, `TTrack.merge`: entry |
| `ptracks.nextFree` / `ptracks.resize.cas` / `ptracks.slot.cas` | `PtracksArray.addCaller`: after each `nextFree` read / before the `data` CAS / before the slot CAS |
| `xsft.promised` / `xsft.beforePromisedToUp` | `DelayedXSFTProxyNode.runByWaitingOrEvaluating`: after `promiseOrAwait` returns / before `cips.promisedToUp =` |
| `xsft.offer.beforeSync` | `DelayedXSFTProxyNode.offerValue`: before `synchronized(this)` |
| `xs.run` | `DelayedXSProxyNode.run`: entry |
| `dtq.schedule` | `DependencyTrackerQueue.scheduleAction`: entry |
| `dtq.barrier.beforeWait` / `dtq.cancelOrphans.beforeWait` | the `foreach` lambdas of `evaluationBarrier` / `cancelOrphanedNodesAndResetCancellationScope`: after `cancelInternal`, before `waitForCancelledNodesToStop` (once per scheduler) |
| `sched.waitCancelled.loop` | `OGScheduler.waitForCancelledNodeToStop`: top of the polling loop body |

The existing `OGTrace.TESTABLE_FIXED_DELAY_CACHE/XS/XSFT` counters are still available where they fit.

## Things to know

- **XSFT is the default policy in this build** (`optimus.profile.enableXSFT` defaults to true). At the root, `getNode` of a plain cacheable property returns a `DelayedXSFTProxyNode`, not the property node itself. Run with `-Doptimus.profile.enableXSFT=false` for Basic. The smoke passes both ways.
- `Tweak.equals` ignores `evaluateIn`. Scenarios that differ only in bind/bindOnce/by-name therefore share a cached child stack under the same parent, and the second one resolves with the first one's tweak. The smoke reproduces this deterministically (ring-3 S5 / W5 / D13): `[a:=3][b:=a bind][a:=9]` returns 3 on a parent that has already seen the by-name variant, while the oracle says 9.
- When counting hooks, remember that `dtq.barrier.beforeWait` fires once per scheduler per barrier, and only when an evaluation scope exists.
