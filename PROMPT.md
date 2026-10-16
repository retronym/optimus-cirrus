# Replaying this analysis with Claude Code

The prompt below reproduces, more or less, the review and adversarial-testing campaign recorded in this directory, without handing over the findings. Give it to a fresh Claude Code session in a checkout of this repository (ideally in a worktree) with subagent support. Expect it to take a few hours of agent time and several hundred thousand tokens. Hints are deliberate; answers are not.

---

Do a deep code review of the core scheduler of this repository, the Optimus graph engine ("cirrus"), then design and run an adversarial test campaign against it. Work design-first: write a review document and a campaign plan as Markdown files at the repo root for me to read, keep them updated as you go, and commit at natural checkpoints with plain commit messages (no attribution trailers). Use Opus subagents for the coding and building; keep the reading, judgement and verification yourself. Never commit anything under `optimus/`; all probe code goes in a new top-level `probes/` directory.

## Part 1: review

The scheduler lives in `optimus/platform/projects/core/src/main/scala/optimus/graph/`. Start with `OGScheduler.java`, `OGSchedulerContext.java` (including its nested `WorkQueue` and `SyncStack`), `NodeTask.java`, `NodeAwaiter.java`, `Scheduler.scala`, `Node.scala`, `OGLocalTables.java` and `Settings.java`, plus `optimus/core/IndexedArrayList.java`. Look for races, inefficiencies, foot-guns and missing wins. Grade each finding by how likely it is to bite in production and cite file:line.

Things worth being suspicious of, without telling you what you will find:

- Fields that pack two values into one `int`, and what happens to each half at the edges of its range. Think about where the inputs come from and how fast they grow over a process lifetime.
- Index arithmetic in the work-stealing deque: whether comparisons are wrap-safe, what the "reset" path assumes, and what *other* state outside the queue remembers an index.
- The comment block on `WorkQueue` describes states the queue may be in. Check whether each one is still reachable with the current code; stale comments are themselves a finding.
- Every place a method returns a task after a CAS or write that was supposed to confirm the take. What guards the system against running the same task twice, and whether any caller treats `size()` as a count of pending work.
- The paths that are meant to kill the process on an "impossible" error. Trace what happens if any step on that path itself throws, and whether any guard flag is ever reset.
- Collections that are protected by one lock on some paths and a different lock, or none, on others. `foreach` over a packed array with swap-remove is a classic.
- Lock ordering between the scheduler's lock and any static, process-wide lock, especially in cycle detection/recovery, and whether the reasoning in the comments still holds with more than one scheduler alive.
- Check-then-act sequences on the node state word versus the CAS loops that mutate it. Compare sibling methods that do the same job with and without a re-check inside the loop.
- Objects that are pooled and reused while something else may still hold a reference to them (sync-stack frames, waiter registrations).
- Iteration over a `PriorityQueue` while calling user callbacks that may mutate it on the same thread (the lock is reentrant).
- Busy-wait loops on plain (non-volatile) fields, and what makes them terminate today.
- What the spinning thread costs when another thread is in a long computation, what runs under the global lock (thread creation?), what every blocking wait allocates unconditionally, and timer-driven recovery that wakes every parked thread.
- Settings whose defaults come from a build-time source set rather than a runtime flag, and asserts that only exist behind a system property.

Write the review with sections for races, foot-guns, inefficiencies and missing wins. Be candid: mark what you could not confirm by reading alone.

## Part 2: build

Have an Opus subagent get the `core` module compiling and running locally without the internal build tool. Expect: a specific minimum Scala 2.13 patch version (a compiler plugin reflects on a reporter method that only exists from a certain release), a `-Yimports` flag that nothing documents (look for implicit conversions that fail to resolve), a pinned logback 1.2.x, stubs for internal Morgan Stanley libraries, and that the scheduler's static initialiser hard-requires the entity agent as `-javaagent`. Stay on JDK 21. Have the agent write a smoke/stress test with hand-written `NodeTask` subclasses (sync via `runAndWait` inside `run()`, async via `continueWith`/`onChildCompleted`) and run it under several thread counts with `-Doptimus.scheduler.asserts=true`. Record the recipe in the review.

## Part 3: campaign plan

Write a campaign plan with three tiers, each probe stating which invariant it attacks and what "falsified" looks like:

- **Tier 0, whitebox.** Each component in isolation, in package `optimus.graph` for package-private access, with private state set directly by reflection (queue indices near `Integer.MAX_VALUE`, the thread-id counter near 2^15 and 2^16, the causality id on a context) and interleavings forced by a schedule-driven step harness (participants call `step()` between actions; a schedule string like `A1 B1 B2 A2` decides the order; enumerate all permutations). Cover the deque, the indexed list, the node state word and waiter list, the sync stack, the stalled-callback bookkeeping, the context transitions, and the fixed cost of a blocking wait. Where the interesting window is *inside* a method, plan a delay-injection agent: an ASM load-time transformer that calls a hook after each slot read and before each CAS/swap in the deque, which a probe can route through the step harness or make park randomly.
- **Tier 1, primed end-to-end.** The real scheduler in a child JVM per configuration (settings are `static final`), with the same primed state, under sync-stack-heavy workloads, with a watchdog that dumps `OGScheduler.toString()` and classifies a hang from the scheduler state. Include a probe for double completion and one for a listener mutating the callback set during a stall; for those, record the exit code and read what actually happened to the process, not what the code says should happen.
- **Tier 2, hunters.** jcstress on the lock-free pairs (owner pop vs thief, grow vs steal, scan vs steal, waiter add vs complete, each pair of state-flag mutators) with the entity agent in the forked VMs; Lincheck on the deque against a spec that allows at-least-once delivery (you will need a custom verifier, and Lincheck will not be able to instrument the agent-rewritten node class, which is itself a testability finding); and a seeded random-DAG fuzzer mixing sync, FSM, promise-style completion from non-graph threads, async roots, failing leaves and mid-run cancellation, swept across thread counts × asserts × delay injection × `limitThreads`, with oracles for result correctness, exactly-once completion, hang classification and thread accounting after quiescence.

Record every probe as *demonstrated*, *not reproduced* (say what you tried) or *quantified*. When a probe contradicts a claim in the review, withdraw or downgrade the claim rather than leaving it standing, and when a probe shows something the review missed, add it with its own id.

## Part 4: run it

Implement and run Tier 0 yourself first (it is small and decisive), then hand the delay-injection agent, Tier 1 and the Tier 2 hunters to Opus subagents on disjoint files, verify every subagent result by rerunning it yourself before recording it, and commit after each tier. Finish with a "Method" section in the review summarising the tools and techniques used.
