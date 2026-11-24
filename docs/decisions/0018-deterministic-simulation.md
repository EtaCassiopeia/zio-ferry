# 0018. Simulate deterministically: a run is a function of its decisions

Status: Accepted.

## Context

The simulations of 0.3 ran on ZIO's `TestClock`. Virtual time made them fast, but which fiber ran next was up to the
real scheduler. A seed reproduced a run's workload, not its interleaving, so a failure could not be replayed exactly
or shrunk.

## Decision

A generic simulator in the unpublished `testkit` module:

- **Scheduling.** All fibers run on one executor, driven from the calling thread. When more than one task can run,
  which one does is a decision.
- **Time.** The clock is virtual and jumps to the next sleep when nothing can run. A sleep that is due competes with
  the runnable fibers as one more option.
- **Program-owned nondeterminism**, such as a call's latency or outcome, goes through `Simulator.choose`.
- **Escapes**, meaning submissions from any other thread, are recorded and fail the determinism check.

Every decision is recorded. `Decisions.scripted` replays a run exactly, `Decisions.prefixed` forks it, and `Shrink`
reduces a failing script to the shortest that still fails.

## Consequences

- Building it showed that ZIO's fiber ids are random, so options are ordered by when the simulator first saw each
  fiber. `Runtime.unsafe.fromLayer` can deadlock against a simulator that is not being driven yet, so the runtime is
  built directly from its `FiberRef`s, which needs one file in package `zio`. And STM wakes waiters in a
  hash-derived order ([0019](0019-answers-after-commit.md)).
- Tests compare runs by their decisions, traces and the kind of each answer: an `Exit` carries fiber ids and
  exception instances that differ on every run.
- The simulator is single-threaded. The stress tests on the real scheduler remain the check against real-thread
  surprises.
- The `TestClock` simulations of `make` and `perKey` (`Simulation`, `PerKeySimulation`) are retired. Their contract
  checks moved into `FerrySimulation`, which also runs larger scenarios than the model is checked with. Checks that
  depend on the order of protocol steps (per-key order, nothing sent after a give-up or after close) are left to the
  model, which sees that order exactly; a test-side clock only approximates it. The writer simulations still use the
  `TestClock`.
