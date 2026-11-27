# 0002. Verify with tests, model checking and mutation testing together

Status: Accepted. The mutation-testing layer is superseded by [0009](0009-drop-mutation-testing.md); the layers now are
those of [verification](../verification.md) ([0017](0017-model-conformance.md), [0018](0018-deterministic-simulation.md)).

## Context

The batcher's failure modes are concurrency bugs: a caller left waiting forever, an input sent twice, a batch split or
reordered. Example-based tests only cover the interleavings someone thought of, and a passing suite says nothing
about whether it would notice a bug.

## Decision

Each technique covers what the others cannot:

| Layer | What it gives | Where |
|---|---|---|
| Unit tests on virtual time | Exact timing of deadlines and delays | `*Spec.scala` |
| Model-based tests | Batches and dispatch instants agree with an executable specification | `FerryModelSpec`, `LanesSpec` |
| Seeded simulations | Random workloads, failures, interruptions and shutdown, every trace checked against the contract | `Simulation`, `PerKeySimulation`, `FencedWriterSimulation` |
| Stress tests on the real scheduler | Interleavings virtual time never picks | `*StressSpec` |
| TLA+ / TLC | Every interleaving of the protocol in a small instance, for safety and liveness | `formal/` |
| Mutation testing | Evidence that the suite fails on wrong code: Stryker4s for syntax, hand-written mutants for concurrency | `mutation/` |

Two rules tie the layers together:

- **Every formal model ships with weakened variants that must fail.** Each removes one mechanism; TLC finding a
  counterexample proves the mechanism is needed, and that the model is able to notice its absence.
- **A semantic mutant that survives the tests is accepted only if a formal variant covers it.** Such mutants are races
  too narrow for any scheduling-based test; the harness reports them as formally covered and fails on any other
  survivor.

## Consequences

- The formal models are written by hand and can drift from the code. They are kept at the granularity of the code's
  atomic steps (one action per STM transaction or uninterruptible region) so that a change to the code maps to a change
  in the model. Trace validation would close the gap mechanically; see [0008](0008-deferred.md).
- Mutation testing costs one full test run per mutant, so it runs weekly in CI, not on every push.
- Sample counts and test timeouts are overridable (`FERRY_SAMPLES`, `FERRY_TEST_TIMEOUT_SECONDS`) so that a
  mutation run stays affordable and a hanging mutant fails fast.
