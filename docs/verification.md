# Verification

A batcher's bugs are concurrency bugs: a caller left waiting forever, an input sent twice, a key with two calls in
flight, a batch out of order. Example-based tests cover only the interleavings someone thought of, so Ferry is
checked in layers, each covering what the others cannot. See [0002](decisions/0002-layered-verification.md) and
[0009](decisions/0009-drop-mutation-testing.md).

| Layer | What it gives | Where |
|---|---|---|
| Unit tests on virtual time | exact timing of deadlines and delays | `FerrySpec`, `PerKeySpec`, `FoldWriteSpec`, `ProjectedWriteSpec` (one object per record, and one row per entry) |
| Model-based tests | batches and dispatch instants agree with an independent executable model, step by step | `FerryModelSpec`, `LanesSpec` |
| Seeded simulations | random workloads, failures, interruptions and shutdown; every trace checked against the contract | `Simulation`, `PerKeySimulation`, `FencedWriterSimulation`, `ProjectedWriterSimulation`, `PartialProjectionSimulation` |
| Stress tests on the real scheduler | interleavings that virtual time never picks | `FerryStressSpec`, `PerKeyStressSpec` |
| TLA+ model checking | every interleaving of the protocol, in a small instance, for safety and liveness | [`formal/`](https://github.com/EtaCassiopeia/zio-ferry/tree/main/formal) |

## What the simulations check

Every trace, for every random scenario:

- every caller gets an answer, and the right one for its batch: its result, its batch's failure, or an interruption
  that someone actually caused;
- no input is sent twice; a rejected input, or one whose caller gave up while it waited, is never sent;
- no call is rejected while the waiting room could not have been full;
- batches respect `maxBatchSize` and `maxBatchWeight`, and a partial batch never leaves before its deadline;
- nothing is sent after close, and every call made after close is interrupted;
- (`perKey`) a call holds one key's inputs, a key never has two calls in flight, and a key's inputs are never sent out
  of order;
- (`perKey`) a direct call carries exactly its caller's input, and happens only with zero delay;
- (fenced writer) through crashes, lost responses, throttling and misrouted clients, no input is applied twice, and
  every answer matches what the store holds.
- (commit, then project) through the same faults, a refused projection and a crash between the two writes, no entry is
  projected twice, every view is a projection of its log, an input answered `Projected` was in the view, and with a
  sweeper every view catches up with its log. Without the projection's fence an entry is projected twice; without a
  sweeper a projection that ran out of retries is never finished; answering `Projected` straight after the commit
  claims a view that is not there.

The stress tests add what only real threads show: the number of batches in flight never exceeds
`maxInFlightBatches`, and no caller hangs.

## Formal models

Each model is written at the granularity of the code's atomic steps: one action per STM transaction or uninterruptible
region, so a change to the code maps to a change in the model.

**Every model ships with weakened variants that must fail.** Each variant removes one mechanism. TLC finding a
counterexample proves the mechanism is needed, and that the model can notice its absence.

| Model | Checks | Variants that must fail |
|---|---|---|
| `Ferry.tla` | no input sent twice, turned-away inputs never sent, one assembler, bounded batches, one owner per input, nobody stranded on close; every admitted caller is answered | `UnserialisedAdmission`, `NoCleanupOnInterrupt`, `NonAtomicTake`: a caller is stranded |
| `PerKey.tla` (`PerKey`, `PerKeyBatchOnly`) | all of the above per key, plus one call in flight per key (direct calls included) and rank order; every caller answered, every direct caller done | `PerKeyUnsorted`: out of rank order. `PerKeyNoBusyGuard`, `PerKeyNoDirectGuard`, `PerKeyNoAwaitCall`: two calls in flight for one key. `PerKeyNonAtomicTake`, `PerKeyNonAtomicAdmission`: a caller is stranded |
| `FencedWriter.tla` | across processes with arbitrary routing, crashes and lost answers: no input applied twice, every answer true | `FencedWriterUnfenced`, `FencedWriterNoDedup`: an input is applied twice |
| `ProjectedWriter.tla` | the same for a commit followed by a fenced projection and a sweeper: no entry applied or projected twice, the view a prefix of the log, every answer true, an input answered "projected" only once it is in the view; the view eventually equals the log | `ProjectedWriterUnfencedProject`: an entry is projected twice. `ProjectedWriterEarlyAnswer`: answered "projected" before the view has it. `ProjectedWriterNoResume`: without retrying a failed projection or a sweeper, one cut short by a crash or a give-up is never finished (liveness) |
| `ProjectedWriterPartial.tla` | the same for a view of one row per entry, each row its own write: no entry applied twice, no row written twice, every row from the log, the marker clear only when every entry has its row, every answer true, an input answered "projected" only once its row is in the view; eventually every entry has its row and the marker is clear | `ProjectedWriterPartialUnconditionalRows`: a row is written twice. `ProjectedWriterPartialEarlyClear`: the marker clears with a refused row missing. `ProjectedWriterPartialUnfencedClear`: a clear made for a shorter log clears the marker a later commit set. `ProjectedWriterPartialNoResume`: without reading again or a sweeper, a refused row is never written (liveness) |

CI runs every configuration on every change, and fails if a passing model fails or a weakened variant passes. A variant
that removes a liveness mechanism must violate its temporal property rather than an invariant.

## Running it

```sh
sbt testFull                                  # every test; sbt 2's `test` is incremental
FERRY_SAMPLES=2000 sbt testFull            # search longer, e.g. before a release
formal/check.sh path/to/tla2tools.jar         # TLC, every model and variant
```

`FERRY_TEST_TIMEOUT_SECONDS` sets the per-test time limit, so a change that makes callers hang fails fast.

## Limits

- The models check small instances (a few callers, keys and workers). Larger or unbounded checks are listed in
  [0008](decisions/0008-deferred.md).
- The models are written by hand and can drift from the code. Trace validation would catch that mechanically; it is
  deferred too.
- Nothing here measures performance. Benchmarks come before any performance work.
