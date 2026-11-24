# 0016. One protocol for every constructor, as a pure state machine in one `TRef`

Status: Accepted. Supersedes the implementations of [0001](0001-single-queue-batcher.md) and
[0003](0003-per-key-batching.md); their contracts stand.

## Context

0.3 had two kernels. `Ferry.make` used a queue, a doorbell, a lock serialising admission with close, a semaphore so
that one worker assembled at a time, and uninterruptible regions so that a worker owned what it took. `Ferry.perKey`
kept its lanes in one `TRef`, but a worker's ownership of its batch lived in a separate `Ref`, set after the
transaction. Each kernel had its own model, written by hand at the granularity of its steps. Nothing but discipline
kept the code and the model in step ([0008](0008-deferred.md)), and the steps of `make` were not atomic with each
other: a check against the model would have needed a hand-written refinement mapping.

## Decision

- **One protocol.** `Ferry.make` is the protocol for one key, whose batches may run concurrently
  (`oneCallPerKey = false`) and which never calls an idle key directly. `Ferry.perKey` is the general case.
- **All shared state in one immutable value**, `Protocol`, held in one `TRef`. That includes what used to be kept
  elsewhere: which worker holds which batch, which callers are pending, which caller has a direct call.
- **One method per step, one STM transaction per step.** Each method is an action of
  [`formal/Ferry.tla`](https://github.com/EtaCassiopeia/zio-ferry/blob/main/formal/Ferry.tla) with the same name,
  and returns the actions it took ([0017](0017-model-conformance.md)).
- **Close first.** Shutdown closes admission before it interrupts workers and direct calls, and a worker takes
  nothing once admission is closed. An interrupted call is therefore always an event after close.
- **A caller that gives up leaves its lane at once**, freeing its place in the bound, instead of staying until its
  batch is assembled.

## Consequences

- The races 0.3 guarded with a lock, a semaphore and uninterruptible regions cannot happen. Three of the model's
  weakened variants (`NonAtomicTake`, `NoCleanupOnInterrupt`, `PerKeyNoAwaitCall`) were deleted because the
  mechanisms they removed became structural.
- `Ferry.make` loses the forming lock and the doorbell. Its contract is unchanged: a batch leaves when full or
  `maxBatchDelay` after its oldest input.
- Two tests that relied on a cancelled input holding its place now advance the clock instead.
- Every caller contends on one `TRef` in `make` too. Sharding stays deferred until a measurement asks for it
  ([0008](0008-deferred.md)).
