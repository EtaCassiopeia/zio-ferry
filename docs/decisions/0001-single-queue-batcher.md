# 0001. One admission queue, one batch assembled at a time

Status: Accepted (recorded after the fact for the ZIO 2 rewrite of `Ferry.make`). The implementation is superseded by [0016](0016-one-protocol.md), which keeps the contract.

## Context

A downstream system is called once per input, and the per-call overhead dominates. Coalescing concurrent calls into
batches cuts that overhead, but a batcher in front of a system is also its bulkhead: it decides how much load to let
through, how much to hold, and what to do with the rest. It must never lose a caller: every caller gets a result, an
error, or an interruption.

## Decision

- **A bounded, dropping queue for admission.** A full queue rejects immediately (`FerryError.Rejected`) instead of
  blocking the caller: under overload, failing fast is the only answer that does not grow latency without bound.
- **One worker per batch slot, and a forming lock.** `maxInFlightBatches` workers each assemble and run batches, but only
  the lock holder assembles, so a batch is never split between two idle workers.
- **The deadline belongs to the oldest input**, not to when assembly started. An input that waited while every slot was
  busy is dispatched as soon as a slot frees, and its wait does not restart.
- **Admission is serialised with close**, and closing interrupts the workers before draining the queue, so no caller can
  enter a queue that has already been drained.
- **Taking inputs off the queue and recording them as owned by the worker is one uninterruptible step**, and an
  interrupted worker interrupts every caller it owns.
- **Callers interrupted before dispatch are left out of their batch**; their promise is already complete.
- **Typed errors and validated configuration.** `FerryError` separates rejection from batch failure; a defect stays a
  defect. `FerryConfig.make` reports every invalid setting at once.

## Consequences

- One queue serves every caller, so the batcher cannot keep inputs of different entities apart or in order; that is
  what [0003](0003-per-key-batching.md) adds.
- The admission lock is a single point of contention. It has not been measured to matter; see [0008](0008-deferred.md).
- Two of the mechanisms above guard races only a few instructions wide, which tests do not hit reliably; the formal
  model covers them ([0002](0002-layered-verification.md)).
