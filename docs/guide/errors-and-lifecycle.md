# Errors and lifecycle

## What a caller can get back

Every caller gets exactly one of these. None is left waiting.

| Outcome | When | What to do |
|---|---|---|
| Its result `A` | The batch call succeeded. With per-input outcomes, `A` may itself be a rejection of this input on its merits. | Use it. A rejection on the merits is not worth retrying. |
| `Outcome.Committed` (with [`ProjectedWrite`](projected-write.md)) | The input is durable, but its projection was not confirmed: it kept failing. | Tell the client it was accepted. A sweeper you run, or the key's next batch, finishes the view. Do not resubmit to hurry it. |
| `FerryError.Rejected` | The waiting room was full (`maxQueueing`, or `maxQueuedWeight` for `perKey`). Nothing was sent. | Retry after a backoff, or shed the load. |
| `FerryError.Failed(e)` | The batch call carrying the input failed with `e`. Every caller in that batch gets it. | Retry if the call is idempotent, which a [fenced write](fold-write.md) is. |
| A defect | The batch call died, or `extract` threw for this caller. | Fix the bug. |
| Interruption | The ferry's scope closed while the caller waited, or the caller called after it closed. | The process is shutting down. |

Later calls are still served after a failed batch. One failure never poisons the ferry.

## Interrupting a caller

- **While its input waits**, the input is dropped from its batch. Nothing is sent for it.
- **While its batch is in flight**, the caller stops waiting, but the batch call runs on for the other callers in it.
  The input may still be applied. Treat it like a timeout: the outcome is unknown, and a retry must be safe.
- **During a [direct call](per-key.md#only-busy-keys-are-batched)** (`perKey`, idle key), the call is interrupted
  too. The key is released only once the call is over, so a later batch can never overtake a call still on the wire.

## Closing the scope

A ferry lives in the `Scope` it was created in. When the scope closes:

1. the workers are interrupted, and every caller whose batch they held is interrupted;
2. admission stops, and every input still waiting is released by interrupting its caller;
3. calls made afterwards are interrupted.

Admission and close are one atomic step, so no input can slip into a waiting room that has already been drained.
Direct calls run in the ferry's scope too, and are interrupted with it.

## Guidance for clients

Ferry holds waiting inputs only in memory. A crash loses what was waiting, which is safe as long as:

- **nothing is acknowledged before it is durable.** A caller is answered only once the batch call returned;
- **clients retry** on `Failed`, `Rejected`, interruption and their own timeouts, with the same input id and content;
- **the write is idempotent**, so a retry after a lost response does not apply an input twice;
- **client timeouts exceed the expected batch latency.** Otherwise clients retry while their batch is still committing,
  which is safe but wasted work.

[Running on a fleet](../architecture/fleet.md) explains why these rules make the system correct even when routing is
not.
