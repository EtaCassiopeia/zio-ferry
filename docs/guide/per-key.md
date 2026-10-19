# Per-key batching

`Ferry.perKey` batches each key's inputs separately and keeps them in order. A key is whatever your updates must
be serialised by: an entity, a stream, a document, a log.

It suits a write path where each entity is updated by a read-modify-write guarded by a conditional write. Such an
entity is limited by round trips, not by work: one round trip applies one update or twenty at about the same cost.
`perKey` makes the process the only writer of each key, so writers no longer contend, and each round trip carries
everything that arrived while the previous one was in flight.

```scala
--8<-- "Snippets.scala:per-key"
```

The batch call receives the key and that key's inputs, in order. `extract` hands each caller its own outcome.

## Guarantees

- **A batch carries one key's inputs**, never a mix.
- **A key never has two calls in flight.** Its next batch leaves only once its previous call has completed.
- **A key's inputs are sent in order**: submission order by default, or the [order you choose](ordering.md).
- **Keys don't starve each other.** Keys with work waiting take turns. A key that still has inputs when its call
  completes goes to the back of the line.
- **Different keys run in parallel**, up to `maxInFlightBatches` batch calls in total.

## How batches form

With the default `maxBatchDelay` of zero, there is no batching timer. A key's next batch leaves as soon as its previous
call completes and a worker is free, and it carries everything that arrived in the meantime:

- at low load, an input goes out alone, at once;
- at high load, batches grow by themselves, to roughly the arrival rate times the round-trip time, up to
  `maxBatchSize` and `maxBatchWeight`.

This is the group-commit pattern. A fixed delay is right at one load only; dispatching on completion adapts to every
load. See [0004](../decisions/0004-dispatch-on-completion.md).

![An idle key is sent at once; a busy key's inputs leave together in its next call](../assets/ferry.svg)

## Only busy keys are batched

Most keys are quiet most of the time; a few are busy, sometimes only for a while, during a bulk import or a
reprocessing run. With zero delay, an input for an **idle key** (nothing waiting, nothing in flight) is sent at once by
its own caller, as a batch of one. It does not wait for a worker, and does not queue behind busy keys.

Inputs that arrive for the key while that call is in flight wait in the key's lane and are batched behind it. A key
therefore switches to batching exactly when it has concurrent work, and back when it is idle again. Nothing has to
classify keys as busy up front. See [0010](../decisions/0010-direct-calls-for-idle-keys.md).

Direct calls are not counted against `maxInFlightBatches`, which bounds the batches of busy keys. In total, concurrent
calls are bounded by `maxInFlightBatches` plus the number of idle keys called at that moment, the same as without
batching. If the downstream needs one hard bound on concurrent calls, turn direct calls off:

```scala
PerKeyConfig.make(maxBatchSize = 25, maxInFlightBatches = 8, maxQueueing = 1000, directIdleKeys = false)
```

A positive `maxBatchDelay` turns them off too, since it means "wait for company", even for an idle key.

## Bounding by weight

Counts bound neither of the two limits that usually run out: the downstream's request size limit, in bytes, and the
memory held by waiting inputs. Give `perKey` a `weigh` function, and set `maxBatchWeight` and `maxQueuedWeight`:

```scala
--8<-- "Snippets.scala:per-key-tuned"
```

A batch never weighs more than `maxBatchWeight`, except a single input that is heavier on its own: it travels alone.
An input that would take the waiting room above `maxQueuedWeight` is rejected, like one beyond `maxQueueing`. Both
default to unbounded. Weights are computed once, when the input is submitted. See
[0007](../decisions/0007-bound-by-weight.md).

To keep memory bounded, submit references to large payloads (an id, a storage locator), not the payloads.

## Outcomes per input

If one input in a batch is invalid, it should fail only its own caller. Let the batch call return one outcome per input
and fail only when the batch as a whole did not happen:

- `call` returns, for example, `Map[Id, Either[Rejection, Result]]`, and applies the valid inputs;
- `call` fails only when nothing was applied (the store was unavailable, the write was refused). Every caller in the
  batch then gets `FerryError.Failed`, and that is the failure worth retrying;
- `extract` picks each caller's outcome.

See [0006](../decisions/0006-per-input-outcomes.md). [`FoldWrite`](fold-write.md) implements this for the read,
apply, conditional-write round trip.

## Next

- [Dispatch order](ordering.md): send a key's inputs by effective date, timestamp or sequence number.
- [Fenced writes with FoldWrite](fold-write.md): the write that makes retries and crashes safe.
- [Commit, then project](projected-write.md): the same for a store that writes a projected view in a second step.
- [Running on a fleet](../architecture/fleet.md): one writer per key across many processes.
