# 0010. Send an idle key's input directly; batch only keys with concurrent work

Status: Accepted

## Context

Most keys are quiet most of the time; a few are busy, sometimes only for a while, such as during a bulk import or a
reprocessing run. Batching pays off only for a key with concurrent work: the inputs that pile up while its previous call
is in flight. For an input whose key has nothing waiting and nothing in flight, going through a lane and a worker only
costs latency, and when busy keys fill every worker slot, it waits behind them.

Classifying keys as busy up front (a list, a flag in the input, a rate threshold) was considered. It needs a mechanism
to keep the classification current, and gets it wrong at the edges of every burst.

## Decision

With the default zero `maxBatchDelay`, an input for an idle key (no lane: nothing waiting, nothing in flight) is
**claimed** for its caller in the admission transaction and sent at once, as a batch of one, without a queue or a
worker. The key is busy while that call is in flight, so inputs that arrive meanwhile wait in its lane and are batched
behind it. A key switches to batching exactly when it has concurrent work, and back when it is idle again: the key's own
concurrency is the classification.

- The call runs in a fiber forked into the ferry's scope, so closing the scope interrupts it like any batch in flight.
- An interrupted caller interrupts its call and releases the key **only once the call is over**; releasing earlier
  would let the next batch overtake a call still on the wire.
- Direct calls are not counted against `maxInFlightBatches`, which bounds the batches of busy keys: quiet traffic never
  waits behind busy keys for a slot. At most one direct call runs per idle key.
- `PerKeyConfig.directIdleKeys` (default `true`) turns the direct path off, for a downstream that needs one hard bound
  on concurrent calls. A positive `maxBatchDelay` turns it off too: it means "wait for company", even for an idle key.

## Consequences

- Total concurrent calls are bounded by `maxInFlightBatches` plus the number of idle keys called at once, which is the
  same as unbatched traffic. A downstream that cannot take that sets `directIdleKeys = false`.
- `metrics.inFlightBatches` counts direct calls too.
- `formal/PerKey.tla` (now `formal/Ferry.tla`, with `FerryNoDirectGuard`, since [0016](0016-one-protocol.md)) models the claim, the forked call and the release; `PerKeyNoDirectGuard` (claiming a key that is
  not idle) and `PerKeyNoAwaitCall` (releasing before the call is over) must fail with two calls in flight for one key.
  `PerKeyBatchOnly` keeps the batch-everything mode checked.
