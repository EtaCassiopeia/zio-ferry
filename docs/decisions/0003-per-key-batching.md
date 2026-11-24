# 0003. Per-key batching with one batch in flight per key

Status: Accepted. The implementation is superseded by [0016](0016-one-protocol.md), which keeps the contract.

## Context

A common reason to batch is a write path where each entity (a stream, a document, a log) is updated in order, and each
update is a read-modify-write guarded by a conditional write. Such an entity is limited by round trips, not by work:
one round trip applies one update or twenty at about the same cost. Batching an entity's updates raises its throughput,
but only if:

1. **an entity's updates are not split across concurrent batches.** Two batches for one entity race; with a conditional
   write the loser is refused and retried, so concurrency turns into contention instead of throughput.
2. **an entity's updates keep their order.** A later batch must not overtake an earlier one.

`Ferry.make` cannot provide either: it batches one undifferentiated stream and runs up to `maxInFlightBatches`
batches at once. Two ways to use it per entity both fail:

- *One `Ferry` per entity* costs a queue, a lock and worker fibers per entity: memory and scheduler load that grow
  with the number of entities, most of them idle.
- *One `Ferry` for everything* mixes entities in a batch and runs batches of the same entity concurrently.

## Decision

Add `Ferry.perKey`, a separate constructor with its own `PerKeyConfig`:

- **Each key has a lane**, a FIFO of its waiting inputs, created when its first input arrives and removed when it is
  empty and idle. Memory is proportional to waiting inputs, not to the number of keys ever seen.
- **At most one batch in flight per key.** A lane is *waiting* (partial, not yet due), *ready* (full or due) or *busy*
  (its batch is in flight). Inputs keep joining a busy lane; it is never taken again until its batch completes.
- **Batches carry one key's inputs, in submission order**, and the batch call receives the key.
- **`maxInFlightBatches` workers are shared by all keys.** Ready lanes are served first come, first served, and a lane
  that still has inputs when its batch completes joins the back of the line, so a key that always has more cannot
  starve the others.
- **All state is one immutable value (`Lanes`) in one `TRef`.** Admission is one STM transaction with the check that
  the ferry is open, and taking a batch is one transaction with marking its key busy. This rules out, by construction,
  the races [0001](0001-single-queue-batcher.md) guards with a lock and an uninterruptible region. Recording a taken
  batch as owned by the worker happens in the same uninterruptible step as the take, so an interrupted worker can
  always release its callers.
- `Ferry.make` is unchanged: per-key ordering is a different guarantee, not a generalisation.

The lane structure is pure so that it can be tested step by step against a model (`LanesSpec`), and the protocol is
model-checked in `formal/PerKey.tla`, with variants showing that the busy guard, the atomic take and the atomic
admission are each needed.

## Consequences

- An idle worker waits for any change to the lanes, or for the earliest waiting lane's deadline. Every admission wakes
  every idle worker; with a handful of workers this is cheap, and only the first finds work.
- One `TRef` is contended by every caller. Under very high rates, several independent ferries, each owning a share of
  the keys (`hash(key) % n`), spread that contention without changing any per-key guarantee, because every guarantee is
  local to a key. Building sharding in is deferred until a measurement asks for it ([0008](0008-deferred.md)).
- A key that is busy for a long time holds its inputs back by design; `maxQueueing` and `maxQueuedWeight` bound how
  much can pile up behind it, across all keys.
- The idea is the one that lets purpose-built databases such as TigerBeetle serve heavily contended entities without
  lock contention: serialise per entity, and batch to amortise the fixed cost of each round trip.
