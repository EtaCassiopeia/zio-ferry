# 0011. A key's waiting inputs are sent in a pluggable order

Status: Accepted. Builds on the proposal in [`docs/design/ordering.md`](../design/ordering.md).

## Context

Inputs often carry a time of their own, such as the date or instant the thing they describe takes effect, and that
time, not when the input happened to arrive, decides which comes first. Inputs arrive out of that order, with some
latency. Applications differ in what decides the order and in what their store can do with an input that arrives
after a later one was already written.

The batcher only decides **dispatch order**: the order in which a key's inputs reach the store. Whether a late input is
correct is the store's verdict, because the store holds the key's state; a batcher that remembered what it last sent
per key would hold state the store already holds, be wrong after a routing handover, and cost memory per key ever seen
(the same reason as [0005](0005-fenced-single-writer.md)).

## Decision

`Ferry.perKey` takes an `order: Order[I]`: a pure `rank(input): Long`, evaluated once per input at submission,
outside any transaction. A lane keeps its inputs in ascending rank, ties in arrival order, and batches are taken from its
front.

- `Order.arrival` (default): one rank for all, which is today's behaviour.
- `Order.by(rank)`: e.g. an effective date as `toEpochDay`, an instant as epoch milliseconds, a sequence number.
- An application implements `Order` for anything else.

An input that arrives after inputs of a higher rank were already sent is **not held back or rejected**: it goes in the
key's next batch, in rank order among what is waiting then, and the store places it. Inputs that wait together are
always sent in rank order, and under load everything that arrived during the previous round trip waits together: a
settling window equal to one round trip, at no added latency. To widen it, set a positive `maxBatchDelay`; its deadline
counts from a partial batch's oldest input by arrival, whatever its rank.

## Consequences

- No new error and no flag passed to the batch call: a late input carries its own time, and the store decides.
- `formal/PerKey.tla` (now `formal/Ferry.tla`, since [0016](0016-one-protocol.md)) checks that inputs waiting together are sent in rank order, ties by admission, and lets an input
  admitted after another left the lane follow it; `PerKeyUnsorted` (joining the back of the lane) must fail.
  `LanesSpec` and the simulations check the same against independent models.
- Deferred, each until a store needs it:
  - **holding inputs for a bound** (`eventTime(at, lateness)`: wait until `at + lateness` on the local clock, report
    later inputs as late), for an append-only store that refuses an input older than its newest. It costs the bound in
    latency on every input, and disables the direct path;
  - **rejecting late inputs** (`FerryError.Late`), for the same kind of store;
  - **partition boundaries** (batches never mix, say, two time buckets), for a group operation that refuses such a
    group;
  - **gap waiting on sequence numbers**: never in the batcher, because the next expected number is the store's state,
    and waiting for a lost input deadlocks.
