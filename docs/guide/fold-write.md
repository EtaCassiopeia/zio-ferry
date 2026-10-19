# Fenced writes with `FoldWrite`

There are two ways to process a batch of a key's inputs:

- **Hand it to the store's own group operation.** The batch call passes the batch through; no helper is needed.
- **Read once, apply in memory, write once.** Read the key's state, apply the inputs to it in order, and write the
  result only if the store still holds what was read. `FoldWrite` packages this round trip.

## The round trip

![Read once, apply in memory, write if unchanged; a conflict retries](../assets/fold-write.svg)

`FoldWrite(id, read, step, write, retry)`:

| Parameter | What it does |
|---|---|
| `id: I => Id` | Identifies an input. Inputs of one batch with the same id are applied once and share the first one's outcome. |
| `read: K => ZIO[R, E, S]` | Reads a key's state, including whatever makes the write conditional, such as a version or a length. |
| `step: (S, I) => (S, O)` | Your logic: the next state and the input's outcome. It must be pure: it runs again on every retry. |
| `write: (K, S, S) => ZIO[R, E, Any]` | Writes `after` if the store still holds `before`, and fails otherwise. Skipped when nothing changed. |
| `retry: Schedule[R, E, Any]` | When to retry the whole round trip after `read` or `write` failed. |

`fold.call` is the batch call and `fold.outcome` the extract:

```scala
--8<-- "OrderedWrites.scala:fold"
```

## Making retries safe

A write can land and its response be lost. The retry then reads a state that already contains the batch's inputs.
`step` must answer such an input from the state, without applying it again:

```scala
--8<-- "OrderedWrites.scala:step"
```

Here an invalid input gets its own rejection and fails no other input. An input already in the log is answered with
its position. Only a store error fails the batch, and that is the case `retry` retries.

To answer a retried input with exactly the answer it got the first time, keep the answers in the state: `S` holds,
for each applied id, what `step` returned, and `step` returns it again instead of applying the input.

## Each input is answered as of its own position

`step` runs once per input, in order, on the state left by the inputs before it. An input's outcome is computed then,
from that state, not from the state the batch ends with: in a batch of three appends to an empty log, the first is
answered with position 0, the second with 1 and the third with 2.

## Order that lives in the state

Some inputs change how the ones after them apply: closing a period, rolling a business date over, switching a
schedule. That is a state transition, and it belongs in `step`:

```scala
--8<-- "Snippets.scala:period"
```

In the batch `Add(1)`, `Close`, `Add(2)` the first amount lands in period 0 and the second in period 1.
[`Order.by`](ordering.md) can make such inputs meet in the right order when they wait together; for an input that
arrives after a later one was already written, the fold over the state decides, as for any late input
([0011](../decisions/0011-pluggable-dispatch-order.md)).

## Inputs that depend on the ones before them

By default each input is judged on its own: a rejected input leaves the state as it was, and the inputs after it in the
batch apply as if it had not been there. One caller's invalid input never fails another caller's valid one
([0006](../decisions/0006-per-input-outcomes.md)).

Some keys are not like that. If an account's entries must apply in sequence, an entry after a rejected one must not be
attempted. That rule is about the key, not about the batch: which inputs share a batch is a matter of timing, so a
rule that only looked back inside one batch would answer the same inputs differently under load. Put the block in the
state instead, where `step` sees it for every later input, in this batch and the next:

```scala
--8<-- "Snippets.scala:dependent"
```

A caller then gets one of three answers: its result, its own rejection, or `NotAttempted` naming the entry that
blocked the key. Lifting the block is an entry of its own, a correction or a reversal, which `step` applies by clearing
`blockedBy`; until it arrives, every entry of the key is answered `NotAttempted` without a write. A rejected entry that
blocked the key does change the state, so the batch is written, and a retry after a lost response finds the block in
place and answers the same way.

If the dependency really should last only for the batch, keep the block in a wrapper around the state that `read`
creates unblocked and `write` strips; `FoldWrite` writes whenever the wrapper changed, so a batch that only set the
block costs one write of an unchanged record.

!!! note "`step` must be total"
    `step` returns an outcome; it must not throw. An exception in `step` is a defect of the whole call, and every
    caller in the batch sees it. A rejection, a "not attempted", a validation error are all values of `O`.

## Putting it together

```scala
--8<-- "OrderedWrites.scala:wiring"
```

The [OrderedWrites example](https://github.com/EtaCassiopeia/zio-ferry/blob/main/examples/src/main/scala/ferry/examples/OrderedWrites.scala)
runs this against a store that throttles one request in seven. It writes 300 entries in about twenty requests, each
entry exactly once, ordered by effective day.

## Why not cache the state?

`FoldWrite` reads the state for every batch. Keeping it in memory between batches would save a read, but make the
process a second source of truth: wrong after a crash, after another process wrote the key, or after a routing change.
The store stays the only source of truth. See [0005](../decisions/0005-fenced-single-writer.md) and
[Running on a fleet](../architecture/fleet.md).

## Two writes: commit, then project

A store that writes a batch in two steps, a commit and then a projection into a read-side view, needs a second fenced
write, a way to report "committed, view pending", and a way to finish the projection after a crash.
[`ProjectedWrite`](projected-write.md) wraps a `FoldWrite` and adds all three.

!!! note "All-or-nothing group operations"
    If the store's group operation rejects a whole group for one invalid input, validate in `call` before the round
    trip, or split and retry inside the call. The key stays busy meanwhile, so order is preserved.
