# 0020. Check the fenced writer's simulation against its model

Status: Accepted.

## Context

`FencedWriter.tla` models the deployment pattern `perKey` is meant for: nodes that write a key through a store with
conditional appends, under arbitrary routing, crashes and lost answers. `FencedWriterSimulation` runs the real `perKey`
through the same faults. The two were written separately, so they could drift as the protocol's model and code could
([0017](0017-model-conformance.md)).

They also disagreed on granularity. In the model, the store's answer also answered the node's clients, in one step; in
the code, the store answers the node, and the node answers its clients in a later step, so a crash can fall between
them. The model had no throttling, and no node that gives up after its retries, which the code has.

## Decision

- The model takes the code's steps. The store's delivery and the node's `Reply` are separate steps. `Throttle`,
  `GiveUp` and `Abandon` (a crashed node's batch, cut short) are added. A crashed node no longer takes, but the batch
  it was running still ends. The order of an append is the node's choice, not a fixed one.
- Every step sets an `act` record, as in `Ferry.tla`, and `Vocabulary` lists them.
- The simulation records `perKey`'s actions (through a `Recorder`) and the store's reads, deliveries and refusals in
  one STM log, so its trace is in the order the steps took effect. The store's steps run in STM for that reason.
- `FencedWriterConformance` maps the ferry's actions onto the model's: `Enqueue` is `Send`; `Claim` is `Send` then
  `Take`; a completion is `Reply`, `GiveUp` or `Abandon` by its outcome; `Close` is `Crash`. Each key's projection is
  checked against `FencedWriterTrace.tla`, with the model's switches set from the code's `Writer`.

## Consequences

- Each safeguard is shown to be needed from both sides. Model checking shows the model is unsafe without it. Trace
  validation shows that a writer without it is rejected by the model with it, and is accepted by the model without
  it.
- The passing configuration has about 790 thousand distinct states, up from 19 thousand for the coarser model, and
  still checks in seconds. A `VIEW` leaves `act` out of the state, so the history variable adds no states.
- The projection models (`ProjectedWriter`, `ProjectedWriterPartial`) are not yet checked this way.
