# 0022. Check the projected writers' simulations against their models

Status: Accepted.

## Context

[0020](0020-writer-conformance.md) brought the fenced writer under trace validation. The two projection models,
`ProjectedWriter.tla` ([0014](0014-commit-then-project.md)) and `ProjectedWriterPartial.tla`
([0015](0015-per-entry-projection.md)), had the same gap. They also had the same kind of granularity drift, and more of
it:
- In `ProjectedWriter`, a landed commit put its projection on the wire in the same step. In the code the node sends it
  later, so a crash between the two leaves nothing on the wire.
- `ProjectedWriter` tracked the view's length, where the store keeps a marker.
- In `ProjectedWriterPartial`, every row of a projection went on the wire at once. The code reads which rows the view
  has, then writes the missing ones one at a time, in log order, waiting for each.

## Decision

- Both models take the code's steps, as `ProjectedWrite` makes them through the store: read, commit, project (or read
  the rows, write each row, clear), and answer, each its own step. Each step sets an `act` record. Throttling, a failed
  commit and an abandoned batch are added, as in `FencedWriter`. The sweeper is a projector whose reads are steps only
  when it finds work.
- The simulations record each node's `perKey` actions and the store's steps, including the sweeper's, in one STM log,
  each with who made it. The mapping of the ferry's actions is shared by all three writers (`WriterTraces`).
- Each safeguard of each writer is a switch of its model and a flag of the code's writer. A writer without one is
  rejected by the model with it, and accepted by the model without it.
- A delivery's record says whether the write applied. Without that, rows written unconditionally were accepted by the
  model with idempotent rows, because a duplicate row changed no later step's record.

## Consequences

- Every TLA+ model in the repository is now checked against the code it describes.
- The refined models are larger. `ProjectedWriter` has about a million states and checks, with liveness, in about
  25 seconds. `ProjectedWriterPartial` is checked in two configurations:
  - safety at the original bounds, with a `VIEW` that leaves `act` out, in about 16 seconds;
  - liveness with one fault and no view, because liveness checking must see every state, in about two minutes.

  The coarser model was 617 thousand states, checked in 17 seconds.
- The models' `NoResume` variants have no counterpart in the code, which always resumes. They stay model-only
  arguments for resume and the sweeper.
