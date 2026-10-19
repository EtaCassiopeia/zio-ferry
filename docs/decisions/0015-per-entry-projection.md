# 0015. Project per entry: a projection may land for some entries of a batch, and `Committed` says why

Status: Accepted. Builds the two things [0014](0014-commit-then-project.md) deferred.

## Context

`ProjectedWrite` ([0014](0014-commit-then-project.md)) projects a record in one fenced write: the view is one
object per record, and the projection lands whole or fails. A store appeared whose view is one row per entry,
each row its own conditional write. There, one throttled row out of many leaves the record partially projected:
most entries are in the view, one is not. Reported all-or-nothing, every input of the call is answered
`Committed` although most are visibly projected, no reason is attached, and the sweeper re-projects the whole
record for one row.

## Decision

`project(key, state)` answers with what it left pending, `Projection[Id, E]`: for each entry whose row is not
known to be in the view, why. Empty means the view includes the whole record and the marker is cleared. Each
input's outcome is its own: `Projected` if the verdict does not list it, `Committed(provisional, cause)` with
its own cause otherwise. When `project` fails and `resume` fails after its retries, every input is `Committed`
with that error. `resume` answers the same `Projection`, for sweepers to log.

The store's contract for a view of many rows: each row write is idempotent (a row already present is present,
never a duplicate and never a cause); the marker clears only when every row is present, and only if the store
still holds the record, version and marker. `project` derives rows and ids from the committed record alone,
because `resume` has no inputs: a state that cannot yield them is not a candidate for per-entry projection.

A verdict with entries pending is final for the call: the library does not retry it, because `fold.retry` is
driven by one error and a partial verdict has none that is honest. The store owns its per-row retries and
chooses between failing the effect (re-read and retry the round trip) and reporting what is pending (answer
now, the sweeper finishes). A store whose view is one object per record uses `ProjectedWrite.wholeRecord`,
whose `project` never leaves anything pending, and behaves as under 0014.

## Consequences

- `Outcome[+O]` becomes `Outcome[+E, +O]` and `Committed` carries its cause; `project`'s result type and
  `resume`'s change. The release is 0.3.0; the migration is mechanical (`wholeRecord`, a second pattern
  variable, `.unit` on `resume`).
- The library still holds nothing between calls ([0005](0005-fenced-single-writer.md)): which rows are missing
  is the store's knowledge, read from the record and the view.
- A client of a `Committed` outcome can now tell a throttle from a bug; it must still treat the input as durable
  and the view as late, and not resubmit to hurry it.
- Rows need no fence of their own, only idempotence: a row is a function of its immutable entry, so an older
  projection writes the same row a newer one would. The marker clear is what the fence protects.
- `ProjectedWriteSpec`, `PartialProjectionSimulation` and `formal/ProjectedWriterPartial.tla` check the laws;
  the model's `UnconditionalRows`, `EarlyClear`, `UnfencedClear` and `NoResume` variants must fail.
  `formal/ProjectedWriter.tla` remains the whole-record model.
