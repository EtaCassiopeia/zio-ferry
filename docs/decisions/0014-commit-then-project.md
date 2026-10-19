# 0014. Package "commit, then project" as `ProjectedWrite`, with the marker in the store

Status: Accepted. Revises the out-of-scope list of [`docs/design/ordering.md`](../design/ordering.md), Part C.3.

## Context

Some stores write a key's record in two dependent conditional writes: a commit that appends the new entries, fenced on
the record read, and a projection that writes the record's read-side view, fenced on the record being as committed.
The record carries a marker saying whether its projection is pending, so a process that fails between the writes
leaves a committed, unprojected record that any process can finish. A caller whose input is committed but not yet
projected is answered "accepted", not failed.

`FoldWrite` ([0012](0012-fold-write-helper.md)) packages one fenced write. Putting both writes inside its `write`
leaves no way to retry the projection alone, no way to report "committed, not projected", and no way to finish a
projection from the store; it also takes the real write out of what `FencedWriter.tla` checks.

The ordering design note listed "two-phase or pending workflows" among the application's concerns, out of scope for
the library. This record reverses that: the maintainer decided the library should carry this shape, once a store
needed it, for the same reason 0012 packaged the one-write round trip. It is a new decision, not the trigger of an
earlier deferral.

## Decision

`ProjectedWrite(fold, pending, project)` is the round trip: `fold`'s read, fold and fenced commit, then
`project(key, state)`, fenced on the store still holding `state`, and skipped unless `pending(state)`.

- Every outcome of a call is `Projected` if the view is known to include the call's inputs when it returns (its own
  projection landed, or a re-read found nothing pending), and `Committed` otherwise: durable, projection not confirmed.
  Both carry the outcome `step` gave the input.
- The call fails only when the commit is not known to have happened ([0006](0006-per-input-outcomes.md)); retrying is
  then safe, because `step` answers inputs already applied. A projection that exhausted its retries yields `Committed`.
- The first projection uses the state just committed, without reading it; any failure falls back to `resume`.
- `resume(key)` runs the projection alone, retried with the same schedule. It is safe to run anywhere, at any time,
  concurrently with batches and other calls of `resume`.

The marker is part of the state the store holds and `read` returns. The library never caches it, so
[0005](0005-fenced-single-writer.md) is unchanged: the store stays the only source of truth, and crash recovery is still
"read again and continue". `resume` is that, with no new inputs.

## Consequences

- A client of a `Committed` outcome must treat its input as durable and its view as late, and must not resubmit to
  hurry it: a sweeper the application runs, or the key's next batch, finishes it. Without either, it stays pending.
- Finding pending records is the application's query; the library provides `resume`.
- The commit must never move the stored marker backwards: it leaves the marker as stored, or is fenced on the whole
  record read, marker included. Otherwise a commit racing a sweeper could restore an old marker and have entries
  projected twice.
- `project`'s fence must cover the marker and the version: the marker refuses a second projection of one record, the
  version keeps a stale projection from clearing a marker that a later commit set. `ProjectedWriteSpec`,
  `ProjectedWriterSimulation` and `formal/ProjectedWriter.tla` check both. The model's `UnfencedProject`, `NoResume` and
  `EarlyAnswer` variants must fail.
- `FoldWrite`'s public API is unchanged; `ProjectedWrite` reuses its round trip.
- Deferred until a store needs them: a projection that can land for some entries of one batch and not others, and the
  projection's error carried on `Committed`. Built by [0015](0015-per-entry-projection.md).
