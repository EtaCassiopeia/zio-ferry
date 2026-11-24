# Design decisions

Each record states a decision, why it was made, and what it costs. Records are not edited after the fact except to
mark them superseded; a changed decision gets a new record. Proposals not yet decided live in
[`docs/design`](../design/ordering.md).

| #                                       | Decision                                                                 | Status   |
|-----------------------------------------|--------------------------------------------------------------------------|----------|
| [0001](0001-single-queue-batcher.md)    | One admission queue, one batch assembled at a time                       | Implementation superseded by 0016 |
| [0002](0002-layered-verification.md)    | Verify with tests, model checking and mutation testing together          | Partly superseded by 0009 |
| [0003](0003-per-key-batching.md)        | Per-key batching with one batch in flight per key                        | Accepted; implementation superseded by 0016 |
| [0004](0004-dispatch-on-completion.md)  | Dispatch a key's next batch when its previous one completes              | Accepted |
| [0005](0005-fenced-single-writer.md)    | Pair per-key batching with a fenced, idempotent write                    | Accepted |
| [0006](0006-per-input-outcomes.md)      | Report outcomes per input; fail a batch only when it did not happen      | Accepted |
| [0007](0007-bound-by-weight.md)         | Bound batches and the waiting room by weight as well as count            | Accepted |
| [0008](0008-deferred.md)                | Deferred: sharding, lock-free admission, adaptive concurrency, and more | Deferred |
| [0009](0009-drop-mutation-testing.md)   | Drop mutation testing                                                    | Accepted |
| [0010](0010-direct-calls-for-idle-keys.md) | Send an idle key's input directly; batch only keys with concurrent work | Accepted |
| [0011](0011-pluggable-dispatch-order.md) | A key's waiting inputs are sent in a pluggable order                    | Accepted |
| [0012](0012-fold-write-helper.md)       | Package "read once, apply, conditional write" as `FoldWrite`             | Accepted |
| [0013](0013-rename-to-ferry.md)        | Rename BulkDike to Ferry                                                 | Accepted |
| [0014](0014-commit-then-project.md)    | Package "commit, then project" as `ProjectedWrite`, marker in the store | Accepted |
| [0015](0015-per-entry-projection.md)   | Project per entry: a projection may land for some entries, and `Committed` says why | Accepted |
| [0016](0016-one-protocol.md)           | One protocol for every constructor, as a pure state machine in one `TRef` | Accepted |
| [0017](0017-model-conformance.md)      | Check the code against the model in both directions                      | Accepted |
| [0018](0018-deterministic-simulation.md) | Simulate deterministically: a run is a function of its decisions       | Accepted |
| [0019](0019-answers-after-commit.md)   | Deliver answers after a step commits                                     | Accepted |
