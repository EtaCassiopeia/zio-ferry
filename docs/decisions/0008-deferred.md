# 0008. Deferred until a measurement or a use case asks for them

Status: Deferred

Each of these was considered and left out for now. Each names what would bring it back.

## Built-in sharding

Several independent ferries, each owning `hash(key) % n` of the keys, would spread contention on the single `TRef` of
[0003](0003-per-key-batching.md). Every per-key guarantee is local to a key, so applications can already do this
themselves without loss. **Revisit when** a benchmark shows STM retries on the lanes `TRef` limiting throughput.

## Lock-free admission for `Ferry.make`

Admission there is serialised with close by a `Ref.Synchronized`. A counter of admissions in progress plus a closed flag
would avoid the lock, but it is exactly the mechanism the formal model shows to be load-bearing, so a replacement must
be modelled first. `Ferry.perKey` already admits through an STM transaction. **Revisit when** profiling shows the lock
on a hot path.

## Benchmarks (JMH)

Throughput and allocation per call (`-prof gc`) for both constructors. **Revisit before** any of the performance items
above: they should be justified by numbers, not intuition.

## Adaptive concurrency

Adjusting `maxInFlightBatches` on throttling signals (add one slot after a success, halve on throttling) would protect a
downstream better than a static limit. Per key, concurrency is fixed at one by design; a throttled batch call should
back off and retry inside its own slot, which slows exactly the throttled key. **Revisit when** a downstream's capacity
varies enough that a static `maxInFlightBatches` is either wasteful or overloads it.

## Larger model checking

- *Symmetry reduction* in TLC is unsound for liveness properties, which the `Ferry` and `PerKey` configurations check.
  It could be added to safety-only configurations to check more callers.
- *Simulation mode* (`tlc -simulate`) explores long random behaviours of larger instances.
- *Apalache with an inductive invariant* would prove safety for any number of callers and keys, not just small
  instances.
- *Trace validation*: no longer deferred. It is done, in both directions, by [0017](0017-model-conformance.md).

**Revisit when** the protocol changes.
