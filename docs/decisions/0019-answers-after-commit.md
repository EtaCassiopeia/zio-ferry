# 0019. Deliver answers after a step commits

Status: Accepted.

## Context

Completing callers' `TPromise`s inside the step's transaction made the answer atomic with the state change. But STM
wakes the fibers waiting on the `TRef`s a transaction wrote by walking its journal, which is ordered by identity hash.
The answered callers therefore woke in a different order on every run, and the same program and decisions gave
different runs under the deterministic simulator ([0018](0018-deterministic-simulation.md)).

## Decision

The transaction decides who is answered and with what, together with the state change and the record. The shell
delivers the answers through ordinary `Promise`s right after the commit, in the order the step lists them. Every path
that commits a step delivers before it can be interrupted: workers and direct calls are uninterruptible there, and so
are finalizers and `onInterrupt`.

## Consequences

- Only the workers wait on STM, all on the one `TRef`, which keeps the order of wake-ups deterministic for the small
  numbers of workers the simulations use.
- A caller is answered a moment after the step that answered it, never before it. The model's `answer` is the
  protocol's decision, which is still atomic with the step.
