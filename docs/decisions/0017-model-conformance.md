# 0017. Check the code against the model in both directions

Status: Accepted. Takes trace validation out of [0008](0008-deferred.md). The protocol's half is superseded by
[0023](0023-the-code-is-the-model.md): the protocol is checked directly, and `Ferry.tla` is retired. The writer models
keep this decision.

## Context

A model checked by TLC says nothing about the code unless the two describe the same protocol, and a hand-written
model drifts: someone splits a transaction, relaxes a guard, or simplifies the model with a guard the code lacks, and
every test still passes. "One action per atomic step" helped, but nothing enforced it.

## Decision

Three rules, and the checks they make possible:

1. **Every step of the code is an action of the model** ([0016](0016-one-protocol.md)). Inside the step's
   transaction, the engine hands the step's actions to a `Recorder`, so a recorded trace is in linearization order.
2. **The model keeps a history variable `act`** holding the record of the action just taken. That record has the
   same shape as the code's `Action`, so there is one shared event schema and one mapping (`Conformance.encode`).
3. **The model's free choices are only the environment's**: arrivals, deadlines passing (`Due`), outcomes, callers
   giving up, close. Which lane a worker takes, and how much of it, the model decides as the code does.

The checks:

- **Code → model (trace validation).** A recorded run is written as a TLA+ module. `FerryTrace.tla` takes one model
  step per event, requiring `act' = Trace[i + 1]`, and TLC reports the trace accepted, or rejected after *n* events.
- **Model → code (replay).** TLC generates random behaviours of each configuration (`-simulate file=`). Each step's
  `act` is applied to the code's `Protocol`, which must take the same action and reach the same state.
- **Vocabulary.** The `Action` cases are the model's `Vocabulary`, and every action occurs in some simulated run and
  in some model behaviour.
- **Planted drift.** Wrong engine rules and altered models must be caught, as tests (`DriftSpec`).
- **Verdicts are three-way.** Accepted, rejected, or skipped/tool error. Neither of the last two is ever a pass.

## Consequences

- Drift between `Protocol` and `Ferry.tla` fails the build, with a reproducible counterexample.
- The coverage check found that no model configuration could reject an input (the waiting room never filled), so
  `TurnedAwayNeverSent` had held vacuously. The configurations now have `MaxQueue = 2`.
- The tests need TLC. They look for `tla2tools.jar` in `$TLA2TOOLS`, at the root of the repository, or in the usual
  local installs, and CI provides it.
- This is bounded evidence: the runs simulated and the behaviours generated. Model checking still covers every
  behaviour, of the model.
