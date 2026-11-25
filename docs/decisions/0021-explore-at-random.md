# 0021. Explore decisions at random: guidance did not beat chance

Status: Accepted.

## Context

A run is a function of its decisions, so a run that reached something new can be revisited and varied, as
coverage-guided fuzzers vary inputs. That could find more behaviour for the same number of runs than drawing every run
from a fresh seed. It was worth trying only if it beats chance, measured on the same budget.

## Decision

Measure before adopting. `Explore` (in the testkit) runs a program by three strategies and counts the distinct
fingerprints seen:
- `random` draws every run from a fresh seed;
- `guided` keeps every run that reached a new fingerprint and varies one of them, either by replaying a random prefix
  and continuing at random (`Fork`) or by changing up to three of its decisions (`Mutate`).

The fingerprint is the *shape* of each protocol state a run passes through. `ferry.explore.Coverage` replays the
recorded trace through `Protocol`. For each key it keeps the lane's length, schedule and calls in flight. It also keeps
the length of the ready line, the batches and direct calls in flight, the callers pending, and whether admission is
open.

Measured with 200 runs per strategy, on 10 scenarios of each family, with up to 24 callers
(`sbt "core/Test/runMain ferry.explore.Coverage 200 10 24"`):

| Family | random | Fork | Mutate |
|---|---|---|---|
| `Make` | 220 | 213 (0.97) | 199 (0.90) |
| `PerKeyDirect` | 467 | 423 (0.91) | 383 (0.82) |
| `PerKeyBatched` | 345 | 334 (0.97) | 310 (0.90) |
| `Weighted` | 391 | 363 (0.93) | 334 (0.85) |

Neither kind of guidance beats chance, so the simulation explores at random.

## Consequences

- Seeds stay the only way runs are drawn. `Explore` and `Coverage` remain as the apparatus of this measurement, so it
  can be repeated when the simulator or the fingerprint changes.
- Most of the variance is between scenarios (from 2 to 172 shapes), not between strategies: what a run can reach is
  set mostly by its scenario. More, and more varied, scenarios are the cheaper way to more coverage.
- Guidance aimed at the model's states, or at rare fingerprints, was not tried. A new attempt must beat this table.
