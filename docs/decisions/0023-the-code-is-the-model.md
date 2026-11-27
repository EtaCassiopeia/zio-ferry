# 0023. Check the protocol directly: the code is the model

Status: Accepted. Supersedes the protocol's half of [0017](0017-model-conformance.md); the writer models keep it.

## Context

[0017](0017-model-conformance.md) kept the protocol in two descriptions, `Protocol.scala` and `formal/Ferry.tla`, and
checked each against the other in both directions: recorded runs validated by TLC, TLC's behaviours replayed through
the code, and a shared vocabulary of action names. The checks worked, and they exist only because the protocol was
written twice.

[0016](0016-one-protocol.md) had already made the protocol a pure value whose only choices are its environment's:
callers arriving, deadlines passing, calls returning, callers giving up, the ferry closing. Such a state machine can be
explored directly, as TLC explores a model: apply every environment action from every reachable state of a small
instance, and check the properties in each.

## Decision

- **The protocol is checked by exploring the code itself** (`ferry.check.ProtocolSpace`, on the generic
  `ferry.testkit.check.StateSpace`). Breadth first over small instances, through [`Engine.Rules`](../architecture/engine.md),
  with time as an event. It checks the safety properties in every state, liveness once the environment stops (every
  caller let in is answered), and that the state graph has no cycle.
- **The properties are stated over what an observer sees**: who was let in, in what order per key, what was sent, who
  was answered. Two of them were implied by the model's actions and never written down: an input whose caller gave up
  while it waited is never sent, and ready lanes are served first come, first served. Without them, two of the planted
  drifts broke no property.
- **Recorded runs of the engine are replayed through the protocol** (`ProtocolSpace.replay`): each recorded step must
  be exactly the step the protocol takes from the state the run reached, and every property must hold after it. This
  replaces trace validation for the protocol. It checks the shell against the core, and the core's properties on the
  paths the real engine took.
- **`formal/Ferry.tla` and its trace module, configurations and variants are removed**, with the code that wrote traces
  for it and replayed its behaviours. The writer models stay ([0020](0020-writer-conformance.md),
  [0022](0022-projection-conformance.md)): they describe nodes, a store and a network that are not the library's code,
  so there is nothing to explore in their place.
- The STM shell stays. A single-owner shell (one fiber applying commands from a queue) was built and benchmarked: it is
  no faster, since both shells have one serial point per ferry, and it is better only on tail latency under contention.
  Splitting a ferry by key hash is what raises throughput (about three times with 8 shards on 10 cores, under 256 and
  1024 callers); it is measured in `ferry.bench.FerryBench` and not yet part of the API.

## Consequences

- The protocol and its checks cannot drift apart: there is one description of the protocol. A change to `Protocol` is
  checked by the next test run, with nothing to update by hand.
- The explorer agrees with TLC on every configuration `Ferry.tla` checked, and catches every weakened variant that can
  be written as code. One variant, admission split from its check, cannot be written at all: `submit` is one method.
  Each configuration explores in under a second; TLC took several.
- Replay is cheap enough to run on every simulated run, including the heavier runs of up to 24 callers that were too
  costly to hand to TLC.
- Liveness is weaker: "every caller is answered once the environment stops", on an acyclic graph, rather than TLA+'s
  temporal properties under fairness. For this protocol, whose every step consumes something, the two coincide.
- There is no longer a short, abstract specification of the protocol to read. The properties in `ProtocolSpace` and the
  architecture pages take its place for readers.
- The ready-lane fairness property observes the lanes' schedule from the protocol's own state: a drift in how lanes
  become ready would be trusted.
