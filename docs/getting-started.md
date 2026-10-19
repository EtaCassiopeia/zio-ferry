# Getting started

## Requirements

- Scala 3.3 LTS or later. The library is compiled against 3.3, so any Scala 3 project can use it.
- ZIO 2.1.
- JDK 17 or later.

## Add the dependency

Ferry is published to Maven Central:

```scala
libraryDependencies += "io.github.etacassiopeia" %% "zio-ferry" % "0.2.0"
```

Its only dependency is ZIO itself. Versions follow early semver: before 1.0, a minor version may change the API.

## A first ferry

A ferry lives in a `Scope`. Closing the scope stops its workers and interrupts any caller still waiting, so create it
once, at startup, next to the client it protects.

The common case is an API that answers a batch of inputs with one result per distinct input. `Ferry.keyed` covers
it:

```scala
import ferry.*
import zio.*

--8<-- "Snippets.scala:first-ferry"
```

`prices("NL")` waits until a batch carrying `"NL"` returns, then answers with that country's price, or `None` if the
batch result has no entry for it. Each distinct input is sent once per batch, even if several callers ask for it.

`FerryConfig.make` and `PerKeyConfig.make` return an `Either` that lists every invalid setting at once, so a
misconfiguration fails at startup, not on the first call.

## Run the examples

Two runnable programs live in [`examples/`](https://github.com/EtaCassiopeia/zio-ferry/tree/main/examples/src/main/scala/ferry/examples):

```sh
sbt "examples/runMain ferry.examples.PricingAggregation"
sbt "examples/runMain ferry.examples.OrderedWrites"
```

- **PricingAggregation** coalesces concurrent price lookups and wraps each batch call in a timeout, a retry and a
  circuit breaker.
- **OrderedWrites** appends entries to per-entity logs through `perKey`, ordered by effective day, with a fenced,
  idempotent write that survives throttling and conflicts.

## Next

- [Batching calls](guide/batching.md): `make` and `keyed` in depth.
- [Per-key batching](guide/per-key.md): ordered writes per entity.
- [Configuration and tuning](guide/configuration.md): every setting, and how to choose it.
