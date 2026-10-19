<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.png">
    <img src="docs/assets/logo.png" width="240" alt="zio-ferry">
  </picture>
</h1>

[![CI](https://github.com/EtaCassiopeia/zio-ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/EtaCassiopeia/zio-ferry/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.etacassiopeia/zio-ferry_3)](https://central.sonatype.com/artifact/io.github.etacassiopeia/zio-ferry_3)
[![Docs](https://img.shields.io/badge/docs-etacassiopeia.github.io%2Fzio-ferry-blue)](https://etacassiopeia.github.io/zio-ferry/)

A ZIO 2 micro-batching bulkhead. It coalesces concurrent calls into batched calls to an external system, caps how
many batches run at once and how many inputs may wait, and gives each caller its own share of the result.

<p align="center">
  <img src="docs/assets/ferry.svg" width="100%" alt="Ferry.perKey: an idle key's input is sent at once; a busy key's inputs wait in its lane while its call is in flight and leave together in its next call, in order, one call per key at a time.">
</p>

**[Documentation](https://etacassiopeia.github.io/zio-ferry/)**: [getting started](https://etacassiopeia.github.io/zio-ferry/getting-started/),
[guide](https://etacassiopeia.github.io/zio-ferry/guide/batching/),
[architecture](https://etacassiopeia.github.io/zio-ferry/architecture/),
[verification](https://etacassiopeia.github.io/zio-ferry/verification/),
[API](https://etacassiopeia.github.io/zio-ferry/api/).

```scala
libraryDependencies += "io.github.etacassiopeia" %% "zio-ferry" % "0.1.0"
```

```scala
for
  cfg   <- ZIO.fromEither(FerryConfig.make(maxBatchSize = 5, maxBatchDelay = 5.millis, maxInFlightBatches = 2, maxQueueing = 50))
  ferry <- Ferry.keyed(cfg)((countries: NonEmptyChunk[String]) => pricingApi.fetch(countries))
  price <- ferry("NL") // IO[FerryError[E], Option[Price]]
yield price
```

For writes that must stay in order per entity, `Ferry.perKey` batches each key separately, with at most one call in
flight per key. Idle keys are not batched at all; busy keys get batches that grow with load, sent in an
[order](https://etacassiopeia.github.io/zio-ferry/guide/ordering/) of your choice. `FoldWrite` packages the fenced,
idempotent "read once, apply, conditional write" call to pair it with, and `ProjectedWrite` the same followed by a
fenced projection into a read-side view.

Every caller gets an answer, overload fails fast, and per-key order holds. This is checked by fault-injecting
simulations, stress tests and TLA+ models with weakened variants that must fail.

```sh
sbt testFull
formal/check.sh path/to/tla2tools.jar
```

Like a ferry, each batch carries whoever gathered while the previous one was away, and leaves when it is back.

Formerly BulkDike. A Scala 3 rewrite of the queueing core of
[api-aggregation-service](https://github.com/EtaCassiopeia/api-aggregation-service), inspired by
[rezilience](https://github.com/svroonland/rezilience). Scala 3.3 LTS / 3.9, ZIO 2.1, sbt 2, JDK 17+. Apache 2.0.
