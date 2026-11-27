<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.png">
    <img src="docs/assets/logo.png" width="240" alt="zio-ferry">
  </picture>
</h1>

[![CI](https://github.com/EtaCassiopeia/zio-ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/EtaCassiopeia/zio-ferry/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.etacassiopeia/zio-ferry_3)](https://central.sonatype.com/artifact/io.github.etacassiopeia/zio-ferry_3)
[![Docs](https://img.shields.io/badge/docs-etacassiopeia.github.io%2Fzio-ferry-blue)](https://etacassiopeia.github.io/zio-ferry/)

Micro-batching for ZIO 2. Concurrent calls are combined into batched calls to an external system, with a cap on how
many batches run at once and how many inputs may wait. Each caller gets its own share of the result.

<p align="center">
  <img src="docs/assets/ferry.svg" width="100%" alt="Ferry.perKey: an idle key's input is sent at once; a busy key's inputs wait in its lane while its call is in flight and leave together in its next call, in order, one call per key at a time.">
</p>

```scala
libraryDependencies += "io.github.etacassiopeia" %% "zio-ferry" % "0.3.0"
```

```scala
for
  cfg   <- ZIO.fromEither(FerryConfig.make(maxBatchSize = 5, maxBatchDelay = 5.millis, maxInFlightBatches = 2, maxQueueing = 50))
  ferry <- Ferry.keyed(cfg)((countries: NonEmptyChunk[String]) => pricingApi.fetch(countries))
  price <- ferry("NL") // IO[FerryError[E], Option[Price]]
yield price
```

- **`Ferry.make` / `Ferry.keyed`**: batch calls, by size and by how long the oldest input may wait.
- **`Ferry.perKey`**: ordered writes per entity, one call in flight per key, bounded by count and weight; idle keys
  are sent at once.
- **`FoldWrite`, `ProjectedWrite`**: the fenced, idempotent write to pair with it, safe on a fleet.

Every caller gets an answer, overload fails fast, and per-key order holds. This is checked by deterministic simulation
and by TLA+ models that the code is mechanically kept in step with.

**[Documentation](https://etacassiopeia.github.io/zio-ferry/)**: [getting started](https://etacassiopeia.github.io/zio-ferry/getting-started/) ·
[guide](https://etacassiopeia.github.io/zio-ferry/guide/batching/) ·
[architecture](https://etacassiopeia.github.io/zio-ferry/architecture/) ·
[verification](https://etacassiopeia.github.io/zio-ferry/verification/) ·
[API](https://etacassiopeia.github.io/zio-ferry/api/)
