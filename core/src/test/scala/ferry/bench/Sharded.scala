package ferry.bench

import zio.*

import ferry.{Ferry, PerKeyConfig}

/**
 * An experiment: a per-key ferry split into `shards` independent ferries, each owning the keys whose hash falls in it.
 * A key always lands in the same shard, so per-key order and "one call per key" hold as before; the shards share
 * nothing, so they run on different cores without contending. The limits are split between them: each shard gets its
 * share of the calls in flight and of the waiting room.
 */
object Sharded:

  def perKey[K, I, B, A](
    shards: Int,
    maxBatchSize: Int,
    maxInFlightBatches: Int,
    maxQueueing: Int,
    key: I => K
  )(call: (K, NonEmptyChunk[I]) => UIO[B])(extract: (I, B) => A): URIO[Scope, Ferry[I, Nothing, A]] =
    val config = PerKeyConfig
      .make(
        maxBatchSize = maxBatchSize,
        maxInFlightBatches = (maxInFlightBatches / shards) max 1,
        maxQueueing = (maxQueueing / shards) max 1
      )
      .toOption
      .get
    ZIO.foreach(Chunk.fromIterable(0 until shards))(_ => Ferry.perKey(config, key)(call)(extract)).map { parts =>
      new Ferry[I, Nothing, A]:
        def apply(input: I) = parts(Math.floorMod(key(input).hashCode, shards))(input)
        def metrics         =
          ZIO.foreach(parts)(_.metrics).map { ms =>
            Ferry.Metrics(ms.map(_.queued).sum, ms.map(_.inFlightBatches).sum)
          }
    }
