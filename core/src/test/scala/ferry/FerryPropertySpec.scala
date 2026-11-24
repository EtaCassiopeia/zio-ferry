package ferry

import zio.*
import zio.test.*

object FerryPropertySpec extends ZIOSpecDefault:

  def spec = suite("Ferry properties")(
    test("keyed sends each distinct input once and answers every caller from the map") {
      check(Gen.chunkOfBounded(1, 20)(Gen.int(0, 6))) { inputs =>
        for
          cfg     <- ZIO.fromEither(FerryConfig.make(inputs.size, 1.hour, 1, inputs.size)).orDie
          sent    <- Ref.make(Chunk.empty[Chunk[Int]])
          results <- ZIO.scoped {
            Ferry
              .keyed(cfg) { (batch: NonEmptyChunk[Int]) =>
                sent.update(_ :+ batch.toChunk).as(batch.filter(_ % 2 == 0).map(n => n -> n * 10).toMap)
              }
              .flatMap(ferry => ZIO.foreachPar(inputs)(ferry(_)))
          }
          batches <- sent.get
        yield assertTrue(
          batches.size == 1,
          batches.head.sorted == inputs.distinct.sorted,
          results == inputs.map(n => Option.when(n % 2 == 0)(n * 10))
        )
      }
    },
    test("accepts a configuration exactly when every constraint holds, and reports each one that does not") {
      check(Gen.int(-1, 6), Gen.long(-2, 3), Gen.int(-1, 3), Gen.int(-1, 8)) { (size, delayMs, inFlight, queueing) =>
        val broken = List(size < 1, delayMs < 0, inFlight < 1, queueing < size).count(identity)
        FerryConfig.make(size, delayMs.millis, inFlight, queueing) match
          case Right(cfg) =>
            assertTrue(
              broken == 0,
              cfg.maxBatchSize == size,
              cfg.maxBatchDelay == delayMs.millis,
              cfg.maxInFlightBatches == inFlight,
              cfg.maxQueueing == queueing
            )
          case Left(invalid) => assertTrue(invalid.problems.size == broken)
      }
    }
  ) @@ Effort.timeout(10.minutes)
