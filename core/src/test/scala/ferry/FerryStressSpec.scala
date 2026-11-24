package ferry

import zio.*
import zio.test.*

/**
 * Runs Ferry on the live clock and the real multi-threaded scheduler, where interleavings are not under the test's
 * control, and repeats each run to shake out races that a deterministic simulation cannot reach.
 */
object FerryStressSpec extends ZIOSpecDefault:

  private val configs =
    for
      maxBatchSize       <- Gen.int(1, 8)
      maxBatchDelayMs    <- Gen.elements(0, 1, 3)
      maxInFlightBatches <- Gen.int(1, 4)
      maxQueueing        <- Gen.int(maxBatchSize, 256)
    yield FerryConfig.make(maxBatchSize, maxBatchDelayMs.millis, maxInFlightBatches, maxQueueing)

  private def failing(batch: NonEmptyChunk[Int]): Boolean = batch.head % 7 == 0

  private val hangLimit = 20.seconds

  def spec = suite("Ferry under real concurrency")(
    test("accounts for every call with many concurrent callers, interruptions and failing batches") {
      check(configs) { cfgOrError =>
        for
          cfg     <- ZIO.fromEither(cfgOrError).orDie
          batches <- Ref.make(Chunk.empty[NonEmptyChunk[Int]])
          active  <- Ref.make(0)
          peak    <- Ref.make(0)
          exits   <- ZIO.scoped {
            for
              ferry <- Ferry.make(cfg) { (ids: NonEmptyChunk[Int]) =>
                val track = active.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n))
                ZIO.acquireReleaseWith(track)(_ => active.update(_ - 1)) { _ =>
                  batches.update(_ :+ ids) *>
                    Random.nextIntBounded(2).flatMap(ms => ZIO.sleep(ms.millis)) *>
                    ZIO.fail(s"batch ${ids.head}").when(failing(ids)).as(ids.map(-_).toList)
                }
              }((id, _) => -id)
              calls <- ZIO.foreach(0 until 1500)(id => ferry(id).fork)
              _     <- ZIO.foreachDiscard(calls.zipWithIndex.collect { case (f, i) if i % 5 == 0 => f })(
                _.interrupt.fork
              )
              result <- ZIO.foreach(calls)(_.await).timeout(hangLimit)
            yield result
          }
          sent    <- batches.get
          maxSeen <- peak.get
        yield
          val batchOf   = sent.flatMap(b => b.map(_ -> b)).toMap
          val sentTwice = sent.flatMap(_.toChunk).size - batchOf.size
          val wrong     = exits.toList.flatMap(_.zipWithIndex).collect {
            case (Exit.Success(v), id) if v != -id || batchOf.get(id).forall(failing)   => id
            case (exit, id) if FerrySimulation.isRejected(exit) && batchOf.contains(id) => id
            case (Exit.Failure(c), id)
                if c.failureOption.exists(_.isInstanceOf[FerryError.Failed[?]]) &&
                  !batchOf.get(id).exists(failing) =>
              id
            case (Exit.Failure(c), id) if c.isInterruptedOnly && id % 5 != 0 => id
          }
          assertTrue(exits.isDefined, sentTwice == 0, maxSeen <= cfg.maxInFlightBatches, wrong.isEmpty)
      }
    } @@ Effort.samples(40),
    test("closing while callers keep arriving never strands one") {
      for
        cfg   <- ZIO.fromEither(FerryConfig.make(4, 2.millis, 2, 16)).orDie
        calls <- ZIO.scoped {
          for
            ferry <- Ferry.make(cfg)((ids: NonEmptyChunk[Int]) => ZIO.sleep(1.milli).as(ids.size))((_, n) => n)
            calls <- ZIO.foreach(0 until 200) { id =>
              (Random.nextIntBounded(4).flatMap(ms => ZIO.sleep(ms.millis)) *> ferry(id)).fork
            }
            _ <- Random.nextIntBounded(4).flatMap(ms => ZIO.sleep(ms.millis))
          yield calls
        }
        exits <- ZIO.foreach(calls)(_.await).timeout(hangLimit)
      yield assertTrue(
        exits.isDefined,
        exits.toList.flatten.forall {
          case Exit.Success(n)     => n >= 1 && n <= 4
          case Exit.Failure(cause) => cause.isInterruptedOnly || cause.failureOption.contains(FerryError.Rejected)
        }
      )
    } @@ Effort.repeats(100)
  ) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom @@ Effort.timeout(10.minutes)
