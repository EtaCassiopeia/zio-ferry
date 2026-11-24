package ferry

import zio.*
import zio.test.*

/**
 * Runs `Ferry.perKey` on the live clock and the real multi-threaded scheduler, where interleavings are not under the
 * test's control, and repeats each run to shake out races that a deterministic simulation cannot reach.
 */
object PerKeyStressSpec extends ZIOSpecDefault:

  private val configs =
    for
      maxBatchSize       <- Gen.int(1, 8)
      maxBatchDelayMs    <- Gen.elements(0, 0, 1, 3)
      maxInFlightBatches <- Gen.int(1, 6)
      maxQueueing        <- Gen.int(maxBatchSize, 512)
      maxBatchWeight     <- Gen.elements(Long.MaxValue, 8L, 20L)
    yield PerKeyConfig.make(maxBatchSize, maxInFlightBatches, maxQueueing, maxBatchDelayMs.millis, maxBatchWeight)

  private val keys = 12

  private def failing(batch: NonEmptyChunk[Int]): Boolean = batch.head % 7 == 0

  private val hangLimit = 20.seconds

  /**
   * Waits until a caller is parked awaiting its result, i.e. its input has been admitted (or it has already finished).
   */
  private def admitted(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
    fiber.status.repeatUntil {
      case _: Fiber.Status.Running => false
      case _                       => true
    }.unit

  def spec = suite("Ferry.perKey under real concurrency")(
    test("keeps each key to one batch in flight, in submission order, and accounts for every call") {
      check(configs) { cfgOrError =>
        for
          cfg     <- ZIO.fromEither(cfgOrError).orDie
          batches <- Ref.make(Chunk.empty[(Int, NonEmptyChunk[Int])])
          busy    <- Ref.make(Map.empty[Int, Int])
          overlap <- Ref.make(false)
          active  <- Ref.make(0)
          peak    <- Ref.make(0)
          direct  <- Ref.make(Chunk.empty[NonEmptyChunk[Int]])
          caller  <- FiberRef.make(false) // set by callers, so a call made by one rather than a worker is told apart
          exits   <- ZIO.scoped {
            for
              ferry <- Ferry.perKey(cfg, (_: Int) % keys, (id: Int) => (id % 5 + 1).toLong) {
                (key, ids: NonEmptyChunk[Int]) =>
                  // Only workers' batches count against maxInFlightBatches.
                  def enter(byCaller: Boolean) =
                    (if byCaller then direct.update(_ :+ ids)
                     else active.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n))) *>
                      busy
                        .modify(m => m.getOrElse(key, 0) -> m.updated(key, m.getOrElse(key, 0) + 1))
                        .flatMap(n => overlap.set(true).when(n > 0))
                  def exit(byCaller: Boolean) =
                    active.update(_ - 1).unless(byCaller) *> busy.update(m => m.updated(key, m(key) - 1))
                  ZIO.acquireReleaseWith(caller.get.tap(enter))(exit) { _ =>
                    batches.update(_ :+ (key -> ids)) *>
                      Random.nextIntBounded(2).flatMap(ms => ZIO.sleep(ms.millis)) *>
                      ZIO.fail(s"batch ${ids.head}").when(failing(ids)).unit
                  }
              }((id, _) => -id)
              // Each key's inputs are submitted one after the other (each caller is admitted before the next one
              // starts), so each key has a well-defined submission order; the keys themselves run in parallel.
              calls <- ZIO
                .foreachPar(0 until keys) { key =>
                  ZIO.foreach((0 until 1200).filter(_ % keys == key))(id =>
                    caller.locally(true)(ferry(id)).fork.tap(admitted).map(id -> _)
                  )
                }
                .map(_.flatten.sortBy(_._1).map(_._2))
              _ <- ZIO.foreachDiscard(calls.zipWithIndex.collect { case (f, i) if i % 5 == 0 => f })(_.interrupt.fork)
              result <- ZIO.foreach(calls)(_.await).timeout(hangLimit)
            yield result
          }
          sent        <- batches.get
          maxSeen     <- peak.get
          overlapping <- overlap.get
          byCallers   <- direct.get
        yield
          val ids     = sent.flatMap(_._2.toChunk)
          val batchOf = sent.flatMap((_, b) => b.map(_ -> b)).toMap
          val wrong   = exits.toList.flatMap(_.zipWithIndex).collect {
            case (Exit.Success(v), id) if v != -id || batchOf.get(id).forall(failing)   => id
            case (exit, id) if FerrySimulation.isRejected(exit) && batchOf.contains(id) => id
            case (Exit.Failure(c), id)
                if c.failureOption.exists(_.isInstanceOf[FerryError.Failed[?]]) &&
                  !batchOf.get(id).exists(failing) =>
              id
            case (Exit.Failure(c), id) if c.isInterruptedOnly && id % 5 != 0 => id
          }
          val outOfOrder = sent.groupBy(_._1).values.exists { b =>
            val order = b.flatMap(_._2.toChunk)
            order != order.sorted
          }
          assertTrue(
            exits.isDefined,
            ids.distinct.size == ids.size,
            !overlapping,
            wrong.isEmpty,
            !outOfOrder,
            maxSeen <= cfg.maxInFlightBatches,
            byCallers.forall(_.size == 1),
            byCallers.isEmpty || cfg.maxBatchDelay.isZero,
            sent.forall((key, b) => b.forall(_ % keys == key) && b.size <= cfg.maxBatchSize)
          )
      }
    } @@ Effort.samples(30),
    test("closing while callers keep arriving never strands one") {
      for
        cfg   <- ZIO.fromEither(PerKeyConfig.make(4, 2, 16)).orDie
        calls <- ZIO.scoped {
          for
            ferry <- Ferry.perKey(cfg, (_: Int) % 3)((_, ids: NonEmptyChunk[Int]) => ZIO.sleep(1.milli).as(ids.size))(
              (_, n) => n
            )
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
