package ferry

import zio.*
import zio.test.*
import zio.test.Assertion.*

object FerrySpec extends ZIOSpecDefault:

  private def config(
    maxBatchSize: Int = 5,
    maxBatchDelay: Duration = 5.seconds,
    maxInFlightBatches: Int = 1,
    maxQueueing: Int = 16
  ): UIO[FerryConfig] =
    ZIO.fromEither(FerryConfig.make(maxBatchSize, maxBatchDelay, maxInFlightBatches, maxQueueing)).orDie

  /** A batch call that records every batch it receives and answers `n -> s"result-$n"`. */
  final private case class Recorder(batches: Ref[Chunk[Chunk[Int]]]):
    def call(inputs: NonEmptyChunk[Int]): UIO[Map[Int, String]] =
      batches.update(_ :+ inputs.toChunk).as(inputs.map(n => n -> s"result-$n").toMap)

  private val recorder = Ref.make(Chunk.empty[Chunk[Int]]).map(Recorder(_))

  private def recordingFerry(cfg: FerryConfig, rec: Recorder) =
    Ferry.make(cfg)(rec.call)((n, results) => results(n))

  /** Waits until some fiber is blocked on the TestClock, i.e. a partial batch is waiting for its deadline. */
  private val batchWaiting: UIO[Unit] =
    TestClock.sleeps.repeatUntil(_.nonEmpty).unit

  /** Waits until `fiber` is parked, e.g. a caller whose input has been queued and is awaiting its result. */
  private def suspended(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
    // A fiber that has not started yet also reports Suspended, with no trace: it must have run and then parked.
    fiber.status.repeatUntil {
      case Fiber.Status.Suspended(_, trace, _) => trace != Trace.empty
      case Fiber.Status.Done                   => true
      case _                                   => false
    }.unit

  private def metricsReach(ferry: Ferry[?, ?, ?])(p: Ferry.Metrics => Boolean): UIO[Unit] =
    ferry.metrics.repeatUntil(p).unit

  def spec = suite("Ferry")(
    suite("batching")(
      test("dispatches a full batch immediately, without waiting for the delay") {
        for
          rec     <- recorder
          cfg     <- config(maxBatchSize = 5)
          ferry   <- recordingFerry(cfg, rec)
          results <- ZIO.foreachPar(1 to 5)(ferry(_))
          batches <- rec.batches.get
        yield assertTrue(
          results == (1 to 5).map(n => s"result-$n"),
          batches.size == 1,
          batches.head.sorted == Chunk(1, 2, 3, 4, 5)
        )
      },
      test("dispatches a partial batch exactly maxBatchDelay after its oldest input") {
        for
          rec     <- recorder
          cfg     <- config(maxBatchSize = 5, maxBatchDelay = 5.seconds)
          ferry   <- recordingFerry(cfg, rec)
          _       <- TestClock.adjust(4.seconds) // idle time must not count towards the deadline
          first   <- ferry(1).fork
          _       <- batchWaiting
          _       <- TestClock.adjust(3.seconds)
          second  <- ferry(2).fork
          _       <- suspended(second)
          _       <- TestClock.adjust(1.second)
          early   <- first.poll
          _       <- TestClock.adjust(1.second)
          results <- first.join.zip(second.join)
          batches <- rec.batches.get
        yield assertTrue(
          early.isEmpty,
          results == ("result-1", "result-2"),
          batches == Chunk(Chunk(1, 2))
        )
      },
      test("dispatches an overdue input as soon as a batch slot frees up, without restarting its delay") {
        for
          gate    <- Promise.make[Nothing, Unit]
          cfg     <- config(maxBatchSize = 2, maxBatchDelay = 5.seconds, maxInFlightBatches = 1)
          ferry   <- Ferry.make(cfg)((ids: NonEmptyChunk[Int]) => gate.await.when(ids.contains(1)))((n, _) => n)
          busy    <- ZIO.foreach(List(1, 2))(ferry(_).fork) // a full batch occupies the only slot
          _       <- metricsReach(ferry)(_.inFlightBatches == 1)
          overdue <- ferry(3).fork
          _       <- metricsReach(ferry)(_.queued == 1)
          _       <- TestClock.adjust(10.seconds)           // 3 has now waited twice its delay
          _       <- gate.succeed(())
          _       <- ZIO.foreach(busy)(_.join)
          _       <- TestClock.adjust(Duration.Zero)        // let the freed worker run, without moving time
          result  <- overdue.poll
        yield assertTrue(result.exists(_ == Exit.succeed(3)))
      },
      test("passes duplicates to make, and each caller gets its own result") {
        for
          rec     <- recorder
          cfg     <- config(maxBatchSize = 3)
          ferry   <- recordingFerry(cfg, rec)
          results <- ZIO.foreachPar(List(7, 7, 8))(ferry(_))
          batches <- rec.batches.get
        yield assertTrue(results == List("result-7", "result-7", "result-8"), batches.head.sorted == Chunk(7, 7, 8))
      },
      test("keyed passes each distinct input once and answers None for missing keys") {
        for
          seen  <- Ref.make(Chunk.empty[Chunk[Int]])
          cfg   <- config(maxBatchSize = 4)
          ferry <- Ferry.keyed(cfg) { (inputs: NonEmptyChunk[Int]) =>
            seen.update(_ :+ inputs.toChunk).as(inputs.filter(_ != 3).map(n => n -> n * 10).toMap)
          }
          results <- ZIO.foreachPar(List(1, 1, 2, 3))(ferry(_))
          batches <- seen.get
        yield assertTrue(
          results == List(Some(10), Some(10), Some(20), None),
          batches.head.sorted == Chunk(1, 2, 3)
        )
      }
    ),
    suite("load limiting")(
      test("rejects immediately when maxQueueing inputs are already waiting") {
        for
          gate     <- Promise.make[Nothing, Unit]
          cfg      <- config(maxBatchSize = 1, maxBatchDelay = Duration.Zero, maxInFlightBatches = 1, maxQueueing = 1)
          ferry    <- Ferry.make(cfg)((_: NonEmptyChunk[Int]) => gate.await)((n, _) => n)
          running  <- ferry(1).fork
          _        <- metricsReach(ferry)(_.inFlightBatches == 1)
          queued   <- ferry(2).fork
          _        <- metricsReach(ferry)(_.queued == 1)
          rejected <- ferry(3).exit
          _        <- gate.succeed(())
          done     <- running.join.zip(queued.join)
        yield assert(rejected)(fails(equalTo(FerryError.Rejected))) && assertTrue(done == (1, 2))
      },
      test("never runs more than maxInFlightBatches batch calls at once") {
        for
          gate    <- Promise.make[Nothing, Unit]
          current <- Ref.make(0)
          peak    <- Ref.make(0)
          cfg     <- config(maxBatchSize = 1, maxBatchDelay = Duration.Zero, maxInFlightBatches = 2)
          ferry   <- Ferry.make(cfg) { (_: NonEmptyChunk[Int]) =>
            ZIO.acquireReleaseWith(current.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n)))(_ =>
              current.update(_ - 1)
            )(_ => gate.await)
          }((n, _) => n)
          calls   <- ZIO.foreach(1 to 6)(ferry(_).fork)
          _       <- metricsReach(ferry)(m => m.inFlightBatches == 2 && m.queued == 4)
          _       <- gate.succeed(())
          results <- ZIO.foreach(calls)(_.join)
          maxSeen <- peak.get
        yield assertTrue(maxSeen == 2, results == (1 to 6))
      }
    ),
    suite("failures")(
      test("fails every caller of a batch whose call fails, and keeps serving later calls") {
        for
          attempt <- Ref.make(0)
          cfg     <- config(maxBatchSize = 2)
          ferry   <- Ferry.make(cfg) { (_: NonEmptyChunk[Int]) =>
            attempt.getAndUpdate(_ + 1).flatMap(n => ZIO.fail("upstream down").when(n == 0))
          }((n, _) => n)
          failed <- ZIO.foreachPar(List(1, 2))(ferry(_).exit)
          later  <- ZIO.foreachPar(List(3, 4))(ferry(_))
        yield assert(failed)(forall(fails(equalTo(FerryError.Failed("upstream down"))))) &&
          assertTrue(later == List(3, 4))
      },
      test("propagates a defect in the batch call to its callers instead of hanging them") {
        val boom = new RuntimeException("boom")
        for
          cfg   <- config(maxBatchSize = 2)
          ferry <- Ferry.make(cfg)((_: NonEmptyChunk[Int]) => ZIO.die(boom))((n, _: Nothing) => n)
          exits <- ZIO.foreachPar(List(1, 2))(ferry(_).exit)
        yield assertTrue(exits.forall(_.causeOption.flatMap(_.dieOption).contains(boom)))
      },
      test("turns an exception thrown by extract into a defect for that caller only") {
        val boom = new RuntimeException("bad input")
        for
          cfg   <- config(maxBatchSize = 2)
          ferry <- Ferry.make(cfg)((_: NonEmptyChunk[Int]) => ZIO.unit)((n, _) => if n == 1 then throw boom else n)
          exits <- ZIO.foreachPar(List(1, 2))(ferry(_).exit)
        yield assertTrue(exits.head.causeOption.flatMap(_.dieOption).contains(boom), exits(1) == Exit.succeed(2))
      }
    ),
    suite("interruption and shutdown")(
      test("leaves out the input of a caller interrupted before dispatch, which frees its place at once") {
        for
          rec     <- recorder
          cfg     <- config(maxBatchSize = 2)
          ferry   <- recordingFerry(cfg, rec)
          gone    <- ferry(1).fork
          _       <- batchWaiting  // 1 is in a partial batch, waiting for its deadline
          _       <- gone.interrupt
          second  <- ferry(2).fork // 1 has left: 2 starts a partial batch of its own
          _       <- suspended(second)
          early   <- rec.batches.get
          _       <- TestClock.adjust(5.seconds)
          result  <- second.join
          batches <- rec.batches.get
        yield assertTrue(early.isEmpty, result == "result-2", batches == Chunk(Chunk(2)))
      },
      test("interrupts callers still waiting when the scope closes") {
        for
          gate    <- Promise.make[Nothing, Unit]
          started <- Promise.make[Nothing, Unit]
          cfg     <- config(maxBatchSize = 1, maxBatchDelay = Duration.Zero, maxInFlightBatches = 1)
          callers <- ZIO.scoped {
            for
              ferry   <- Ferry.make(cfg)((_: NonEmptyChunk[Int]) => started.succeed(()) *> gate.await)((n, _) => n)
              running <- ferry(1).fork
              _       <- started.await
              queued  <- ferry(2).fork
              _       <- metricsReach(ferry)(_.queued == 1)
            yield (running, queued)
          }
          running <- callers._1.await
          queued  <- callers._2.await
        yield assertTrue(running.isInterrupted, queued.isInterrupted)
      },
      test("interrupts calls made after the scope has closed") {
        for
          cfg   <- config()
          ferry <- ZIO.scoped(Ferry.make(cfg)((_: NonEmptyChunk[Int]) => ZIO.unit)((n, _) => n))
          exit  <- ferry(1).exit
        yield assertTrue(exit.isInterrupted)
      }
    ),
    suite("FerryConfig")(
      test("accepts a sensible configuration") {
        assertTrue(FerryConfig.make(5, 5.seconds, 2, 10).isRight)
      },
      test("reports every problem at once") {
        val result = FerryConfig.make(0, (-1).second, 0, -1)
        assert(result.left.map(_.problems.size))(isLeft(equalTo(4)))
      },
      test("names each broken setting and the value it was given") {
        val problems = FerryConfig.make(0, (-1).second, 0, 0).left.toOption.toList.flatMap(_.problems)
        assertTrue(
          problems.exists(p => p.contains("maxBatchSize") && p.contains("got 0")),
          problems.exists(p => p.contains("maxBatchDelay") && p.contains("negative")),
          problems.exists(p => p.contains("maxInFlightBatches") && p.contains("got 0"))
        )
      },
      test("explains errors when turned into exceptions") {
        assertTrue(
          FerryError.Rejected.toException.getMessage.contains("queue is full"),
          FerryError.Failed("upstream down").toException.getMessage.contains("upstream down")
        )
      },
      test("requires the queue to hold at least one full batch") {
        assert(FerryConfig.make(5, 1.second, 1, 4).left.map(_.problems.head))(
          isLeft(containsString("maxQueueing (4) must be at least maxBatchSize (5)"))
        )
      }
    )
  ) @@ Effort.timeout(30.seconds)
