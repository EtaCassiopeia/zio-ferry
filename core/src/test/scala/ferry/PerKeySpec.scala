package ferry

import zio.*
import zio.test.*
import zio.test.Assertion.*

object PerKeySpec extends ZIOSpecDefault:

  private def config(
    maxBatchSize: Int = 3,
    maxInFlightBatches: Int = 4,
    maxQueueing: Int = 16,
    maxBatchDelay: Duration = Duration.Zero,
    maxBatchWeight: Long = Long.MaxValue,
    maxQueuedWeight: Long = Long.MaxValue,
    directIdleKeys: Boolean = true
  ): UIO[PerKeyConfig] =
    ZIO
      .fromEither(
        PerKeyConfig.make(
          maxBatchSize,
          maxInFlightBatches,
          maxQueueing,
          maxBatchDelay,
          maxBatchWeight,
          maxQueuedWeight,
          directIdleKeys
        )
      )
      .orDie

  /** Inputs are `(key, n)` pairs. Records every batch call; batches of a key in `held` wait for `gate`. */
  final private case class Downstream(batches: Ref[Chunk[(String, Chunk[Int])]], gate: Promise[Nothing, Unit]):
    def call(held: Set[String])(key: String, inputs: NonEmptyChunk[(String, Int)]): UIO[Unit] =
      batches.update(_ :+ (key -> inputs.map(_._2).toChunk)) *> gate.await.when(held(key)).unit

  private val downstream =
    Ref.make(Chunk.empty[(String, Chunk[Int])]).zipWith(Promise.make[Nothing, Unit])(Downstream(_, _))

  private def ferry(cfg: PerKeyConfig, down: Downstream, held: Set[String] = Set.empty) =
    Ferry.perKey(cfg, (_: (String, Int))._1)(down.call(held))((input, _) => input._2)

  /** Waits until `fiber` is parked awaiting its result, or already done: either way, its input was admitted. */
  private def suspended(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
    // A fiber that has not started yet also reports Suspended, with no trace: it must have run and then parked.
    fiber.status.repeatUntil {
      case Fiber.Status.Suspended(_, trace, _) => trace != Trace.empty
      case Fiber.Status.Done                   => true
      case _                                   => false
    }.unit

  private def metricsReach(ferry: Ferry[?, ?, ?])(p: Ferry.Metrics => Boolean): UIO[Unit] =
    ferry.metrics.repeatUntil(p).unit

  /** Submits each input from its own fiber, one after the other, so that submission order is well defined. */
  private def submitInOrder[A](d: Ferry[(String, Int), Nothing, A], inputs: (String, Int)*) =
    ZIO.foreach(inputs)(input => d(input).fork.tap(suspended))

  def spec = suite("Ferry.perKey")(
    suite("batching")(
      test("batches each key separately, in submission order") {
        for
          down    <- downstream
          cfg     <- config(maxBatchSize = 3, maxBatchDelay = 1.second)
          d       <- ferry(cfg, down)
          callers <- submitInOrder(d, "a" -> 1, "b" -> 2, "a" -> 3, "b" -> 4, "a" -> 5)
          batches <- down.batches.get.repeatUntil(_.nonEmpty) // the "a" batch is full; "b" waits for its delay
          _       <- TestClock.adjust(1.second)
          results <- ZIO.foreach(callers)(_.join)
          after   <- down.batches.get
        yield assertTrue(
          batches == Chunk("a" -> Chunk(1, 3, 5)),
          after == Chunk("a" -> Chunk(1, 3, 5), "b" -> Chunk(2, 4)),
          results == List(1, 2, 3, 4, 5)
        )
      },
      test("never has two batches of one key in flight, and sends what arrived meanwhile as the next batch") {
        for
          down  <- downstream
          cfg   <- config(maxBatchSize = 10, maxInFlightBatches = 4)
          d     <- ferry(cfg, down, held = Set("k"))
          first <- d("k" -> 1).fork
          _     <- metricsReach(d)(_.inFlightBatches == 1)
          rest  <- submitInOrder(d, "k" -> 2, "k" -> 3, "k" -> 4)
          held  <- d.metrics // three slots are free, yet none is used for "k"
          _     <- down.gate.succeed(())
          _     <- ZIO.foreach(first +: rest)(_.join)
          sent  <- down.batches.get
        yield assertTrue(
          held == Ferry.Metrics(queued = 3, inFlightBatches = 1),
          sent == Chunk("k" -> Chunk(1), "k" -> Chunk(2, 3, 4))
        )
      },
      test("runs batches of different keys concurrently, up to maxInFlightBatches") {
        for
          down    <- downstream
          cfg     <- config(maxBatchSize = 1, maxInFlightBatches = 2, maxBatchDelay = 1.milli) // no direct calls
          d       <- ferry(cfg, down, held = Set("a", "b", "c"))
          callers <- ZIO.foreach(List("a", "b", "c"))(k => d(k -> 0).fork)
          _       <- metricsReach(d)(m => m.inFlightBatches == 2 && m.queued == 1)
          _       <- down.gate.succeed(())
          _       <- ZIO.foreach(callers)(_.join)
          sent    <- down.batches.get
        yield assertTrue(sent.map(_._1).sorted == Chunk("a", "b", "c"))
      },
      test("holds a partial batch until maxBatchDelay after its oldest input") {
        for
          down  <- downstream
          cfg   <- config(maxBatchSize = 5, maxBatchDelay = 5.seconds)
          d     <- ferry(cfg, down)
          first <- d("k" -> 1).fork
          _     <- TestClock.adjust(4.seconds)
          late  <- d("k" -> 2).fork
          _     <- suspended(late)
          early <- down.batches.get
          _     <- TestClock.adjust(1.second)
          _     <- first.join.zip(late.join)
          sent  <- down.batches.get
        yield assertTrue(early.isEmpty, sent == Chunk("k" -> Chunk(1, 2)))
      },
      test("dispatches a batch once it reaches maxBatchWeight, and sends a heavier input on its own") {
        for
          down    <- downstream
          cfg     <- config(maxBatchSize = 10, maxBatchDelay = 1.hour, maxBatchWeight = 10)
          d       <- Ferry.perKey(cfg, (_: (String, Int))._1, _._2.toLong)(down.call(Set.empty))((i, _) => i._2)
          callers <- submitInOrder(d, "k" -> 4, "k" -> 6) // exactly maxBatchWeight: full, so no waiting for the delay
          first   <- down.batches.get.repeatUntil(_.nonEmpty)
          heavier <- submitInOrder(d, "k" -> 12)
          _       <- ZIO.foreach(callers ++ heavier)(_.join)
          sent    <- down.batches.get
        yield assertTrue(first == Chunk("k" -> Chunk(4, 6)), sent == Chunk("k" -> Chunk(4, 6), "k" -> Chunk(12)))
      },
      test("sends an idle key's input at once from its caller, even while busy keys fill every batch slot") {
        for
          gates <- ZIO.foreach(List(1, 2))(n => Promise.make[Nothing, Unit].map(n -> _)).map(_.toMap)
          calls <- Ref.make(Chunk.empty[Chunk[Int]])
          cfg   <- config(maxBatchSize = 10, maxInFlightBatches = 1)
          d     <- Ferry.perKey(cfg, (_: (String, Int))._1) { (_, inputs: NonEmptyChunk[(String, Int)]) =>
            val ns = inputs.map(_._2).toChunk
            calls.update(_ :+ ns) *> ZIO.foreachDiscard(gates.collect { case (n, g) if ns.contains(n) => g })(_.await)
          }((i, _) => i._2)
          first   <- d("hot" -> 1).fork                 // idle: sent directly, and held
          _       <- calls.get.repeatUntil(_.size == 1)
          batched <- submitInOrder(d, "hot" -> 2, "hot" -> 3)
          _       <- gates(1).succeed(())
          _       <- calls.get.repeatUntil(_.size == 2) // the only batch slot now carries (2, 3), held
          waiting <- submitInOrder(d, "hot" -> 4)
          cold    <- d("cold" -> 5)                     // neither waits for a slot nor queues behind "hot"
          _       <- gates(2).succeed(())
          _       <- ZIO.foreach(first +: (batched ++ waiting))(_.join)
          sent    <- calls.get
        yield assertTrue(cold == 5, sent == Chunk(Chunk(1), Chunk(2, 3), Chunk(5), Chunk(4)))
      },
      test("leaves an idle key's input to a worker when direct calls are off") {
        for
          down  <- downstream
          cfg   <- config(maxInFlightBatches = 1, directIdleKeys = false)
          d     <- ferry(cfg, down, held = Set("a"))
          a     <- d("a" -> 1).fork
          _     <- metricsReach(d)(_.inFlightBatches == 1)
          b     <- d("b" -> 2).fork
          _     <- metricsReach(d)(_.queued == 1) // the only worker is busy with "a", so "b" waits for it
          waits <- d.metrics
          _     <- down.gate.succeed(())
          _     <- a.join *> b.join
        yield assertTrue(waits == Ferry.Metrics(queued = 1, inFlightBatches = 1))
      },
      test("sends each key's waiting inputs in `order`, ties in submission order") {
        for
          down <- downstream
          cfg  <- config(maxBatchSize = 10, maxBatchDelay = 1.second)
          byRest = Order.by((input: (String, Int)) => (input._2 % 3).toLong)
          d       <- Ferry.perKey(cfg, (_: (String, Int))._1, order = byRest)(down.call(Set.empty))((i, _) => i._2)
          callers <- submitInOrder(d, "k" -> 1, "k" -> 2, "k" -> 3, "k" -> 4, "k" -> 5)
          _       <- TestClock.adjust(1.second)
          _       <- ZIO.foreach(callers)(_.join)
          sent    <- down.batches.get
        yield assertTrue(sent == Chunk("k" -> Chunk(3, 1, 4, 2, 5)))
      },
      test("interrupts a direct call when its caller is interrupted, and frees the key") {
        for
          calls <- Ref.make(Chunk.empty[Int])
          cfg   <- config()
          d     <- Ferry.perKey(cfg, (_: Int) => "k") { (_, inputs: NonEmptyChunk[Int]) =>
            calls.update(_ ++ inputs) *> ZIO.never.when(inputs.head == 1)
          }((n, _) => n)
          stuck <- d(1).fork
          _     <- calls.get.repeatUntil(_.nonEmpty)
          _     <- stuck.interrupt
          later <- d(2)
          sent  <- calls.get
        yield assertTrue(later == 2, sent == Chunk(1, 2))
      },
      test("lets each input succeed or fail on its own when the batch call reports outcomes per input") {
        for
          cfg <- config(maxBatchSize = 3)
          d   <- Ferry.perKey(cfg, (_: Int) => "k") { (_, inputs: NonEmptyChunk[Int]) =>
            ZIO.succeed(inputs.map(n => n -> Either.cond(n > 0, n * 10, s"$n is not positive")).toMap)
          }((n, outcomes) => outcomes(n))
          results <- ZIO.foreachPar(List(1, -2, 3))(d(_))
        yield assertTrue(results == List(Right(10), Left("-2 is not positive"), Right(30)))
      }
    ),
    suite("load limiting")(
      test("rejects an input that would exceed maxQueueing or maxQueuedWeight") {
        for
          down <- downstream
          cfg  <- config(
            maxBatchSize = 1,
            maxInFlightBatches = 1,
            maxQueueing = 2,
            maxBatchWeight = 5,
            maxQueuedWeight = 5
          )
          d        <- Ferry.perKey(cfg, (_: (String, Int))._1, _._2.toLong)(down.call(Set("k")))((i, _) => i._2)
          running  <- d("k" -> 1).fork
          _        <- metricsReach(d)(_.inFlightBatches == 1)
          queued   <- submitInOrder(d, "k" -> 3)
          tooHeavy <- d("k" -> 3).exit // 3 + 3 > 5
          lighter  <- submitInOrder(d, "k" -> 2)
          tooMany  <- d("k" -> 0).exit // two are already waiting
          _        <- down.gate.succeed(())
          _        <- running.join *> ZIO.foreach(queued ++ lighter)(_.join)
        yield assert(tooHeavy)(fails(equalTo(FerryError.Rejected))) &&
          assert(tooMany)(fails(equalTo(FerryError.Rejected)))
      }
    ),
    suite("failures")(
      test("fails only the callers of the failed batch, and keeps serving the key afterwards") {
        for
          cfg <- config(maxBatchSize = 2)
          d   <- Ferry.perKey(cfg, (_: (String, Int))._1) { (_, inputs: NonEmptyChunk[(String, Int)]) =>
            ZIO.fail("unavailable").when(inputs.exists(_._2 <= 2))
          }((i, _) => i._2)
          failed <- ZIO.foreachPar(List("a" -> 1, "a" -> 2))(d(_).exit)
          others <- ZIO.foreachPar(List("b" -> 3, "a" -> 4))(d(_))
        yield assert(failed)(forall(fails(equalTo(FerryError.Failed("unavailable"))))) &&
          assertTrue(others == List(3, 4))
      },
      test("releases a key whose batch call died, so its later inputs are still served") {
        val boom = new RuntimeException("boom")
        for
          calls <- Ref.make(0)
          cfg   <- config(maxBatchSize = 1)
          d     <- Ferry.perKey(cfg, (_: Int) => "k") { (_, _: NonEmptyChunk[Int]) =>
            calls.getAndUpdate(_ + 1).flatMap(n => ZIO.die(boom).when(n == 0))
          }((n, _) => n)
          died  <- d(1).exit
          later <- d(2)
        yield assertTrue(died.causeOption.flatMap(_.dieOption).contains(boom), later == 2)
      }
    ),
    suite("interruption and shutdown")(
      test("leaves out the input of a caller interrupted before dispatch, which frees its place at once") {
        for
          down    <- downstream
          cfg     <- config(maxBatchSize = 2, maxBatchDelay = 1.second)
          d       <- ferry(cfg, down)
          gone    <- d("k" -> 1).fork
          _       <- suspended(gone)
          _       <- gone.interrupt
          second  <- d("k" -> 2).fork // 1 has left its lane: 2 starts a partial batch of its own
          _       <- suspended(second)
          _       <- TestClock.adjust(1.second)
          result  <- second.join
          batches <- down.batches.get
        yield assertTrue(result == 2, batches == Chunk("k" -> Chunk(2)))
      },
      test("interrupts every waiting and in-flight caller when the scope closes, and every later caller") {
        for
          down    <- downstream
          cfg     <- config(maxBatchSize = 1)
          scope   <- Scope.make
          d       <- scope.extend(ferry(cfg, down, held = Set("k")))
          running <- d("k" -> 1).fork
          _       <- metricsReach(d)(_.inFlightBatches == 1)
          queued  <- submitInOrder(d, "k" -> 2)
          other   <- d("j" -> 3) // other keys are still served meanwhile
          _       <- scope.close(Exit.unit)
          exits   <- ZIO.foreach(running +: queued)(_.await)
          after   <- d("k" -> 4).exit
        yield assertTrue(other == 3, exits.forall(_.isInterrupted), after.isInterrupted)
      }
    ),
    suite("PerKeyConfig")(
      test("defaults to sending as soon as a key is free, with no weight limits") {
        for cfg <- config()
        yield assertTrue(
          cfg.maxBatchDelay == Duration.Zero,
          cfg.maxBatchWeight == Long.MaxValue,
          cfg.maxQueuedWeight == Long.MaxValue,
          cfg.directIdleKeys
        )
      },
      test("accepts a configuration exactly when every constraint holds") {
        check(Gen.int(0, 3), Gen.int(0, 2), Gen.int(0, 4), Gen.long(-1, 1), Gen.long(0, 3), Gen.long(0, 4)) {
          (size, inFlight, queueing, delayMs, batchWeight, queuedWeight) =>
            val valid = size >= 1 && inFlight >= 1 && queueing >= size && delayMs >= 0 && batchWeight >= 1 &&
              queuedWeight >= batchWeight
            val made = PerKeyConfig.make(size, inFlight, queueing, delayMs.millis, batchWeight, queuedWeight)
            assertTrue(made.isRight == valid)
        }
      },
      test("reports every problem at once, naming each setting") {
        val problems = PerKeyConfig.make(0, 0, -1, (-1).second, 0, -1).left.toOption.toList.flatMap(_.problems)
        assertTrue(
          problems.size == 6,
          problems.exists(_.contains("maxBatchWeight must be at least 1, got 0")),
          problems.exists(_.contains("maxQueuedWeight (-1) must be at least maxBatchWeight (0)"))
        )
      }
    )
  ) @@ Effort.timeout(30.seconds)
