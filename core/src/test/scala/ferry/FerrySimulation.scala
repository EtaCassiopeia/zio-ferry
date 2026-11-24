package ferry

import scala.util.Random

import zio.*
import zio.stm.*

import ferry.Protocol.Action
import ferry.testkit.sim.{Decisions, Simulator}

/**
 * Ferry under the deterministic simulator.
 *
 * A [[FerrySimulation.Scenario]] is the input of a run: the configuration, the callers (when each arrives, its key,
 * weight and rank, and whether it gives up), and whether the scope closes early. It is generated from a seed before the
 * run. Everything that happens during the run is a decision of the simulator: the interleaving, which timer fires
 * first, and each call's latency and outcome. A run is therefore a function of (scenario, decisions), and replays
 * exactly from them.
 *
 * Each run is checked against Ferry's documented contract by [[FerrySimulation.violations]], independently of the
 * model, and its recorded trace is what trace validation checks against the model. The two divide the work:
 *
 *   - the contract checks what the model cannot see: virtual time (no partial batch leaves before its deadline), the
 *     user's call and what each caller is answered from it, and how many calls run at once;
 *   - the model checks what depends on the order of protocol steps, which only the trace records: per-key order, that a
 *     caller who gave up is never sent, and that nothing is taken after close.
 */
object FerrySimulation:

  enum Family:
    case Make, PerKeyDirect, PerKeyBatched, Weighted

  final case class Caller(gapMs: Int, key: String, weight: Long, rank: Long, giveUpAfterMs: Option[Int])

  final case class Scenario(
    family: Family,
    limits: Protocol.Limits,
    workers: Int,
    callers: Chunk[Caller],
    closeAfterMs: Option[Int]
  )

  /** How a call ends, as the simulator decided. */
  enum Ends:
    case Succeeds, Fails, Dies

  /** A call the ferry made: `direct` is the caller who made it itself, when no worker did. */
  final case class Call(serial: Int, key: String, inputs: Chunk[Int], startedAt: Long, ends: Ends, direct: Option[Int])

  /** An action the run took, with the simulator's step and virtual time (nanoseconds) when it took it. */
  final case class Event(step: Long, at: Long, action: Action[String])

  /** A recorder that also notes where in the simulation each action was taken, for timelines. */
  final class TimedRecorder(log: TRef[Chunk[Event]]) extends Recorder[String]:
    def record(actions: Chunk[Action[String]]): USTM[Unit] =
      log.update { events =>
        val (step, at) = Simulator.position().getOrElse((0L, 0L))
        events ++ actions.map(Event(step, at, _))
      }
    def timeline: UIO[Chunk[Event]] = log.get.commit

  /**
   * What one run did: every caller's answer, every call made, and the recorded protocol trace, with times; and, for the
   * contract, when each caller arrived, who gave up, who arrived after the scope had closed, and the most worker calls
   * that ran at once.
   */
  final case class Observation(
    answers: Map[Int, Exit[FerryError[String], Int]],
    calls: Chunk[Call],
    overlapping: Set[String],
    peak: Int,
    arrivedAt: Map[Int, Long],
    gaveUp: Set[Int],
    late: Set[Int],
    timeline: Chunk[Event]
  ):
    def trace: Chunk[Action[String]] = timeline.map(_.action)

    def recorded(s: Scenario): Conformance.Recorded[String] = Conformance.Recorded(s.limits, s.workers, trace, identity)

    /**
     * What the run did, comparable across runs: an `Exit` carries fiber ids and exception instances that differ in
     * every run, so answers are reduced to what they say.
     */
    def summary: (Chunk[Action[String]], Chunk[Call], Map[Int, String]) =
      (
        trace,
        calls,
        answers.map((i, exit) =>
          i -> exit.foldExit(
            c => if c.isInterruptedOnly then "interrupted" else c.failureOption.fold("died")(_.toString),
            n => s"answered $n"
          )
        )
      )

  /**
   * A scenario of `family`, from `seed`: small enough for TLC, busy enough that arrivals, deadlines and calls collide.
   */
  def scenario(family: Family, seed: Long, maxCallers: Int = 6): Scenario =
    val r                  = Random(seed)
    def pick[A](xs: A*): A = xs(r.nextInt(xs.size))
    val keys               = if family == Family.Make then Vector("k") else Vector("a", "b", "c").take(1 + r.nextInt(3))
    val delayMs            = family match
      case Family.PerKeyDirect => 0
      case _                   => pick(0, 2, 5)
    val maxBatch = 1 + r.nextInt(3)
    val limits   = family match
      case Family.Make =>
        Ferry.makeLimits(
          FerryConfig.make(maxBatch, delayMs.millis, 1 + r.nextInt(3), maxBatch + r.nextInt(4)).toOption.get
        )
      case Family.PerKeyDirect =>
        Ferry.perKeyLimits(PerKeyConfig.make(maxBatch, 1 + r.nextInt(2), maxBatch + r.nextInt(4)).toOption.get)
      case Family.PerKeyBatched =>
        Ferry.perKeyLimits(
          PerKeyConfig
            .make(maxBatch, 1 + r.nextInt(2), maxBatch + r.nextInt(4), delayMs.millis, directIdleKeys = false)
            .toOption
            .get
        )
      case Family.Weighted =>
        val maxWeight = 2L + r.nextInt(3)
        Ferry.perKeyLimits(
          PerKeyConfig
            .make(
              maxBatch + 1,
              1 + r.nextInt(2),
              maxBatch + 1 + r.nextInt(4),
              delayMs.millis,
              maxWeight,
              maxWeight + r.nextInt(4)
            )
            .toOption
            .get
        )
    val callers = Chunk.fill(1 + r.nextInt(maxCallers)) {
      Caller(
        gapMs = pick(0, 0, 1, 3),
        key = keys(r.nextInt(keys.size)),
        weight = if family == Family.Weighted then 1L + r.nextInt(3) else 1L,
        rank = if family == Family.Weighted then r.nextInt(3).toLong else 0L,
        giveUpAfterMs = Option.when(r.nextInt(5) == 0)(pick(0, 1, 4))
      )
    }
    val workers = 1 + r.nextInt(if family == Family.Make then 3 else 2)
    Scenario(family, limits, workers, callers, Option.when(r.nextInt(4) == 0)(pick(0, 2, 6)))

  /**
   * The run itself. Calls take a latency and an outcome chosen by the simulator, and answer each caller with the call's
   * serial number, so that an answer shows which call it came from.
   */
  def program(s: Scenario, rules: Engine.Rules = Engine.Rules.standard): UIO[Observation] =
    for
      recorder <- TRef.make(Chunk.empty[Event]).commit.map(TimedRecorder(_))
      calls    <- Ref.make(Chunk.empty[Call])
      serials  <- Ref.make(0)
      inFlight <- Ref.make(Map.empty[String, Int])
      overlap  <- Ref.make(Set.empty[String])
      running  <- Ref.make(0)
      peak     <- Ref.make(0)
      arrived  <- Ref.make(Map.empty[Int, Long])
      gaveUp   <- Ref.make(Set.empty[Int])
      closed   <- Ref.make(false)
      late     <- Ref.make(Set.empty[Int])
      // Set by each caller: a direct call is forked from its caller's fiber, a worker's from the ferry's creator.
      direct <- ZIO.succeed(Unsafe.unsafe(u ?=> FiberRef.unsafe.make(Option.empty[Int])(using u)))
      scope  <- Scope.make
      call = (key: String, inputs: NonEmptyChunk[(String, Int)]) =>
        // Acquired and released as one: a call is interrupted whenever its caller gives up or the scope closes.
        val enter =
          for
            serial  <- serials.getAndUpdate(_ + 1)
            at      <- Clock.nanoTime
            by      <- direct.get
            before  <- inFlight.modify(m => (m.getOrElse(key, 0), m.updated(key, m.getOrElse(key, 0) + 1)))
            _       <- overlap.update(_ + key).when(before > 0 && s.limits.oneCallPerKey)
            _       <- running.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n)).when(by.isEmpty)
            latency <- Simulator.pick("latency", NonEmptyChunk(0, 1, 2, 5))
            outcome <- Simulator.choose("outcome", 4)
            ends = outcome match
              case 3 => Ends.Dies
              case 2 => Ends.Fails
              case _ => Ends.Succeeds
            _ <- calls.update(_ :+ Call(serial, key, inputs.map(_._2).toChunk, at, ends, by))
          yield (serial, latency, ends, by)
        ZIO.acquireReleaseWith(enter) { (_, _, _, by) =>
          inFlight.update(m => m.updated(key, m(key) - 1)) *> running.update(_ - 1).when(by.isEmpty)
        } { (serial, latency, ends, _) =>
          ZIO.sleep(latency.millis) *> (ends match
            case Ends.Dies     => ZIO.die(RuntimeException("boom"))
            case Ends.Fails    => ZIO.fail("unavailable")
            case Ends.Succeeds => ZIO.succeed(serial))
        }
      ferry <- scope.extend[Any](
        Engine.make(
          s.limits,
          s.workers,
          (_: (String, Int))._1,
          (i: (String, Int)) => s.callers(i._2).weight,
          (i: (String, Int)) => s.callers(i._2).rank,
          recorder,
          rules
        )(call)((_, serial: Int) => serial)
      )
      _ <- ZIO.foreachDiscard(s.closeAfterMs)(ms =>
        (ZIO.sleep(ms.millis) *> scope.close(Exit.unit) *> closed.set(true)).fork
      )
      fibers <- ZIO.foreach(s.callers.zipWithIndex) { (c, i) =>
        val arrive = Clock.nanoTime.flatMap(t => arrived.update(_ + (i -> t)))
        for
          _     <- ZIO.sleep(c.gapMs.millis)
          _     <- closed.get.flatMap(late.update(_ + i).when(_))
          fiber <- (arrive *> direct.locally(Some(i))(ferry(c.key -> i))).fork
          _     <- ZIO.foreachDiscard(c.giveUpAfterMs)(ms =>
            (ZIO.sleep(ms.millis) *> gaveUp.update(_ + i) *> fiber.interrupt).fork
          )
        yield i -> fiber
      }
      answers <- ZIO.foreach(fibers)((i, f) => f.await.map(i -> _)) // await: an interrupted caller is an answer too
      _       <- scope.close(Exit.unit)
      sent    <- calls.get
      both    <- overlap.get
      most    <- peak.get
      at      <- arrived.get
      gave    <- gaveUp.get
      after   <- late.get
      events  <- recorder.timeline
    yield Observation(answers.toMap, sent, both, most, at, gave, after, events)

  final case class Result(scenario: Scenario, run: Simulator.Run[Nothing, Observation]):
    def observation: Option[Observation] = run.exit.flatMap(_.toEither.toOption)

  def run(s: Scenario, decisions: Decisions, rules: Engine.Rules = Engine.Rules.standard): Result =
    Result(s, Simulator.run(program(s, rules), decisions, maxSteps = 200_000))

  def isRejected(exit: Exit[FerryError[?], ?]): Boolean =
    exit.causeOption.flatMap(_.failureOption).contains(FerryError.Rejected)

  /** Every way a run breaks Ferry's documented contract. Empty for a correct run. */
  def violations(result: Result): Chunk[String] =
    val s = result.scenario
    result.observation match
      case None if result.run.hung => Chunk("hung: some caller was never answered")
      case None                    => Chunk(s"the program failed: ${result.run.exit}")
      case Some(o)                 =>
        val callOf    = o.calls.flatMap(c => c.inputs.map(_ -> c)).toMap
        val sent      = o.calls.flatMap(_.inputs)
        val sentTwice = sent.groupBy(identity).collect { case (i, xs) if xs.size > 1 => i }
        val delay     = s.limits.maxBatchDelayNanos
        val closes    = s.closeAfterMs.isDefined
        // A partial batch may leave early only when a caller gave up (a full lane can shrink) or weights filled it.
        val deadlinesExact = s.family != Family.Weighted && s.callers.forall(_.giveUpAfterMs.isEmpty)

        def answer(i: Int, exit: Exit[FerryError[String], Int]): Option[String] =
          val call = callOf.get(i)
          exit match
            case Exit.Success(serial) =>
              Option.unless(call.exists(c => c.serial == serial && c.ends == Ends.Succeeds))(
                s"caller $i was answered from call $serial, but its input went in $call"
              )
            case Exit.Failure(c) if c.isInterruptedOnly =>
              Option.unless(o.gaveUp(i) || closes)(s"caller $i was interrupted, but nobody interrupted it")
            case Exit.Failure(c) =>
              (c.failureOption, c.dieOption) match
                case (Some(FerryError.Rejected), _) =>
                  call.map(c => s"caller $i was rejected, but its input was sent in call ${c.serial}")
                case (Some(FerryError.Failed(_)), _) =>
                  Option.unless(call.exists(_.ends == Ends.Fails))(s"caller $i failed, but its input went in $call")
                case (_, Some(_)) =>
                  Option.unless(call.exists(_.ends == Ends.Dies))(s"caller $i died, but its input went in $call")
                case _ => Some(s"caller $i ended with an unexpected cause $c")

        Chunk.fromIterable(
          Option.when(result.run.escapes.nonEmpty)(s"escaped the simulator: ${result.run.escapes}") ++
            sentTwice.map(i => s"input $i was sent twice") ++
            o.overlapping.map(k => s"key $k had two calls in flight") ++
            Option.when(o.peak > s.workers)(s"${o.peak} worker calls ran at once, with ${s.workers} workers") ++
            Option.when(
              s.callers.size <= s.limits.maxQueueing && s.callers.map(_.weight).sum <= s.limits.maxQueuedWeight &&
                o.answers.values.exists(isRejected)
            )("rejected a call although everything fits in the queue") ++
            o.calls.flatMap { c =>
              val weight = c.inputs.map(s.callers(_).weight).sum
              List(
                Option.when(c.inputs.size > s.limits.maxBatchSize)(s"call ${c.serial} exceeds maxBatchSize"),
                Option.when(c.inputs.size > 1 && weight > s.limits.maxBatchWeight)(
                  s"call ${c.serial} weighs $weight, over maxBatchWeight"
                ),
                Option.when(c.inputs.map(s.callers(_).key).distinct.size > 1)(s"call ${c.serial} mixes keys"),
                Option.when(c.direct.isDefined && !s.limits.direct)(s"call ${c.serial} was direct, which is off"),
                Option.when(c.direct.exists(i => c.inputs != Chunk(i)))(
                  s"direct call ${c.serial} carries ${c.inputs}, not only its caller's input"
                ),
                Option.when(
                  deadlinesExact && c.direct.isEmpty && c.inputs.size < s.limits.maxBatchSize &&
                    c.startedAt < c.inputs.map(o.arrivedAt).min + delay
                )(s"partial call ${c.serial} left at ${c.startedAt}ns, before its deadline")
              ).flatten
            } ++
            o.answers.toList.flatMap(answer) ++
            o.late.toList.flatMap(i =>
              o.answers.get(i).filterNot(_.isInterrupted).map(e => s"caller $i arrived after close but got $e")
            )
        )
