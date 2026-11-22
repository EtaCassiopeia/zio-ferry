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
 * model, and its recorded trace is what trace validation checks against the model. (Per-key order is left to the model:
 * it depends on the order of admission, which only the trace records.)
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

  /** What one run did: every caller's answer, every call made, and the recorded protocol trace, with times. */
  final case class Observation(
    answers: Map[Int, Exit[FerryError[String], Int]],
    calls: Chunk[(String, Chunk[Int])],
    overlapping: Set[String],
    timeline: Chunk[Event]
  ):
    def trace: Chunk[Action[String]] = timeline.map(_.action)

    def recorded(s: Scenario): Conformance.Recorded[String] = Conformance.Recorded(s.limits, s.workers, trace, identity)

    /**
     * What the run did, comparable across runs: an `Exit` carries fiber ids and exception instances that differ in
     * every run, so answers are reduced to what they say.
     */
    def summary: (Chunk[Action[String]], Chunk[(String, Chunk[Int])], Map[Int, String]) =
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
  def scenario(family: Family, seed: Long): Scenario =
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
    val callers = Chunk.fill(1 + r.nextInt(6)) {
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

  /** The run itself. Calls take a latency and an outcome chosen by the simulator. */
  def program(s: Scenario, rules: Engine.Rules = Engine.Rules.standard): UIO[Observation] =
    for
      recorder <- TRef.make(Chunk.empty[Event]).commit.map(TimedRecorder(_))
      calls    <- Ref.make(Chunk.empty[(String, Chunk[Int])])
      inFlight <- Ref.make(Map.empty[String, Int])
      overlap  <- Ref.make(Set.empty[String])
      scope    <- Scope.make
      call = (key: String, inputs: NonEmptyChunk[(String, Int)]) =>
        for
          _       <- calls.update(_ :+ (key -> inputs.map(_._2).toChunk))
          before  <- inFlight.modify(m => (m.getOrElse(key, 0), m.updated(key, m.getOrElse(key, 0) + 1)))
          _       <- overlap.update(_ + key).when(before > 0 && s.limits.oneCallPerKey)
          latency <- Simulator.pick("latency", NonEmptyChunk(0, 1, 2, 5))
          outcome <- Simulator.choose("outcome", 4)
          result  <- (ZIO.sleep(latency.millis) *> (outcome match
            case 3 => ZIO.die(RuntimeException("boom"))
            case 2 => ZIO.fail("unavailable")
            case _ => ZIO.succeed(inputs.size))).ensuring(inFlight.update(m => m.updated(key, m(key) - 1)))
        yield result
      ferry <- scope.extend[Any](
        Engine.make(
          s.limits,
          s.workers,
          (_: (String, Int))._1,
          (i: (String, Int)) => s.callers(i._2).weight,
          (i: (String, Int)) => s.callers(i._2).rank,
          recorder,
          rules
        )(call)((input, _) => input._2)
      )
      _      <- ZIO.foreachDiscard(s.closeAfterMs)(ms => (ZIO.sleep(ms.millis) *> scope.close(Exit.unit)).fork)
      fibers <- ZIO.foreach(s.callers.zipWithIndex) { (c, i) =>
        for
          _     <- ZIO.sleep(c.gapMs.millis)
          fiber <- ferry(c.key -> i).fork
          _     <- ZIO.foreachDiscard(c.giveUpAfterMs)(ms => (ZIO.sleep(ms.millis) *> fiber.interrupt).fork)
        yield i -> fiber
      }
      answers <- ZIO.foreach(fibers)((i, f) => f.await.map(i -> _)) // await: an interrupted caller is an answer too
      _       <- scope.close(Exit.unit)
      sent    <- calls.get
      both    <- overlap.get
      events  <- recorder.timeline
    yield Observation(answers.toMap, sent, both, events)

  final case class Result(scenario: Scenario, run: Simulator.Run[Nothing, Observation]):
    def observation: Option[Observation] = run.exit.flatMap(_.toEither.toOption)

  def run(s: Scenario, decisions: Decisions, rules: Engine.Rules = Engine.Rules.standard): Result =
    Result(s, Simulator.run(program(s, rules), decisions, maxSteps = 200_000))

  /** Every way a run breaks Ferry's documented contract. Empty for a correct run. */
  def violations(result: Result): Chunk[String] =
    val s = result.scenario
    result.observation match
      case None if result.run.hung => Chunk("hung: some caller was never answered")
      case None                    => Chunk(s"the program failed: ${result.run.exit}")
      case Some(o)                 =>
        val sent      = o.calls.flatMap(_._2)
        val sentTwice = sent.groupBy(identity).collect { case (i, xs) if xs.size > 1 => i }
        val rejected  = o.answers.collect {
          case (i, Exit.Failure(c)) if c.failureOption.contains(FerryError.Rejected) => i
        }
        val succeeded = o.answers.collect { case (i, Exit.Success(_)) => i }
        Chunk.fromIterable(
          Option.when(result.run.escapes.nonEmpty)(s"escaped the simulator: ${result.run.escapes}") ++
            sentTwice.map(i => s"input $i was sent twice") ++
            rejected.filter(sent.contains).map(i => s"input $i was rejected and sent") ++
            succeeded.filterNot(sent.contains).map(i => s"input $i succeeded without being sent") ++
            o.calls.filter(_._2.size > s.limits.maxBatchSize).map(c => s"a batch exceeded maxBatchSize: $c") ++
            o.calls
              .filter(c => c._2.map(i => s.callers(i).key).distinct.size > 1)
              .map(c => s"a batch mixed keys: $c") ++
            o.overlapping.map(k => s"key $k had two calls in flight")
        )
