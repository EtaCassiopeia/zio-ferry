package ferry

import zio.*
import zio.test.*

import ferry.FerrySimulation.Family
import ferry.testkit.sim.Decisions
import ferry.testkit.tla.{Alter, Tlc}

/**
 * Planted drift must be caught, in both directions. Each fixture makes the code and the model disagree in one small,
 * plausible way, the kind of change a refactoring makes by accident, and requires the conformance checks to notice:
 *
 *   - '''code drift''': the engine runs on deliberately wrong [[Engine.Rules]]; simulated runs must be rejected by TLC
 *     (code → model), and the model's behaviours must diverge when replayed through the wrong rules (model → code);
 *   - '''model drift''': a copy of `Ferry.tla` is altered; the code's runs must be rejected against it, or its
 *     behaviours must diverge when replayed through the code.
 *
 * The contract checks of the simulation are run too, and reported: most of these drifts break no documented promise by
 * themselves, which is exactly why a model the code is checked against is worth having.
 */
object DriftSpec extends ZIOSpecDefault:

  private val seeds = 1L to 40L

  /** Serves the last ready lane instead of the first: fairness between keys is gone. */
  object ServeLastReady extends Engine.Rules:
    override def take[K, X](p: Protocol[K, X], worker: Int, now: Long) =
      p.copy(ready = p.ready.reverse)
        .take(worker, now)
        .map(s => s.copy(next = s.next.copy(ready = s.next.ready.reverse)))

  /** Takes batches by count alone, ignoring `maxBatchWeight`. */
  object IgnoreWeightLimit extends Engine.Rules:
    override def take[K, X](p: Protocol[K, X], worker: Int, now: Long) =
      val unlimited = p.copy(limits = p.limits.copy(maxBatchWeight = Long.MaxValue))
      unlimited.take(worker, now).map(s => s.copy(next = s.next.copy(limits = p.limits)))

  /** A caller that gives up stops waiting, but its input stays in the lane and is sent with the batch. */
  object KeepCancelledInLane extends Engine.Rules:
    override def cancel[K, X](p: Protocol[K, X], caller: Long) =
      p.cancel(caller).map(s => s.copy(next = p.copy(pending = p.pending - caller)))

  /** A model in which a worker takes only full lanes: a partial batch whose deadline passed would wait forever. */
  val TakeOnlyFull: Alter =
    Alter.none.replace(
      "Ferry.tla",
      "  /\\ ready # <<>>\n  /\\ LET k    == Head(ready)",
      "  /\\ ready # <<>>\n  /\\ Full(lane[Head(ready)])\n  /\\ LET k    == Head(ready)"
    )

  /** A model in which a caller that gives up keeps its place in the lane. */
  val CancelKeepsPlace: Alter =
    Alter.none.replace(
      "Ferry.tla",
      """  /\ IF \E k \in Keys : c \in Range(lane[k])
     THEN LET k == CHOOSE x \in Keys : c \in Range(lane[x])
              s == Remove(lane[k], c)
          IN  /\ lane' = [lane EXCEPT ![k] = s]
              /\ sched' = [sched EXCEPT ![k] = Place(k, s, busy)[1]]
              /\ ready' = Place(k, s, busy)[2]
     ELSE UNCHANGED <<lane, sched, ready>>""",
      "  /\\ UNCHANGED <<lane, sched, ready>>"
    )

  /** The verdicts of trace validation of the simulated runs of `families`, under `rules` and against `alter`. */
  private def traceVerdicts(families: Seq[Family], rules: Engine.Rules, alter: Alter) =
    ZIO
      .foreachPar(for f <- families; s <- seeds yield (f, s)) { (family, seed) =>
        ZIO.attemptBlocking {
          val result = FerrySimulation.run(FerrySimulation.scenario(family, seed), Decisions.seeded(seed), rules)
          val broken = FerrySimulation.violations(result).nonEmpty
          val trace  = result.observation.map(o =>
            Conformance.validate(s"drift_${family}_$seed", o.recorded(result.scenario), alter)
          )
          (broken, trace)
        }.orDie
      }
      .withParallelism(4)

  private def rejected(verdicts: Seq[(Boolean, Option[Tlc.Verdict])]) =
    verdicts.count(_._2.exists(_.isInstanceOf[Tlc.Verdict.Rejected]))

  private def toolErrors(verdicts: Seq[(Boolean, Option[Tlc.Verdict])]) =
    verdicts.flatMap(_._2).collect { case e: Tlc.Verdict.ToolError => e }

  /**
   * How many of a thousand of the model's behaviours diverge when replayed through `rules`, from a model altered by
   * `alter`. Some drifts show only in rare states (two lanes ready at once), so the sample is large; replay is cheap.
   */
  private def divergences(config: String, rules: Engine.Rules, alter: Alter = Alter.none) =
    ZIO.attemptBlocking {
      val formal = Tlc.formal.get
      ferry.testkit.tla.Behaviours
        .generate(formal, "Ferry", config, 1000, 40, 7L, alter)
        .map(
          _.count(b =>
            ModelReplay
              .replay(b, ModelReplay.instance(formal, config), rules)
              .isInstanceOf[ModelReplay.Verdict.Diverged]
          )
        )
    }.orDie

  def spec = suite("Planted drift is caught")(
    suite("in the code")(
      test("serving the last ready lane first") {
        for
          traces <- traceVerdicts(Seq(Family.PerKeyBatched, Family.Make), ServeLastReady, Alter.none)
          replay <- divergences("FerryBatchOnly", ServeLastReady)
        yield assertTrue(toolErrors(traces).isEmpty, rejected(traces) > 0, replay.exists(_ > 0))
      },
      test("ignoring the weight limit") {
        for traces <- traceVerdicts(Seq(Family.Weighted), IgnoreWeightLimit, Alter.none)
        yield assertTrue(toolErrors(traces).isEmpty, rejected(traces) > 0)
      },
      test("leaving a cancelled input in its lane") {
        for
          traces <- traceVerdicts(Seq(Family.PerKeyBatched, Family.Make), KeepCancelledInLane, Alter.none)
          replay <- divergences("FerryBatchOnly", KeepCancelledInLane)
        yield assertTrue(toolErrors(traces).isEmpty, rejected(traces) > 0, replay.exists(_ > 0))
      }
    ),
    suite("in the model")(
      test("a model that takes only full lanes rejects the code's partial batches") {
        for traces <- traceVerdicts(Seq(Family.PerKeyBatched, Family.Make), Engine.Rules.standard, TakeOnlyFull)
        yield assertTrue(toolErrors(traces).isEmpty, rejected(traces) > 0)
      },
      test("a model in which a cancelled caller keeps its place diverges from the code") {
        for
          traces <- traceVerdicts(Seq(Family.PerKeyBatched), Engine.Rules.standard, CancelKeepsPlace)
          replay <- divergences("FerryBatchOnly", Engine.Rules.standard, CancelKeepsPlace)
        yield assertTrue(toolErrors(traces).isEmpty, rejected(traces) > 0, replay.exists(_ > 0))
      }
    )
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.minutes)
