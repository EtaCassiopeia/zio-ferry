package ferry

import zio.*
import zio.test.*

import ferry.FerrySimulation.Family
import ferry.check.ProtocolSpace
import ferry.check.ProtocolSpace.Config
import ferry.testkit.check.StateSpace
import ferry.testkit.sim.Decisions

/**
 * Planted drift must be caught. Each fixture runs the engine on deliberately wrong [[Engine.Rules]], the kind of change
 * a refactoring makes by accident, and the table records which check notices:
 *
 *   - '''the protocol's properties''': exploring every state of a small instance with the wrong rules
 *     ([[ProtocolSpace.check]]), and replaying the engine's simulated runs through those rules with every property
 *     checked after each step ([[ProtocolSpace.replay]]);
 *   - '''the contract''': the simulation's own checks of what callers see: answers, times and call counts.
 *
 * The two divide the work. The properties see the order of protocol steps but no clock, so a drift in timing is the
 * contract's alone; the contract sees what callers get but not the order of steps, so two of the three drifts the
 * properties catch break no promise a caller could check.
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

  /** Treats every deadline as passed: partial batches leave at once. The protocol has no clock of its own to see it. */
  object IgnoreDeadline extends Engine.Rules:
    override def take[K, X](p: Protocol[K, X], worker: Int, now: Long) = p.take(worker, Long.MaxValue)

  /** A caller that gives up stops waiting, but its input stays in the lane and is sent with the batch. */
  object KeepCancelledInLane extends Engine.Rules:
    override def cancel[K, X](p: Protocol[K, X], caller: Long) =
      p.cancel(caller).map(s => s.copy(next = p.copy(pending = p.pending - caller)))

  private type Run = (Boolean, Option[ProtocolSpace.Replay])

  /**
   * The simulated runs of `families` under `rules`: whether each broke the contract, and its replay through `rules`.
   */
  private def runs(families: Seq[Family], rules: Engine.Rules): UIO[Seq[Run]] =
    ZIO
      .foreachPar(for f <- families; s <- seeds yield (f, s)) { (family, seed) =>
        ZIO.attemptBlocking {
          val result = FerrySimulation.run(FerrySimulation.scenario(family, seed), Decisions.seeded(seed), rules)
          val broken = FerrySimulation.violations(result).nonEmpty
          (broken, result.observation.map(o => ProtocolSpace.replay(result.scenario.limits, o.trace, rules)))
        }.orDie
      }
      .withParallelism(4)

  /** How many runs broke a property of the protocol on the path the engine took. */
  private def violated(runs: Seq[Run]) = runs.count(_._2.exists(_.isInstanceOf[ProtocolSpace.Replay.Violated]))

  /** How many runs broke the contract checks of the simulation. */
  private def broken(runs: Seq[Run]) = runs.count(_._1)

  /** Whether exploring every state of `config` under `rules` breaks `property`. */
  private def explored(config: Config, rules: Engine.Rules, property: String): UIO[Boolean] =
    ZIO.attemptBlocking(ProtocolSpace.check(config, rules)).orDie.map {
      case StateSpace.Verdict.Violated(`property`, _, _) => true
      case _                                             => false
    }

  def spec = suite("Planted drift is caught")(
    test("serving the last ready lane first: by the properties, and by no promise a caller can check") {
      for
        space <- explored(Config.batchOnly, ServeLastReady, "FirstComeFirstServed")
        runs  <- runs(Seq(Family.PerKeyBatched, Family.Make), ServeLastReady)
      yield assertTrue(space, violated(runs) > 0, broken(runs) == 0)
    },
    test("ignoring the weight limit: by the properties and by the contract") {
      for
        space <- explored(Config.weighted, IgnoreWeightLimit, "BatchBounded")
        runs  <- runs(Seq(Family.Weighted), IgnoreWeightLimit)
      yield assertTrue(space, violated(runs) > 0, broken(runs) > 0)
    },
    test("leaving a cancelled input in its lane: by the properties, and by no promise a caller can check") {
      for
        space <- explored(Config.perKey, KeepCancelledInLane, "CancelledNeverSent")
        runs  <- runs(Seq(Family.PerKeyBatched, Family.Make), KeepCancelledInLane)
      yield assertTrue(space, violated(runs) > 0, broken(runs) == 0)
    },
    test("sending partial batches before their deadline: by the contract alone, since the protocol has no clock") {
      for
        space <- ZIO.attemptBlocking(ProtocolSpace.check(Config.perKey, IgnoreDeadline)).orDie
        runs  <- runs(Seq(Family.PerKeyBatched, Family.Make), IgnoreDeadline)
      yield assertTrue(space.holds, violated(runs) == 0, broken(runs) > 0)
    }
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.minutes)
