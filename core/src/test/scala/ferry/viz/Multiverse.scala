package ferry.viz

import zio.Chunk

import ferry.{Conformance, Engine, FerrySimulation}
import ferry.FerrySimulation.{Event, Result, Scenario}
import ferry.Protocol
import ferry.Protocol.Action
import ferry.testkit.sim.{Decisions, Shrink}
import ferry.testkit.tla.Tlc

/**
 * The branches of one scenario: a base run, and runs forked from it by changing one decision and letting the rest play
 * out. Forking is replay: a branch is its parent's first decisions, one of them changed, then fresh choices. So every
 * branch is reproducible from its script alone, however deep in the tree it sits.
 *
 * A branch can also be '''steered''': found by searching the decision space for a run with a given property (say, "a
 * caller gives up after its input was sent"), then shrunk to the shortest script that still has it. The script then
 * pins that interleaving for good. A fault-injection probe pauses a fiber at a point to force an interleaving; here the
 * interleaving is a sequence of decisions, found once and replayed exactly.
 */
object Multiverse:

  final case class Branch(
    id: Int,
    parent: Option[Int],
    label: String,
    forkAt: Option[Int], // the index of the decision changed from the parent
    forced: Int,         // how many leading decisions were imposed, not chosen freely
    result: Result,
    violations: Chunk[String],
    verdict: Tlc.Verdict
  ):
    def decisions            = result.run.decisions
    def events: Chunk[Event] = result.observation.fold(Chunk.empty)(_.timeline)
    def script: Chunk[Int]   = result.run.script

  /**
   * Explores `scenario` from `seed`: the base run, `fanOut` branches forked from it at decisions spread over the run,
   * and `fanOut` branches forked from each of those, deeper in.
   */
  def explore(
    scenario: Scenario,
    seed: Long,
    fanOut: Int = 4,
    validate: Boolean = true,
    rules: Engine.Rules = Engine.Rules.standard
  ): Vector[Branch] =
    val branches = Vector.newBuilder[Branch]
    var next     = 0

    def add(parent: Option[Int], label: String, forkAt: Option[Int], forced: Int, result: Result): Branch =
      val trace   = result.observation.map(o => o.recorded(scenario))
      val verdict =
        if !validate then Tlc.Verdict.Skipped("not validated")
        else trace.fold(Tlc.Verdict.Rejected("the run did not finish"))(t => Conformance.validate(s"branch_$next", t))
      val b = Branch(next, parent, label, forkAt, forced, result, FerrySimulation.violations(result), verdict)
      next += 1
      branches += b
      b

    def forks(from: Branch, depth: Int): Unit =
      val ds = from.decisions
      if depth > 0 && ds.nonEmpty then
        val start = from.forkAt.fold(0)(_ + 1)
        val spots = spread(start until ds.size, fanOut)
        spots.foreach { i =>
          val d      = ds(i)
          val other  = (d.took + 1) % d.options
          val prefix = from.script.take(i) :+ other
          val result =
            FerrySimulation.run(scenario, Decisions.prefixed(prefix, Decisions.seeded(seed * 1009 + i)), rules)
          val instead = result.run.decisions.lift(i).fold(s"option $other")(_.chose)
          val child   =
            add(Some(from.id), s"#$i ${d.point}: $instead instead of ${d.chose}", Some(i), prefix.size, result)
          forks(child, depth - 1)
        }

    val base = add(None, s"base run, seed $seed", None, 0, FerrySimulation.run(scenario, Decisions.seeded(seed), rules))
    forks(base, 2)
    branches.result()

  /**
   * Searches seeds for a run of `scenario` whose events satisfy `wanted`, then shrinks its decisions to the shortest
   * script that still does. Returns the steered run, or `None` if `tries` seeds found nothing.
   */
  def steer(
    scenario: Scenario,
    wanted: Chunk[Event] => Boolean,
    tries: Int = 500,
    rules: Engine.Rules = Engine.Rules.standard
  ): Option[(Long, Chunk[Int], Result)] =
    def holds(r: Result) = r.observation.exists(o => wanted(o.timeline))
    (1L to tries.toLong).iterator
      .map(seed => seed -> FerrySimulation.run(scenario, Decisions.seeded(seed), rules))
      .collectFirst { case (seed, r) if holds(r) => seed -> r }
      .map { (seed, found) =>
        val script =
          Shrink.decisions(
            found.run.script,
            s => holds(FerrySimulation.run(scenario, Decisions.scripted(s), rules)),
            400
          )
        (seed, script, FerrySimulation.run(scenario, Decisions.scripted(script), rules))
      }

  /** A batch call failed, and its key was served again afterwards: the key recovers from a failure. */
  val failedThenServed: Chunk[Event] => Boolean = events =>
    val failedAt = events.indexWhere {
      case Event(_, _, Action.Complete(_, outcomes)) => outcomes.exists(_._2 == Protocol.Outcome.Failed)
      case _                                         => false
    }
    failedAt >= 0 && events.drop(failedAt + 1).exists {
      case Event(_, _, Action.Take(_, _, _)) => true
      case _                                 => false
    }

  /** A caller gave up after its input had been taken into a batch: it is interrupted, yet its input is sent. */
  val gaveUpAfterSent: Chunk[Event] => Boolean = events =>
    val taken = events.zipWithIndex.collect { case (Event(_, _, Action.Take(_, _, batch)), i) =>
      batch.map(_ -> i)
    }.flatten
    events.zipWithIndex.exists {
      case (Event(_, _, Action.Cancel(c)), i) => taken.exists((t, at) => t == c && at < i)
      case _                                  => false
    }

  private def spread(range: Range, n: Int): Vector[Int] =
    if range.isEmpty then Vector.empty
    else if range.size <= n then range.toVector
    else (0 until n).map(k => range.start + (k * (range.size - 1)) / (n - 1).max(1)).distinct.toVector
