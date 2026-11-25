package ferry.explore

import zio.Chunk

import ferry.{FerrySimulation, Protocol}
import ferry.FerrySimulation.{Family, Scenario}
import ferry.Protocol.{Action, Schedule}
import ferry.testkit.sim.{Decisions, Explore}

/**
 * What a run of Ferry covers: the shapes of the protocol states it passed through.
 *
 * A run's recorded trace is replayed through the pure [[Protocol]] (each action names its step and every argument), and
 * each state is reduced to its shape: per key, the length of its lane, its schedule and its calls in flight; the length
 * of the ready line; the batches and direct calls in flight; the callers pending; whether admission is open. Callers
 * and keys are left out, so two states that differ only in who is where have the same shape.
 */
object Coverage:

  final case class Shape(
    keys: List[(Int, String, Int)],
    ready: Int,
    batches: Int,
    direct: Int,
    pending: Int,
    open: Boolean
  )

  def shape(p: Protocol[String, Unit]): Shape =
    val keys = (p.lanes.keySet ++ p.busy.keySet).toList.map { k =>
      val schedule = p.lanes.get(k).map(_.schedule) match
        case None                      => "none"
        case Some(Schedule.Waiting(_)) => "waiting"
        case Some(Schedule.Ready)      => "ready"
        case Some(Schedule.Blocked)    => "blocked"
      (p.lanes.get(k).fold(0)(_.items.size), schedule, p.busy.getOrElse(k, 0))
    }
    Shape(keys.sorted, p.ready.size, p.batches.size, p.direct.size, p.pending.size, p.open)

  /** The shapes of the states a trace passes through. */
  def shapes(limits: Protocol.Limits, trace: Chunk[Action[String]]): Set[Shape] =
    def fail(a: Action[String]) = throw IllegalStateException(s"the trace's step $a does not replay")
    trace
      .scanLeft(Protocol.empty[String, Unit](limits)) { (p, action) =>
        action match
          case Action.Enqueue(_, k, w, r) => p.submit(k, (), w, r, 0L).next
          case Action.Claim(_, k, w, r)   => p.submit(k, (), w, r, 0L).next
          case Action.Reject(_, k, w, r)  => p.submit(k, (), w, r, 0L).next
          case Action.Refuse(_, k, w, r)  => p.submit(k, (), w, r, 0L).next
          case Action.Due(k)              => p.due(k).getOrElse(fail(action)).next
          // Deadlines were recorded as Due steps of their own, so none is promoted here.
          case Action.Take(w, _, _)         => p.take(w, Long.MinValue).toOption.getOrElse(fail(action)).next
          case Action.Complete(w, outcomes) => p.complete(w, outcomes.toMap).getOrElse(fail(action)).next
          case Action.CompleteDirect(c, o)  => p.completeDirect(c, o).getOrElse(fail(action)).next
          case Action.Cancel(c)             => p.cancel(c).getOrElse(fail(action)).next
          case Action.Close(_)              => p.close.getOrElse(fail(action)).next
      }
      .map(shape)
      .toSet

  /** One simulated run of `s`, as exploration sees it. */
  def reached(s: Scenario)(decisions: Decisions): Explore.Reached[Shape] =
    val result = FerrySimulation.run(s, decisions)
    Explore.Reached(result.run.decisions, result.observation.fold(Set.empty)(o => shapes(s.limits, o.trace)))

  /**
   * Random against guided exploration, on the same budget, for scenarios of every family. Prints the shapes each found
   * after `runs` runs, per scenario, and the totals.
   *
   * {{{sbt "core/Test/runMain ferry.explore.Coverage 300 10 24"}}} (runs, scenarios per family, most callers)
   */
  def main(args: Array[String]): Unit =
    val runs      = args.headOption.flatMap(_.toIntOption).getOrElse(300)
    val scenarios = args.lift(1).flatMap(_.toIntOption).getOrElse(10)
    val callers   = args.lift(2).flatMap(_.toIntOption).getOrElse(24)
    println(s"distinct shapes after $runs runs: random, guided by forking, guided by mutating")
    val rows = for
      family <- Family.values.toList
      seed   <- 1L to scenarios.toLong
    yield
      val s      = FerrySimulation.scenario(family, seed, callers)
      val random = Explore.random(runs, seed)(reached(s)).last
      val fork   = Explore.guided(runs, seed, Explore.Strategy.Fork)(reached(s)).last
      val mutate = Explore.guided(runs, seed, Explore.Strategy.Mutate)(reached(s)).last
      println(f"$family%-14s seed $seed%3d  $random%5d $fork%5d $mutate%5d")
      (family, random, fork, mutate)
    rows.groupBy(_._1).toList.sortBy(_._1.ordinal).foreach { (family, rs) =>
      val (r, f, m) = (rs.map(_._2).sum, rs.map(_._3).sum, rs.map(_._4).sum)
      println(
        f"$family%-14s total  $r%6d $f%6d $m%6d   fork/random ${f.toDouble / r}%.2f, mutate/random ${m.toDouble / r}%.2f"
      )
    }
