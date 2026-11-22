package ferry

import zio.*
import zio.test.*

import ferry.FerrySimulation.{Family, Result}
import ferry.testkit.sim.{Decisions, Shrink}
import ferry.testkit.tla.Tlc

/**
 * Every simulated run of Ferry keeps its contract, and is a behaviour of the TLA+ model.
 *
 * Runs are deterministic: a seed fixes the scenario and the simulator's decisions, so a failure is reported with its
 * seed and a shrunk decision script that reproduces it exactly. `FERRY_SAMPLES` sets the number of seeds per family.
 */
object SimulationConformanceSpec extends ZIOSpecDefault:

  private val samples = sys.env.get("FERRY_SAMPLES").flatMap(_.toIntOption).getOrElse(30)

  private def simulate(family: Family, seed: Long): Result =
    FerrySimulation.run(FerrySimulation.scenario(family, seed), Decisions.seeded(seed))

  // Every simulation runs on a blocking thread: the simulator drives its fibers from the calling thread, which must not
  // be one of the runtime's own workers.

  /** A contract failure, with the decision script shrunk to the shortest that still fails. */
  private def explain(result: Result, seed: Long, problems: Chunk[String]): String =
    val shrunk = Shrink.decisions(
      result.run.script,
      script => FerrySimulation.violations(FerrySimulation.run(result.scenario, Decisions.scripted(script))).nonEmpty,
      budget = 300
    )
    s"seed $seed: ${problems.mkString("; ")}\n  scenario: ${result.scenario}\n  shrunk script (${shrunk.size} of ${result.run.script.size}): $shrunk"

  def spec = suite("Simulated runs")(
    test("are a function of their scenario and decisions: the same seed gives the same run, and a script replays it") {
      ZIO
        .attemptBlocking {
          for
            family <- Family.values.toList
            seed   <- 1L to 5L
          yield
            val first   = simulate(family, seed)
            val second  = simulate(family, seed)
            val replay  = FerrySimulation.run(first.scenario, Decisions.scripted(first.run.script))
            val summary = first.observation.map(_.summary)
            summary.isDefined &&
            (first.run.decisions == second.run.decisions && second.observation.map(_.summary) == summary) &&
            (replay.run.decisions == first.run.decisions && replay.observation.map(_.summary) == summary)
        }
        .orDie
        .map(same => assertTrue(same.forall(identity)))
    },
    suite("keep the contract and are behaviours of the model")(
      Family.values.toList.map { family =>
        test(family.toString) {
          for
            results <- ZIO.attemptBlocking((1L to samples.toLong).map(seed => seed -> simulate(family, seed))).orDie
            broken = results.flatMap { (seed, r) =>
              val problems = FerrySimulation.violations(r)
              Option.when(problems.nonEmpty)(explain(r, seed, problems))
            }
            verdicts <- ZIO
              .foreachPar(results.filter((_, r) => r.observation.isDefined)) { (seed, r) =>
                ZIO
                  .attemptBlocking(
                    Conformance.validate(s"trace_${family}_$seed", r.observation.get.recorded(r.scenario))
                  )
                  .orDie
                  .map(seed -> _)
              }
              .withParallelism(4)
            rejected = verdicts.collect {
              case (seed, v) if !v.accepted => s"seed $seed: $v"
            }
          yield assertTrue(broken.isEmpty, rejected.isEmpty)
        }
      }*
    ) @@ TestAspect.sequential
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.minutes)
