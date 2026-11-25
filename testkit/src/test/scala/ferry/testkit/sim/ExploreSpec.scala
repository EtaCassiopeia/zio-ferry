package ferry.testkit.sim

import zio.*
import zio.test.*

object ExploreSpec extends ZIOSpecDefault:

  /** One run of the racy program: the fingerprints are the orders in which it logged. */
  private def reached(decisions: Decisions): Explore.Reached[List[String]] =
    val run = Simulator.run(SimulatorSpec.racy, decisions)
    Explore.Reached(run.decisions, run.exit.flatMap(_.toEither.toOption).toSet)

  def spec = suite("Explore")(
    test("is reproducible, and never forgets what it has seen") {
      ZIO
        .attemptBlocking {
          val curves = for
            _        <- 1 to 2
            strategy <- Explore.Strategy.values.toList
          yield (Explore.random(30, 7L)(reached), Explore.guided(30, 7L, strategy)(reached))
          curves
        }
        .orDie
        .map { curves =>
          val all = curves.flatMap((r, g) => Seq(r, g))
          assertTrue(
            curves.take(2) == curves.drop(2),
            all.forall(c => c.size == 30 && c.zip(c.tail).forall(_ <= _) && c.head >= 1)
          )
        }
    }
  )
