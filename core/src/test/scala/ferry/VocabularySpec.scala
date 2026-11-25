package ferry

import java.nio.file.Files

import scala.compiletime.constValueTuple
import scala.deriving.Mirror

import zio.*
import zio.test.*

import ferry.FerrySimulation.Family
import ferry.testkit.sim.Decisions
import ferry.testkit.tla.Tlc

/**
 * The code and the model speak the same language, and all of it is used.
 *
 *   - The names of [[Protocol.Action]]'s cases are the model's `Vocabulary`: an action that exists on one side only is
 *     a drift no trace would ever show.
 *   - Every action is taken by some simulated run of the code, and by some behaviour of the model: an action nothing
 *     exercises is checked by nothing.
 */
object VocabularySpec extends ZIOSpecDefault:

  private inline def caseNames[T](using m: Mirror.SumOf[T]): Set[String] =
    constValueTuple[m.MirroredElemLabels].productIterator.map(_.toString).toSet

  val code: Set[String] = caseNames[Protocol.Action[Any]]

  /** A model's `Vocabulary == {"Enqueue", ...}`, read from `formal/<module>.tla`. */
  def model(module: String = "Ferry"): Set[String] =
    val text = Files.readString(Tlc.formal.get.resolve(s"$module.tla"))
    val set = text.linesIterator.find(_.startsWith("Vocabulary ==")).get.dropWhile(_ != '{').drop(1).takeWhile(_ != '}')
    set.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toSet

  def spec = suite("Vocabulary")(
    test("the code's actions are exactly the model's") {
      assertTrue(code.nonEmpty, code == model())
    },
    test("every action is taken by some simulated run of the code") {
      for seen <- ZIO.attemptBlocking {
          (for
            family <- Family.values.toSeq
            seed   <- 1L to 40L
            run = FerrySimulation.run(FerrySimulation.scenario(family, seed), Decisions.seeded(seed))
            action <- run.observation.toSeq.flatMap(_.trace)
          yield Conformance.name(action)).toSet
        }.orDie
      yield assertTrue(model() -- seen == Set.empty[String])
    },
    test("every action is taken by some behaviour of the model") {
      for seen <- ZIO.attemptBlocking {
          List("Ferry", "FerryBatchOnly", "FerryMake")
            .flatMap(c => ModelReplaySpec.behaviours(c).toOption.toList.flatten)
            .flatMap(ModelReplaySpec.actionNames)
            .toSet
        }.orDie
      yield assertTrue(model() -- seen == Set.empty[String])
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(10.minutes)
