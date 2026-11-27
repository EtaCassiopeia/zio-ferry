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
 * Every step of the protocol is used: each case of [[Protocol.Action]] is taken by some simulated run of the engine, so
 * none is checked by nothing.
 *
 * [[model]] reads a TLA+ model's `Vocabulary`: the writer models' steps are checked against their simulations' in their
 * own specs.
 */
object VocabularySpec extends ZIOSpecDefault:

  private inline def caseNames[T](using m: Mirror.SumOf[T]): Set[String] =
    constValueTuple[m.MirroredElemLabels].productIterator.map(_.toString).toSet

  val code: Set[String] = caseNames[Protocol.Action[Any]]

  /** A model's `Vocabulary == {"Send", ...}`, read from `formal/<module>.tla`. */
  def model(module: String): Set[String] =
    val text = Files.readString(Tlc.formal.get.resolve(s"$module.tla"))
    // The set may span lines: everything from `Vocabulary ==` to its closing brace.
    val set = text.drop(text.indexOf("\nVocabulary ==")).dropWhile(_ != '{').drop(1).takeWhile(_ != '}')
    set.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toSet

  def spec = suite("Vocabulary")(
    test("every action of the protocol is taken by some simulated run of the engine") {
      for seen <- ZIO.attemptBlocking {
          (for
            family <- Family.values.toSeq
            seed   <- 1L to 40L
            run = FerrySimulation.run(FerrySimulation.scenario(family, seed), Decisions.seeded(seed))
            action <- run.observation.toSeq.flatMap(_.trace)
          yield action.productPrefix).toSet
        }.orDie
      yield assertTrue(code.nonEmpty, code -- seen == Set.empty[String])
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(10.minutes)
