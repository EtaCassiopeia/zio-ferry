package ferry

import zio.*
import zio.test.*

import ferry.FencedWriterSimulation.{lateWriteAfterCrash, lostResponse, Scenario, Writer}
import ferry.testkit.tla.{Tlc, Value}

/**
 * The fenced single writer, as simulated, is a behaviour of `formal/FencedWriter.tla`, key by key; and each safeguard
 * of the writer is a switch of the model: a writer without one is rejected by the model with it, and accepted by the
 * model without it, which model checking shows to be unsafe.
 */
object FencedWriterConformanceSpec extends ZIOSpecDefault:

  private def verdicts(name: String, s: Scenario, writer: Writer, model: Writer) =
    FencedWriterSimulation
      .run(s, writer)
      .flatMap(t => ZIO.attemptBlocking(FencedWriterConformance.check(name, s, t, model)).orDie)

  private def rejected(vs: Map[Int, Tlc.Verdict]) = vs.values.exists(_.isInstanceOf[Tlc.Verdict.Rejected])

  def spec = suite("The fenced single writer and its model")(
    test("every simulated run, under faults, misrouting and a crash, is a behaviour of the model, key by key") {
      check(FencedWriterSimulation.scenario) { s =>
        verdicts("writer", s, Writer(), Writer()).map(vs => assertTrue(vs.nonEmpty, vs.values.forall(_.accepted)))
      }
    } @@ Effort.samples(25),
    test("the simulated runs take every step of the model") {
      for
        scenarios <- FencedWriterSimulation.scenario.runCollectN(200)
        runs      <- ZIO.foreach(scenarios)(s => FencedWriterSimulation.run(s).map(s -> _))
        names = runs.flatMap { (s, t) =>
          FencedWriterConformance
            .traces(t.events, s.submissions.map(_.command.key).toSet)
            .values
            .flatten
            .collect { case Value.Rec(fields) => fields("name") }
        }.toSet
      yield assertTrue(names == VocabularySpec.model("FencedWriter").map(Value.str))
    },
    test("a writer without the fence is rejected by the fenced model, and is a behaviour of the unfenced one") {
      val unfenced = Writer(fenced = false)
      for
        fenced <- verdicts("unfenced", lateWriteAfterCrash, unfenced, Writer())
        weak   <- verdicts("unfenced", lateWriteAfterCrash, unfenced, unfenced)
      yield assertTrue(rejected(fenced), weak.values.forall(_.accepted))
    },
    test("a writer without deduplication is rejected by the deduplicating model, and is a behaviour of the other") {
      val blind = Writer(deduplicated = false)
      for
        deduplicated <- verdicts("blind", lostResponse, blind, Writer())
        weak         <- verdicts("blind", lostResponse, blind, blind)
      yield assertTrue(rejected(deduplicated), weak.values.forall(_.accepted))
    }
  ) @@ TestAspect.sequential @@ Effort.timeout(10.minutes)
