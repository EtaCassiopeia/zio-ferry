package ferry

import zio.*
import zio.test.*

import ferry.PartialProjectionSimulation.{
  crashMidProjection,
  failedCommit,
  lateRow,
  staleClear,
  throttledOnlyRow,
  throttledRow,
  Scenario,
  Writer
}
import ferry.testkit.tla.{Tlc, Value}

/**
 * The per-entry projected writer, as simulated, is a behaviour of `formal/ProjectedWriterPartial.tla`, key by key; and
 * each safeguard of the writer is a switch of the model: a writer without one is rejected by the model with it, and
 * accepted by the model without it, which model checking shows to be unsafe.
 */
object PartialProjectionConformanceSpec extends ZIOSpecDefault:

  private def verdicts(name: String, s: Scenario, writer: Writer, model: Writer) =
    PartialProjectionSimulation
      .run(s, writer)
      .flatMap(t => ZIO.attemptBlocking(PartialProjectionConformance.check(name, s, t, model)).orDie)

  private def accepted(vs: Map[Int, Tlc.Verdict]) = vs.nonEmpty && vs.values.forall(_.accepted)

  private def rejected(vs: Map[Int, Tlc.Verdict]) = vs.values.exists(_.isInstanceOf[Tlc.Verdict.Rejected])

  private val named =
    List(throttledRow, throttledOnlyRow, lateRow, staleClear, crashMidProjection, failedCommit)

  /** A writer without one safeguard: rejected by the model with it, a behaviour of the model without it. */
  private def needs(name: String, s: Scenario, weak: Writer) =
    for
      strong <- verdicts(name, s, weak, Writer())
      same   <- verdicts(name, s, weak, weak)
    yield assertTrue(rejected(strong), accepted(same))

  def spec = suite("The per-entry projected writer and its model")(
    test("every simulated run, under faults, misrouting and crashes, is a behaviour of the model, key by key") {
      check(PartialProjectionSimulation.scenario) { s =>
        verdicts("partial", s, Writer(), Writer()).map(vs => assertTrue(accepted(vs)))
      }
    } @@ Effort.samples(25),
    test("so are the named scenarios: throttled, late and lost rows, a stale clear, crashes, a failed commit") {
      ZIO
        .foreach(named.zipWithIndex)((s, i) => verdicts(s"named$i", s, Writer(), Writer()))
        .map(vs => assertTrue(vs.forall(accepted)))
    },
    test("the simulated runs, and the named scenarios, take every step of the model") {
      // A fixed seed, and the named scenarios: some steps need several faults in a row.
      for
        _      <- TestRandom.setSeed(20261125L)
        random <- PartialProjectionSimulation.scenario.runCollectN(200)
        runs   <- ZIO.foreach(random ++ named)(s => PartialProjectionSimulation.run(s).map(s -> _))
        names = runs.flatMap { (s, t) =>
          PartialProjectionConformance
            .traces(t.events, s.submissions.map(_.command.key).toSet)
            .values
            .flatten
            .collect { case Value.Rec(fields) => fields("name") }
        }.toSet
      yield assertTrue(names == VocabularySpec.model("ProjectedWriterPartial").map(Value.str))
    },
    test("rows written unconditionally are rejected by the model with idempotent rows") {
      needs("unconditional", lateRow, Writer(conditionalRows = false))
    },
    test("a clear that does not wait for every row is rejected by the model whose clear waits") {
      needs("eager", throttledOnlyRow, Writer(clearWhenComplete = false))
    },
    test("an unfenced clear is rejected by the model with the clear's fence") {
      needs("unfenced", staleClear, Writer(fencedClear = false))
    }
  ) @@ TestAspect.sequential @@ Effort.timeout(15.minutes)
