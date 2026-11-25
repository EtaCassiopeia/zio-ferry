package ferry

import zio.*
import zio.test.*

import ferry.ProjectedWriterSimulation.{
  crashBetweenWrites,
  failedCommit,
  lateProjection,
  quiet,
  refusedProjection,
  Scenario,
  Writer
}
import ferry.testkit.tla.{Tlc, Value}

/**
 * The projected writer, as simulated, is a behaviour of `formal/ProjectedWriter.tla`, key by key; and each safeguard of
 * the writer is a switch of the model: a writer without one is rejected by the model with it, and accepted by the model
 * without it, which model checking shows to be unsafe.
 */
object ProjectedWriterConformanceSpec extends ZIOSpecDefault:

  private def verdicts(name: String, s: Scenario, writer: Writer, model: Writer) =
    ProjectedWriterSimulation
      .run(s, writer)
      .flatMap(t => ZIO.attemptBlocking(ProjectedWriterConformance.check(name, s, t, model)).orDie)

  private def accepted(vs: Map[Int, Tlc.Verdict]) = vs.nonEmpty && vs.values.forall(_.accepted)

  private def rejected(vs: Map[Int, Tlc.Verdict]) = vs.values.exists(_.isInstanceOf[Tlc.Verdict.Rejected])

  private val named = List(quiet, lateProjection, refusedProjection, crashBetweenWrites, failedCommit)

  def spec = suite("The projected writer and its model")(
    test("every simulated run, under faults, misrouting and crashes, is a behaviour of the model, key by key") {
      check(ProjectedWriterSimulation.scenario) { s =>
        verdicts("projected", s, Writer(), Writer()).map(vs => assertTrue(accepted(vs)))
      }
    } @@ Effort.samples(25),
    test("so are the named scenarios: a late or refused projection, a failed commit, a crash between the writes") {
      ZIO
        .foreach(named.zipWithIndex)((s, i) => verdicts(s"named$i", s, Writer(), Writer()))
        .map(vs => assertTrue(vs.forall(accepted)))
    },
    test("the simulated runs, and the named scenarios, take every step of the model") {
      // A fixed seed, and the named scenarios: some steps need several faults in a row.
      for
        _      <- TestRandom.setSeed(20261125L)
        random <- ProjectedWriterSimulation.scenario.runCollectN(200)
        runs   <- ZIO.foreach(random ++ named)(s => ProjectedWriterSimulation.run(s).map(s -> _))
        names = runs.flatMap { (s, t) =>
          ProjectedWriterConformance
            .traces(t.events, s.submissions.map(_.command.key).toSet)
            .values
            .flatten
            .collect { case Value.Rec(fields) => fields("name") }
        }.toSet
      yield assertTrue(names == VocabularySpec.model("ProjectedWriter").map(Value.str))
    },
    test("a writer without the projection's fence is rejected by the fenced model, and is a behaviour of the other") {
      val unfenced = Writer(fencedProject = false)
      for
        fenced <- verdicts("unfenced", lateProjection, unfenced, Writer())
        weak   <- verdicts("unfenced", lateProjection, unfenced, unfenced)
      yield assertTrue(rejected(fenced), accepted(weak))
    },
    test("a writer that answers at the commit is rejected by the honest model, and is a behaviour of the other") {
      val early = Writer(earlyAnswer = true)
      for
        honest <- verdicts("early", quiet, early, Writer())
        weak   <- verdicts("early", quiet, early, early)
      yield assertTrue(rejected(honest), accepted(weak))
    }
  ) @@ TestAspect.sequential @@ Effort.timeout(15.minutes)
