package ferry

import zio.*
import zio.test.*

object ProjectedWriterSimulationSpec extends ZIOSpecDefault:

  import ProjectedWriterSimulation.{crashBetweenWrites, lateProjection, quiet, refusedProjection, Scenario, Writer}

  private def violations(s: Scenario, writer: Writer = Writer()) =
    ProjectedWriterSimulation.run(s, writer).map(ProjectedWriterSimulation.violations(s, _))

  def spec = suite("ProjectedWrite on a fleet")(
    test("applies and projects every command exactly once, and answers it truthfully, under faults and crashes") {
      check(ProjectedWriterSimulation.scenario)(s => violations(s).map(found => assertTrue(found == Nil)))
    } @@ Effort.samples(150),
    test("needs the projection's fence: a late projection and a sweeper's would both land") {
      for
        fenced   <- violations(lateProjection)
        unfenced <- violations(lateProjection, Writer(fencedProject = false))
      yield assertTrue(fenced == Nil, unfenced.exists(_.contains("projected an entry twice")))
    },
    test("needs a sweeper: a projection that ran out of retries is otherwise never finished") {
      for
        swept   <- violations(refusedProjection)
        unswept <- violations(refusedProjection.copy(sweeper = false))
      yield assertTrue(swept == Nil, unswept.exists(_.contains("still pending")))
    },
    test("needs to answer after the projection: an answer straight after the commit claims a view that is not there") {
      for
        honest <- violations(quiet)
        early  <- violations(quiet, Writer(earlyAnswer = true))
      yield assertTrue(honest == Nil, early.exists(_.contains("answered Projected before it was in the view")))
    },
    test("finishes a projection whose node crashed between the two writes") {
      check(Gen.boolean) { sweeper =>
        violations(crashBetweenWrites.copy(sweeper = sweeper)).map(found => assertTrue(found == Nil))
      }
    }
  ) @@ Effort.timeout(10.minutes)
