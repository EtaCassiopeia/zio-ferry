package ferry

import zio.*
import zio.test.*

object ProjectedWriterSimulationSpec extends ZIOSpecDefault:

  import ProjectedWriterSimulation.{Command, Fault, Scenario, Submission, Writer}

  private val one = List(Submission(Command(0, 0), 0, misrouted = false))

  /**
   * The writer's projection is late on the wire; meanwhile the sweeper projects the same record. Unfenced, both land.
   */
  private val lateProjection =
    Scenario(one, List(Fault.Pass, Fault.Late(30)), crash = None, crashBeforeProject = None, sweeper = true)

  /** The writer's projection is refused until its retries are exhausted: the caller is answered Committed. */
  private val refusedProjection =
    Scenario(
      one,
      Fault.Pass :: List.fill(6)(Fault.RefuseProject),
      crash = None,
      crashBeforeProject = None,
      sweeper = true
    )

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
      val quiet = Scenario(one, Nil, crash = None, crashBeforeProject = None, sweeper = true)
      for
        honest <- violations(quiet)
        early  <- violations(quiet, Writer(earlyAnswer = true))
      yield assertTrue(honest == Nil, early.exists(_.contains("answered Projected before it was in the view")))
    },
    test("finishes a projection whose node crashed between the two writes") {
      check(Gen.boolean) { sweeper =>
        val s = Scenario(one, Nil, crash = None, crashBeforeProject = Some(0), sweeper = sweeper)
        violations(s).map(found => assertTrue(found == Nil))
      }
    }
  ) @@ Effort.timeout(10.minutes)
