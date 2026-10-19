package ferry

import zio.*
import zio.test.*

object PartialProjectionSimulationSpec extends ZIOSpecDefault:

  import PartialProjectionSimulation.{Ack, Command, Fault, Scenario, StoreError, Submission, Writer}

  private def quiet(submissions: List[Submission]) =
    Scenario(submissions, Nil, Nil, crash = None, crashBeforeProject = None, crashMidProjection = None, sweeper = true)

  private val one = List(Submission(Command(0, 0), 0, misrouted = false))

  /** Three commands on one key at once; the second row written is throttled. */
  private val throttledRow =
    quiet(List.tabulate(3)(id => Submission(Command(id, 0), 0, misrouted = false)))
      .copy(rowFaults = List(Fault.Pass, Fault.ThrottleRow))

  /** The writer's row is late on the wire; meanwhile the sweeper writes the same row. */
  private val lateRow = quiet(one).copy(rowFaults = List(Fault.Late(30)))

  /**
   * Command 0's clear is late on the wire; meanwhile command 1, sent to the other node, commits, and its row is later
   * still. The stale clear lands while row 1 is missing. No sweeper, which would otherwise finish row 1 first.
   */
  private val staleClear =
    quiet(List(Submission(Command(0, 0), 0, misrouted = false), Submission(Command(1, 0), 5, misrouted = true)))
      .copy(faults = List(Fault.Pass, Fault.Late(30)), rowFaults = List(Fault.Pass, Fault.Late(60)), sweeper = false)

  private def trace(s: Scenario, writer: Writer = Writer()) = PartialProjectionSimulation.run(s, writer)

  private def violations(s: Scenario, writer: Writer = Writer()) =
    trace(s, writer).map(PartialProjectionSimulation.violations(s, _))

  def spec = suite("ProjectedWrite per entry on a fleet")(
    test(
      "writes every row exactly once, clears every marker honestly, and answers truthfully, under faults and crashes"
    ) {
      check(PartialProjectionSimulation.scenario)(s => violations(s).map(found => assertTrue(found == Nil)))
    } @@ Effort.samples(150),
    test("answers a batch with a throttled row per input: that row Committed with its cause, the others Projected") {
      for t <- trace(throttledRow)
      yield
        val acks = t.acks.values.toList
        assertTrue(
          PartialProjectionSimulation.violations(throttledRow, t) == Nil,
          acks.count(_.cause.contains(StoreError.Throttled)) == 1,
          acks.count(_.projected) == 2,
          acks.forall { case Ack(_, projected, _, cause) => projected == cause.isEmpty }
        )
    },
    test("needs idempotent rows: a late row and a sweeper's would both land") {
      for
        conditional   <- violations(lateRow)
        unconditional <- violations(lateRow, Writer(conditionalRows = false))
      yield assertTrue(conditional == Nil, unconditional.exists(_.contains("wrote a row twice")))
    },
    test("needs the clear to wait for every row: one that does not clears the marker with a throttled row missing") {
      val s = quiet(one).copy(rowFaults = List(Fault.ThrottleRow))
      for
        waits <- violations(s)
        eager <- violations(s, Writer(clearWhenComplete = false))
      yield assertTrue(waits == Nil, eager.exists(_.contains("marker cleared early")))
    },
    test("needs the clear's fence: a clear made before a second commit would clear the marker that commit set") {
      for
        fenced   <- violations(staleClear)
        unfenced <- violations(staleClear, Writer(fencedClear = false))
      yield assertTrue(fenced == Nil, unfenced.exists(_.contains("marker cleared early")))
    },
    test("needs a sweeper: a throttled row is otherwise never written") {
      val s = quiet(one).copy(rowFaults = List(Fault.ThrottleRow))
      for
        swept   <- violations(s)
        unswept <- violations(s.copy(sweeper = false))
      yield assertTrue(swept == Nil, unswept.exists(_.contains("still pending")))
    },
    test("finishes a projection whose node crashed between two of its row writes") {
      val two = List.tabulate(2)(id => Submission(Command(id, 0), 0, misrouted = false))
      check(Gen.boolean) { sweeper =>
        val s = quiet(two).copy(crashMidProjection = Some(0), sweeper = sweeper)
        trace(s).map(t => assertTrue(t.crashed == Set(0), PartialProjectionSimulation.violations(s, t) == Nil))
      }
    }
  ) @@ Effort.timeout(10.minutes)
