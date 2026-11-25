package ferry

import zio.*
import zio.test.*

object PartialProjectionSimulationSpec extends ZIOSpecDefault:

  import PartialProjectionSimulation.{
    crashMidProjection,
    lateRow,
    staleClear,
    throttledOnlyRow,
    throttledRow,
    Ack,
    Scenario,
    StoreError,
    Writer
  }

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
      for
        waits <- violations(throttledOnlyRow)
        eager <- violations(throttledOnlyRow, Writer(clearWhenComplete = false))
      yield assertTrue(waits == Nil, eager.exists(_.contains("marker cleared early")))
    },
    test("needs the clear's fence: a clear made before a second commit would clear the marker that commit set") {
      for
        fenced   <- violations(staleClear)
        unfenced <- violations(staleClear, Writer(fencedClear = false))
      yield assertTrue(fenced == Nil, unfenced.exists(_.contains("marker cleared early")))
    },
    test("needs a sweeper: a throttled row is otherwise never written") {
      for
        swept   <- violations(throttledOnlyRow)
        unswept <- violations(throttledOnlyRow.copy(sweeper = false))
      yield assertTrue(swept == Nil, unswept.exists(_.contains("still pending")))
    },
    test("finishes a projection whose node crashed between two of its row writes") {
      check(Gen.boolean) { sweeper =>
        val s = crashMidProjection.copy(sweeper = sweeper)
        trace(s).map(t => assertTrue(t.crashed == Set(0), PartialProjectionSimulation.violations(s, t) == Nil))
      }
    }
  ) @@ Effort.timeout(10.minutes)
