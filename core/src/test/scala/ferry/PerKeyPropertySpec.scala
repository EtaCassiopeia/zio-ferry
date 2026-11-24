package ferry

import zio.*
import zio.test.*

object PerKeyPropertySpec extends ZIOSpecDefault:

  import FencedWriterSimulation.{Command, Fault, Scenario, Submission, Writer}

  /** Command 0 is sent to its owner, whose write is delayed on the wire past the owner's crash. */
  private val lateWriteAfterCrash =
    Scenario(List(Submission(Command(0, 0), 0, misrouted = false)), List(Fault.Late(50)), Some(0 -> 10L))

  /** Command 0 is applied, but the response is lost, so the writer retries. */
  private val lostResponse =
    Scenario(List(Submission(Command(0, 0), 0, misrouted = false)), List(Fault.LostResponse), None)

  private def violations(s: Scenario, writer: Writer) =
    FencedWriterSimulation.run(s, writer).map(FencedWriterSimulation.violations(s, _))

  def spec = suite("Ferry.perKey properties")(
    suite("fenced single writer")(
      test("applies every command exactly once and answers it truthfully, under faults, misrouting and a crash") {
        check(FencedWriterSimulation.scenario) { s =>
          violations(s, Writer()).map(found => assertTrue(found == Nil))
        }
      } @@ Effort.samples(150),
      test("needs the fence: a write still on the wire from a crashed owner would apply a command twice") {
        for
          fenced   <- violations(lateWriteAfterCrash, Writer())
          unfenced <- violations(lateWriteAfterCrash, Writer(fenced = false))
        yield assertTrue(fenced == Nil, unfenced.exists(_.contains("applied a command twice")))
      },
      test("needs deduplication: a retry after a lost response would apply a command twice") {
        for
          deduplicated <- violations(lostResponse, Writer())
          blind        <- violations(lostResponse, Writer(deduplicated = false))
        yield assertTrue(deduplicated == Nil, blind.exists(_.contains("applied a command twice")))
      }
    )
  ) @@ Effort.timeout(10.minutes)
