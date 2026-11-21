package ferry

import zio.*
import zio.test.*

import ferry.Protocol.Action
import ferry.testkit.tla.Tlc

/** Trace validation on hand-made runs: a real run is accepted, and a trace that differs from it is rejected. */
object TraceValidationSpec extends ZIOSpecDefault:

  /** A run of `Ferry.perKey` on the real runtime: a direct call, a batch behind it, a give-up and a close. */
  val recordedRun: UIO[Conformance.Recorded[String]] =
    for
      config <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 2, maxInFlightBatches = 2, maxQueueing = 8)).orDie
      limits = Ferry.perKeyLimits(config)
      recorder <- Recorder.inMemory[String]
      gate     <- Promise.make[Nothing, Unit]
      _        <- ZIO.scoped {
        for
          ferry <- Engine.make(limits, 2, (_: (String, Int))._1, (_: (String, Int)) => 1L, _ => 0L, recorder) {
            (_: String, inputs: NonEmptyChunk[(String, Int)]) => gate.await.as(inputs.size)
          }((_, n) => n)
          first <- ferry("a" -> 1).fork                           // claims idle key a
          _     <- recorder.trace.repeatUntil(_.size == 1)
          rest  <- ZIO.foreach(2 to 4)(n => ferry("a" -> n).fork) // wait behind it
          _     <- recorder.trace.repeatUntil(_.size == 4)
          _     <- rest.head.interrupt                            // 2 gives up
          _     <- gate.succeed(())
          _     <- (first.join *> ZIO.foreach(rest.tail)(_.join)).ignore
        yield ()
      }
      trace <- recorder.trace
    yield Conformance.Recorded(limits, 2, trace, identity)

  private def check(name: String, run: Conformance.Recorded[String]) =
    ZIO.attemptBlocking(Conformance.validate(name, run)).orDie

  def spec = suite("Trace validation")(
    test("accepts a recorded run of the code") {
      for
        run     <- recordedRun
        verdict <- check("trace_accepted", run)
      yield assertTrue(
        run.actions
          .map(Conformance.name)
          .toSet
          .subsetOf(Set("Claim", "Enqueue", "Cancel", "CompleteDirect", "Due", "Take", "Complete", "Close")),
        verdict == Tlc.Verdict.Accepted
      )
    },
    test("rejects a trace in which two steps are swapped") {
      for
        run <- recordedRun
        swapped = run.copy(actions =
          run.actions.take(1) ++ run.actions.slice(2, 3) ++ run.actions.slice(1, 2) ++ run.actions.drop(3)
        )
        verdict <- check("trace_swapped", swapped)
      yield assertTrue(swapped.actions != run.actions, verdict.isInstanceOf[Tlc.Verdict.Rejected])
    },
    test("rejects a trace with a batch the model would not take") {
      for
        run <- recordedRun
        changed = run.copy(actions = run.actions.map {
          case Action.Take(w, k, batch) => Action.Take(w, k, batch.reverse)
          case other                    => other
        })
        verdict <- check("trace_reordered_batch", changed)
      yield assertTrue(changed.actions != run.actions, verdict.isInstanceOf[Tlc.Verdict.Rejected])
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(2.minutes)
