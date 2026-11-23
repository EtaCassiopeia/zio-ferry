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
    test("rejects a trace in which a call completes before it was made") {
      for
        run <- recordedRun
        done  = run.actions.indexWhere { case Action.CompleteDirect(_, _) => true; case _ => false }
        early = run.actions(done) +: run.actions.patch(done, Nil, 1)
        verdict <- check("trace_early_completion", run.copy(actions = early))
      yield assertTrue(done > 0, verdict.isInstanceOf[Tlc.Verdict.Rejected])
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
