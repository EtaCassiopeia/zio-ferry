package ferry.testkit.sim

import zio.*
import zio.stm.*
import zio.test.*

object SimulatorSpec extends ZIOSpecDefault:

  /**
   * A small racy program: workers append to a shared log under STM, sleep, time out and wait on each other, so its
   * result depends on scheduling and on which timer fires first.
   */
  val racy: UIO[List[String]] =
    for
      log    <- TRef.make(List.empty[String]).commit
      gate   <- TRef.make(0).commit
      fibers <- ZIO.foreach(1 to 4) { i =>
        (for
          _ <- log.update(s"start$i" :: _).commit
          _ <- ZIO.sleep((i % 2).millis)
          _ <- gate.update(_ + 1).commit
          _ <- gate.get.retryUntil(_ >= 2).commit.timeout(1.milli)
          n <- Simulator.choose("outcome", 3)
          _ <- log.update(s"end$i/$n" :: _).commit
        yield ()).fork
      }
      _      <- ZIO.foreachDiscard(fibers)(_.join)
      result <- log.get.commit
    yield result.reverse

  def spec = suite("Simulator")(
    test("the same decisions give the same run") {
      for runs <- ZIO.attemptBlocking((1 to 3).map(_ => Simulator.run(racy, Decisions.seeded(42)))).orDie
      yield assertTrue(
        runs.map(_.exit).distinct.size == 1,
        runs.map(_.decisions).distinct.size == 1,
        runs.forall(_.escapes.isEmpty),
        runs.head.decisions.nonEmpty
      )
    },
    test("a recorded run replays exactly from its decisions alone") {
      for pairs <- ZIO.attemptBlocking {
          (1L to 50L).map { seed =>
            val original = Simulator.run(racy, Decisions.seeded(seed))
            original -> Simulator.run(racy, Decisions.scripted(original.script))
          }
        }.orDie
      yield assertTrue(pairs.forall((a, b) => a.exit == b.exit && a.decisions == b.decisions))
    },
    test("different decisions explore different interleavings") {
      for exits <- ZIO.attemptBlocking((1L to 200L).map(s => Simulator.run(racy, Decisions.seeded(s)).exit)).orDie
      yield assertTrue(exits.distinct.size > 10)
    },
    test("virtual time passes without real time") {
      val slow = ZIO.sleep(10.days) *> Clock.nanoTime
      for run <- ZIO.attemptBlocking(Simulator.run(slow, Decisions.first)).orDie
      yield assertTrue(run.exit.contains(Exit.succeed(10.days.toNanos)), run.elapsed == 10.days)
    },
    test("a program that can never finish is reported as hung") {
      val stuck = Promise.make[Nothing, Unit].flatMap(_.await)
      for run <- ZIO.attemptBlocking(Simulator.run(stuck, Decisions.first)).orDie
      yield assertTrue(run.hung)
    }
  )
