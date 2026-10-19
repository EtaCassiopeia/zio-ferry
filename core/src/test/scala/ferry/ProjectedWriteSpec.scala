package ferry

import zio.*
import zio.test.*

import ProjectedWrite.Outcome

object ProjectedWriteSpec extends ZIOSpecDefault:

  /**
   * A key's record: its log, whose content is the commit's fence, and how much of the log is projected (the marker).
   */
  final private case class Record(log: Vector[Int], projectedUpTo: Int)

  private val empty = Record(Vector.empty, 0)

  private def pending(r: Record): Boolean = r.projectedUpTo < r.log.size

  private enum Fault:
    case Conflict, LostResponse, Refuse

  /**
   * One record and one view per key. A commit writes the log only if it is still the log the writer read, and leaves
   * the marker as stored. A projection appends the record's unprojected entries to the view and advances the marker,
   * only if the store still holds exactly the record the projector read.
   */
  final private case class Store(
    state: Ref[Map[String, (Record, Vector[Int])]],
    readCalls: Ref[Int],
    commitCalls: Ref[Int],
    projectCalls: Ref[Int],
    commitFaults: Ref[List[Fault]],
    projectFaults: Ref[List[Fault]]
  ):

    def read(key: String): IO[String, Record] = state.get.map(_.get(key).fold(empty)(_._1))

    /** The read the writer makes, counted; the tests' own reads go through `read`. */
    def readCounted(key: String): IO[String, Record] = readCalls.update(_ + 1) *> read(key)

    def view(key: String): UIO[Vector[Int]] = state.get.map(_.get(key).fold(Vector.empty)(_._2))

    def commit(key: String, before: Record, after: Record): IO[String, Unit] =
      commitCalls.update(_ + 1) *> next(commitFaults).flatMap {
        case Some(Fault.Conflict) => // another writer appended first
          state.update(m =>
            m.updated(
              key,
              m.get(key).fold((Record(Vector(-1), 0), Vector.empty[Int]))((r, v) => (r.copy(log = r.log :+ -1), v))
            )
          ) *>
            ZIO.fail("conflict")
        case Some(Fault.LostResponse) => applyCommit(key, before, after) *> ZIO.fail("timeout")
        case Some(Fault.Refuse)       => ZIO.fail("refused")
        case None                     => applyCommit(key, before, after)
      }

    def project(key: String, s: Record): IO[String, Unit] =
      projectCalls.update(_ + 1) *> next(projectFaults).flatMap {
        case Some(Fault.Conflict) => applyProject(key, s).ignore *> ZIO.fail("conflict") // a sweeper got there first
        case Some(Fault.LostResponse) => applyProject(key, s) *> ZIO.fail("timeout")
        case Some(Fault.Refuse)       => ZIO.fail("refused")
        case None                     => applyProject(key, s)
      }

    private def next(faults: Ref[List[Fault]]): UIO[Option[Fault]] = faults.modify(f => f.headOption -> f.drop(1))

    private def applyCommit(key: String, before: Record, after: Record): IO[String, Unit] =
      state.modify { m =>
        val (record, view) = m.getOrElse(key, (empty, Vector.empty[Int]))
        if record.log == before.log then Right(()) -> m.updated(key, (record.copy(log = after.log), view))
        else Left("conflict")                      -> m
      }.absolve

    private def applyProject(key: String, s: Record): IO[String, Unit] =
      state.modify { m =>
        val (record, view) = m.getOrElse(key, (empty, Vector.empty[Int]))
        if record == s then
          Right(())        -> m.updated(key, (s.copy(projectedUpTo = s.log.size), view ++ s.log.drop(s.projectedUpTo)))
        else Left("stale") -> m
      }.absolve

  private def store(
    commitFaults: List[Fault] = Nil,
    projectFaults: List[Fault] = Nil,
    seed: Map[String, (Record, Vector[Int])] = Map.empty
  ): UIO[Store] =
    for
      state        <- Ref.make(seed)
      readCalls    <- Ref.make(0)
      commitCalls  <- Ref.make(0)
      projectCalls <- Ref.make(0)
      cf           <- Ref.make(commitFaults)
      pf           <- Ref.make(projectFaults)
    yield Store(state, readCalls, commitCalls, projectCalls, cf, pf)

  /** Inputs are numbers; zero is invalid, and a number already in the log is answered with its position. */
  private def step(r: Record, n: Int): (Record, Either[String, Int]) =
    if n == 0 then r -> Left("zero")
    else if r.log.contains(n) then r -> Right(r.log.indexOf(n))
    else r.copy(log = r.log :+ n)    -> Right(r.log.size)

  private def fold(s: Store): FoldWrite[Any, String, String, Int, Int, Record, Either[String, Int]] =
    FoldWrite((n: Int) => n, s.readCounted, step, s.commit, Schedule.recurs(3))

  private def projected(s: Store): ProjectedWrite[Any, String, String, Int, Int, Record, Either[String, Int]] =
    ProjectedWrite.wholeRecord(fold(s), pending, s.project)

  def spec = suite("ProjectedWrite")(
    test("commits once and projects once, and answers each input with its own position") {
      for
        s <- store()
        write = projected(s)
        outcomes <- write.call("k", NonEmptyChunk(5, 0, 7))
        record   <- s.read("k")
        view     <- s.view("k")
        reads    <- s.readCalls.get
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
      yield assertTrue(
        reads == 1, // the state just committed is projected as it is, without reading it again
        commits == 1,
        projects == 1,
        record == Record(Vector(5, 7), 2),
        view == Vector(5, 7),
        outcomes == Map(
          5 -> Outcome.Projected(Right(0)),
          0 -> Outcome.Projected(Left("zero")),
          7 -> Outcome.Projected(Right(1))
        ),
        write.outcome(7, outcomes).value == Right(1),
        write.outcome(7, outcomes).isProjected
      )
    },
    test("makes no write and answers from the state when nothing changed and nothing is pending") {
      for
        s        <- store()
        outcomes <- projected(s).call("k", NonEmptyChunk(0))
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
      yield assertTrue(commits == 0, projects == 0, outcomes == Map(0 -> Outcome.Projected(Left("zero"))))
    },
    test("answers Committed when the projection exhausts its retries, and resume finishes it later") {
      for
        s <- store(projectFaults = List.fill(5)(Fault.Refuse))
        write = projected(s)
        outcomes <- write.call("k", NonEmptyChunk(5, 7))
        stuck    <- s.read("k")
        before   <- s.view("k")
        _        <- write.resume("k")
        record   <- s.read("k")
        view     <- s.view("k")
      yield assertTrue(
        outcomes == Map(5 -> Outcome.Committed(Right(0), "refused"), 7 -> Outcome.Committed(Right(1), "refused")),
        !outcomes(5).isProjected,
        stuck == Record(Vector(5, 7), 0),
        before == Vector.empty,
        record == Record(Vector(5, 7), 2),
        view == Vector(5, 7)
      )
    },
    test("answers Projected without projecting twice when a sweeper projected the record first") {
      for
        s        <- store(projectFaults = List(Fault.Conflict))
        outcomes <- projected(s).call("k", NonEmptyChunk(5, 7))
        view     <- s.view("k")
        projects <- s.projectCalls.get
      yield assertTrue(
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1))),
        view == Vector(5, 7),
        projects == 1
      )
    },
    test("never applies an input twice when a commit landed but its response was lost, and projects it once") {
      for
        s        <- store(commitFaults = List(Fault.LostResponse))
        outcomes <- projected(s).call("k", NonEmptyChunk(5, 7))
        record   <- s.read("k")
        view     <- s.view("k")
        commits  <- s.commitCalls.get
      yield assertTrue(
        commits == 1,
        record == Record(Vector(5, 7), 2),
        view == Vector(5, 7),
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("reapplies on top of another writer's commit after a conflict") {
      for
        s        <- store(commitFaults = List(Fault.Conflict))
        outcomes <- projected(s).call("k", NonEmptyChunk(5, 7))
        view     <- s.view("k")
      yield assertTrue(
        view == Vector(-1, 5, 7),
        outcomes == Map(5 -> Outcome.Projected(Right(1)), 7 -> Outcome.Projected(Right(2)))
      )
    },
    test("makes no write when a replayed batch is already committed and projected") {
      for
        s        <- store(seed = Map("k" -> (Record(Vector(5, 7), 2), Vector(5, 7))))
        outcomes <- projected(s).call("k", NonEmptyChunk(5, 7))
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
      yield assertTrue(
        commits == 0,
        projects == 0,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("projects without committing when a replayed batch is committed but its projection is pending") {
      for
        s        <- store(seed = Map("k" -> (Record(Vector(5, 7), 0), Vector.empty)))
        outcomes <- projected(s).call("k", NonEmptyChunk(5, 7))
        reads    <- s.readCalls.get
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
        view     <- s.view("k")
      yield assertTrue(
        reads == 1,
        commits == 0,
        projects == 1,
        view == Vector(5, 7),
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("fails with the store's error, and projects nothing, once the commit's retries are exhausted") {
      for
        s        <- store(commitFaults = List.fill(4)(Fault.Refuse))
        exit     <- projected(s).call("k", NonEmptyChunk(5)).exit
        record   <- s.read("k")
        projects <- s.projectCalls.get
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).contains("refused"),
        record == empty,
        projects == 0
      )
    },
    test("resume makes no write when nothing is pending, and projects a pending record") {
      for
        idle     <- store(seed = Map("k" -> (Record(Vector(5), 1), Vector(5))))
        nothing  <- projected(idle).resume("k")
        none     <- idle.projectCalls.get
        busy     <- store(seed = Map("k" -> (Record(Vector(5, 7), 1), Vector(5))))
        finished <- projected(busy).resume("k")
        record   <- busy.read("k")
        view     <- busy.view("k")
      yield assertTrue(
        none == 0,
        nothing == ProjectedWrite.Projection.complete[Int, String],
        finished == ProjectedWrite.Projection.complete[Int, String],
        record == Record(Vector(5, 7), 2),
        view == Vector(5, 7)
      )
    },
    test("resume fails with the store's error once its retries are exhausted, and leaves the record pending") {
      for
        s <- store(projectFaults = List.fill(4)(Fault.Refuse), seed = Map("k" -> (Record(Vector(5), 0), Vector.empty)))
        exit   <- projected(s).resume("k").exit
        record <- s.read("k")
      yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains("refused"), record == Record(Vector(5), 0))
    },
    test("never projects an entry twice when resumes race each other and a batch of the same key") {
      for
        s <- store(seed = Map("k" -> (Record(Vector(5, 7), 0), Vector.empty)))
        write = projected(s)
        results <- ZIO.foreachPar(1 to 8)(_ => write.resume("k").either) <&> write.call("k", NonEmptyChunk(9))
        record  <- s.read("k")
        view    <- s.view("k")
      yield
        val (resumed, outcomes) = results
        assertTrue(
          resumed.forall(_.isRight),
          record == Record(Vector(5, 7, 9), 3),
          view == Vector(5, 7, 9),
          outcomes == Map(9 -> Outcome.Projected(Right(2)))
        )
    },
    test("re-reads and projects both commits when another writer commits between this call's two writes") {
      for
        s     <- store(seed = Map("k" -> (Record(Vector(5), 0), Vector.empty)))
        rival <- Ref.make(true)
        // The first projection finds that another writer committed 7 since this call committed 9.
        interleaved = (key: String, r: Record) =>
          rival.getAndSet(false).flatMap(first => fold(s).call(key, NonEmptyChunk(7)).when(first)) *> s.project(key, r)
        write = ProjectedWrite.wholeRecord(fold(s), pending, interleaved)
        outcomes <- write.call("k", NonEmptyChunk(9))
        record   <- s.read("k")
        view     <- s.view("k")
        projects <- s.projectCalls.get
      yield assertTrue(
        projects == 2, // the stale one was refused, the re-read one landed
        record == Record(Vector(5, 9, 7), 3),
        view == Vector(5, 9, 7),
        outcomes == Map(9 -> Outcome.Projected(Right(1)))
      )
    },
    test("answers Projected when the first projection is refused and the re-read one lands") {
      for
        s        <- store(projectFaults = List(Fault.Refuse))
        outcomes <- projected(s).call("k", NonEmptyChunk(5))
        reads    <- s.readCalls.get
        view     <- s.view("k")
      yield assertTrue(reads == 2, view == Vector(5), outcomes == Map(5 -> Outcome.Projected(Right(0))))
    },
    test("a projection of a stale record is refused, so it cannot clear the marker of a later commit") {
      for
        s     <- store(seed = Map("k" -> (Record(Vector(5), 0), Vector.empty)))
        stale <- s.read("k")
        _     <- fold(s).call("k", NonEmptyChunk(7)) // commits, does not project
        exit  <- projected(s).project("k", stale).exit
        still <- s.read("k")
      yield assertTrue(exit.isFailure, still == Record(Vector(5, 7), 0))
    },
    test("serves Ferry.perKey as its call and extract") {
      for
        s   <- store()
        cfg <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 10, maxInFlightBatches = 2, maxQueueing = 100)).orDie
        write = projected(s)
        ferry   <- Ferry.perKey(cfg, (_: Int) => "k")(write.call)(write.outcome)
        results <- ZIO.foreachPar(1 to 20)(ferry(_))
        record  <- s.read("k")
        view    <- s.view("k")
      yield assertTrue(
        record.log.sorted == (1 to 20).toVector,
        view == record.log,
        record.projectedUpTo == 20,
        results.forall(_.isProjected)
      )
    }
  )
