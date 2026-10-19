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
    },
    perEntry
  )

  /** A key's record in a store whose view is one row per entry: its log, and whether a projection is pending. */
  final private case class Entries(log: Vector[Int], flag: Boolean)

  private val none = Entries(Vector.empty, flag = false)

  private enum RowFault:
    /** The row write applies. */
    case Pass

    /** The row write is refused and applies nothing. */
    case Throttle

    /** The row write applies, and its response is lost. */
    case Lost

    /** A sweeper writes the same row just before this write applies. */
    case Raced

  /**
   * One record and one view per key, the view a row per entry in landing order. A commit writes the record only if its
   * log is still the log the writer read; `step` sets the flag whenever it appends. `projectRows` writes each row the
   * view is missing, then clears the flag when none is left pending, fenced on the record. The switches turn off, one
   * at a time, the three rules the store must keep.
   */
  final private case class RowStore(
    state: Ref[Map[String, (Entries, Vector[Int])]],
    readCalls: Ref[Int],
    commitCalls: Ref[Int],
    projectCalls: Ref[Int],
    rowWrites: Ref[Int],
    viewFaults: Ref[Int],
    rowFaults: Ref[List[RowFault]],
    conditionalRows: Boolean,
    clearWhenComplete: Boolean,
    fencedClear: Boolean
  ):

    def read(key: String): IO[String, Entries] = state.get.map(_.get(key).fold(none)(_._1))

    def readCounted(key: String): IO[String, Entries] = readCalls.update(_ + 1) *> read(key)

    def rows(key: String): UIO[Vector[Int]] = state.get.map(_.get(key).fold(Vector.empty)(_._2))

    def commit(key: String, before: Entries, after: Entries): IO[String, Unit] =
      commitCalls.update(_ + 1) *> state.modify { m =>
        val (record, rows) = m.getOrElse(key, (none, Vector.empty[Int]))
        if record.log == before.log then Right(()) -> m.updated(key, (after, rows))
        else Left("conflict")                      -> m
      }.absolve

    /** Writes each missing row, idempotently; clears the flag only when every row is present, fenced on the record. */
    def projectRows(key: String, record: Entries): IO[String, ProjectedWrite.Projection[Int, String]] =
      for
        _       <- projectCalls.update(_ + 1)
        present <- rowsOf(key)
        results <- ZIO.foreach(record.log.filterNot(present.contains))(n => writeRow(key, n).either.map(n -> _))
        pending = results.collect { case (n, Left(e)) => n -> e }.toMap
        _ <- clear(key, record).when(pending.isEmpty || !clearWhenComplete)
      yield ProjectedWrite.Projection(pending)

    private def rowsOf(key: String): IO[String, Vector[Int]] =
      viewFaults
        .modify(n => (n > 0) -> (n - 1).max(0))
        .flatMap(refuse => if refuse then ZIO.fail("refused") else rows(key))

    private def writeRow(key: String, n: Int): IO[String, Unit] =
      rowWrites.update(_ + 1) *> rowFaults.modify(f => f.headOption.getOrElse(RowFault.Pass) -> f.drop(1)).flatMap {
        case RowFault.Pass     => applyRow(key, n)
        case RowFault.Throttle => ZIO.fail("throttled")
        case RowFault.Lost     => applyRow(key, n) *> ZIO.fail("timeout")
        case RowFault.Raced    => applyRow(key, n) *> applyRow(key, n)
      }

    private def applyRow(key: String, n: Int): UIO[Unit] =
      state.update { m =>
        val (record, rows) = m.getOrElse(key, (none, Vector.empty[Int]))
        if conditionalRows && rows.contains(n) then m else m.updated(key, (record, rows :+ n))
      }

    private def clear(key: String, record: Entries): IO[String, Unit] =
      state.modify { m =>
        val (stored, rows) = m.getOrElse(key, (none, Vector.empty[Int]))
        if !fencedClear || stored == record then Right(()) -> m.updated(key, (stored.copy(flag = false), rows))
        else Left("stale")                                 -> m
      }.absolve

  private def rowStore(
    rowFaults: List[RowFault] = Nil,
    viewFaults: Int = 0,
    seed: Map[String, (Entries, Vector[Int])] = Map.empty,
    conditionalRows: Boolean = true,
    clearWhenComplete: Boolean = true,
    fencedClear: Boolean = true
  ): UIO[RowStore] =
    for
      state        <- Ref.make(seed)
      readCalls    <- Ref.make(0)
      commitCalls  <- Ref.make(0)
      projectCalls <- Ref.make(0)
      rowWrites    <- Ref.make(0)
      vf           <- Ref.make(viewFaults)
      rf           <- Ref.make(rowFaults)
    yield RowStore(
      state,
      readCalls,
      commitCalls,
      projectCalls,
      rowWrites,
      vf,
      rf,
      conditionalRows,
      clearWhenComplete,
      fencedClear
    )

  private def appendStep(r: Entries, n: Int): (Entries, Either[String, Int]) =
    if n == 0 then r -> Left("zero")
    else if r.log.contains(n) then r      -> Right(r.log.indexOf(n))
    else Entries(r.log :+ n, flag = true) -> Right(r.log.size)

  private def rowFold(s: RowStore): FoldWrite[Any, String, String, Int, Int, Entries, Either[String, Int]] =
    FoldWrite((n: Int) => n, s.readCounted, appendStep, s.commit, Schedule.recurs(3))

  private def perEntryWrite(s: RowStore): ProjectedWrite[Any, String, String, Int, Int, Entries, Either[String, Int]] =
    ProjectedWrite(rowFold(s), (_: Entries).flag, s.projectRows)

  private val complete = ProjectedWrite.Projection.complete[Int, String]

  private def perEntry = suite("one row per entry")(
    test("writes a row per entry, clears the flag, and answers every input Projected") {
      for
        s        <- rowStore()
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(5, 0, 7))
        record   <- s.read("k")
        rows     <- s.rows("k")
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
      yield assertTrue(
        commits == 1,
        projects == 1,
        rows == Vector(5, 7),
        record == Entries(Vector(5, 7), flag = false),
        outcomes == Map(
          5 -> Outcome.Projected(Right(0)),
          0 -> Outcome.Projected(Left("zero")),
          7 -> Outcome.Projected(Right(1))
        )
      )
    },
    test("answers a throttled row's input Committed with its cause and the others Projected, and resume finishes it") {
      for
        s <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Throttle))
        write = perEntryWrite(s)
        outcomes <- write.call("k", NonEmptyChunk(5, 7, 9))
        partial  <- s.rows("k")
        flagged  <- s.read("k")
        projects <- s.projectCalls.get
        reads    <- s.readCalls.get
        written  <- s.rowWrites.get
        left     <- write.resume("k")
        rows     <- s.rows("k")
        record   <- s.read("k")
        total    <- s.rowWrites.get
      yield assertTrue(
        outcomes == Map(
          5 -> Outcome.Projected(Right(0)),
          7 -> Outcome.Committed(Right(1), "throttled"),
          9 -> Outcome.Projected(Right(2))
        ),
        partial == Vector(5, 9),
        flagged.flag,
        projects == 1, // a verdict is final: no second projection inside the call
        reads == 1,    // and no re-read
        written == 3,
        left == complete,
        rows == Vector(5, 9, 7),
        !record.flag,
        total == 4 // resume wrote exactly the one missing row
      )
    },
    test("answers a row whose response was lost Committed, and resume finds it present and only clears") {
      for
        s <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Lost))
        write = perEntryWrite(s)
        outcomes <- write.call("k", NonEmptyChunk(5, 7))
        landed   <- s.rows("k")
        written  <- s.rowWrites.get
        left     <- write.resume("k")
        total    <- s.rowWrites.get
        record   <- s.read("k")
      yield assertTrue(
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Committed(Right(1), "timeout")),
        landed == Vector(5, 7),
        written == 2,
        total == 2,
        left == complete,
        !record.flag
      )
    },
    test("falls back to resume when project fails, and answers Projected when resume lands") {
      for
        s        <- rowStore(viewFaults = 1)
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(5, 7))
        reads    <- s.readCalls.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        reads == 2,
        rows == Vector(5, 7),
        !record.flag,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("answers every input Committed with the error resume ended on when project and resume both fail") {
      for
        s     <- rowStore(viewFaults = 4)
        first <- Ref.make(true)
        // The first attempt fails with its own error; resume's four attempts are refused.
        busyFirst = (key: String, r: Entries) =>
          first.getAndSet(false).flatMap(f => if f then ZIO.fail("busy") else s.projectRows(key, r))
        outcomes <- ProjectedWrite(rowFold(s), (_: Entries).flag, busyFirst).call("k", NonEmptyChunk(5, 0, 7))
        rows     <- s.rows("k")
        record   <- s.read("k")
        reads    <- s.readCalls.get
      yield assertTrue(
        reads == 5, // the commit's read, then resume's first attempt and its three retries
        outcomes == Map(
          5 -> Outcome.Committed(Right(0), "refused"),
          0 -> Outcome.Committed(Left("zero"), "refused"),
          7 -> Outcome.Committed(Right(1), "refused")
        ),
        rows == Vector.empty,
        record.flag
      )
    },
    test("answers from resume's verdict when project fails and resume leaves a row pending") {
      for
        s        <- rowStore(viewFaults = 1, rowFaults = List(RowFault.Pass, RowFault.Throttle))
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(5, 7))
        reads    <- s.readCalls.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        reads == 2,
        rows == Vector(5),
        record.flag,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Committed(Right(1), "throttled"))
      )
    },
    test("re-reads and finishes both commits when its clear is refused because another writer committed") {
      for
        s     <- rowStore()
        rival <- Ref.make(true)
        // The first projection finds that another writer committed 9 since this call committed 5.
        interleaved = (key: String, r: Entries) =>
          rival.getAndSet(false).flatMap(first => rowFold(s).call(key, NonEmptyChunk(9)).when(first)) *>
            s.projectRows(key, r)
        outcomes <- ProjectedWrite(rowFold(s), (_: Entries).flag, interleaved).call("k", NonEmptyChunk(5))
        projects <- s.projectCalls.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        projects == 2, // the first clear was refused as stale, the re-read projection finished
        rows == Vector(5, 9),
        record == Entries(Vector(5, 9), flag = false),
        outcomes == Map(5 -> Outcome.Projected(Right(0)))
      )
    },
    test("reports a row a sweeper wrote first as present, without a duplicate") {
      for
        s        <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Raced))
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(5, 7))
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        rows == Vector(5, 7),
        !record.flag,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("reports a row a sweeper's projection already wrote as present, and writes only the others") {
      for
        s <- rowStore(rowFaults = List(RowFault.Throttle, RowFault.Pass))
        write = perEntryWrite(s)
        // A sweeper projects first: its row 5 is throttled, its row 7 lands, so it leaves the flag set.
        sweptFirst = (key: String, r: Entries) => write.resume(key) *> s.projectRows(key, r)
        outcomes <- ProjectedWrite(rowFold(s), (_: Entries).flag, sweptFirst).call("k", NonEmptyChunk(5, 7))
        written  <- s.rowWrites.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        written == 3, // the sweeper's two, then only row 5
        rows == Vector(7, 5),
        !record.flag,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("projects a pending record for a batch of rejected inputs, which commits nothing") {
      for
        s        <- rowStore(seed = Map("k" -> (Entries(Vector(5), flag = true), Vector.empty)))
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(0))
        commits  <- s.commitCalls.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        commits == 0,
        rows == Vector(5),
        !record.flag,
        outcomes == Map(0 -> Outcome.Projected(Left("zero")))
      )
    },
    test("writes only the missing row when a replayed batch finds its record partially projected") {
      for
        s        <- rowStore(seed = Map("k" -> (Entries(Vector(5, 7), flag = true), Vector(5))))
        outcomes <- perEntryWrite(s).call("k", NonEmptyChunk(5, 7))
        commits  <- s.commitCalls.get
        projects <- s.projectCalls.get
        written  <- s.rowWrites.get
        rows     <- s.rows("k")
        record   <- s.read("k")
      yield assertTrue(
        commits == 0,
        projects == 1,
        written == 1,
        rows == Vector(5, 7),
        !record.flag,
        outcomes == Map(5 -> Outcome.Projected(Right(0)), 7 -> Outcome.Projected(Right(1)))
      )
    },
    test("refuses the clear of a stale record, though its rows land, so the later commit's entry is not lost") {
      for
        s <- rowStore(seed = Map("k" -> (Entries(Vector(5, 7), flag = true), Vector(5))))
        write = perEntryWrite(s)
        stale <- s.read("k")
        _     <- rowFold(s).call("k", NonEmptyChunk(9)) // commits 9, does not project
        exit  <- s.projectRows("k", stale).exit
        rows  <- s.rows("k")
        still <- s.read("k")
        left  <- write.resume("k")
        after <- s.rows("k")
        done  <- s.read("k")
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).contains("stale"),
        rows == Vector(5, 7),
        still == Entries(Vector(5, 7, 9), flag = true),
        left == complete,
        after == Vector(5, 7, 9),
        done == Entries(Vector(5, 7, 9), flag = false)
      )
    },
    test("resume answers what it left: nothing when it only clears, the throttled row's cause otherwise") {
      for
        full      <- rowStore(seed = Map("k" -> (Entries(Vector(5, 7), flag = true), Vector(5, 7))))
        cleared   <- perEntryWrite(full).resume("k")
        noWrites  <- full.rowWrites.get
        clean     <- full.read("k")
        throttled <- rowStore(
          rowFaults = List(RowFault.Throttle),
          seed = Map("k" -> (Entries(Vector(5, 7), flag = true), Vector(5)))
        )
        left    <- perEntryWrite(throttled).resume("k")
        flagged <- throttled.read("k")
        reads   <- throttled.readCalls.get
      yield assertTrue(
        cleared == complete,
        noWrites == 0,
        !clean.flag,
        left == ProjectedWrite.Projection(Map(7 -> "throttled")),
        flagged.flag,
        reads == 1 // a partial verdict is not a failure: resume does not retry it
      )
    },
    test("each of the store's three rules is needed") {
      for
        // Unconditional rows: the racing sweeper's row and this write's both land.
        raced <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Raced), conditionalRows = false)
        _     <- perEntryWrite(raced).call("k", NonEmptyChunk(5, 7))
        twice <- raced.rows("k")
        // A clear that does not wait for every row: the flag clears with 7 missing, and nothing ever writes it.
        early <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Throttle), clearWhenComplete = false)
        _     <- perEntryWrite(early).call("k", NonEmptyChunk(5, 7))
        _     <- perEntryWrite(early).resume("k")
        lost  <- early.rows("k")
        gone  <- early.read("k")
        late  <- early.rowWrites.get
        // A clear not fenced on the record: a projection of the record before 9 clears the flag 9's commit set.
        unfenced <- rowStore(seed = Map("k" -> (Entries(Vector(5), flag = true), Vector.empty)), fencedClear = false)
        stale    <- unfenced.read("k")
        _        <- rowFold(unfenced).call("k", NonEmptyChunk(9))
        _        <- unfenced.projectRows("k", stale)
        _        <- perEntryWrite(unfenced).resume("k")
        missing  <- unfenced.rows("k")
        cleared  <- unfenced.read("k")
      yield assertTrue(
        twice == Vector(5, 7, 7),
        lost == Vector(5),
        gone == Entries(Vector(5, 7), flag = false),
        late == 2, // resume found nothing pending and wrote nothing
        missing == Vector(5),
        cleared == Entries(Vector(5, 9), flag = false)
      )
    },
    test("serves Ferry.perKey with mixed outcomes, and resume finishes the one row left") {
      for
        s   <- rowStore(rowFaults = List(RowFault.Pass, RowFault.Pass, RowFault.Throttle))
        cfg <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 10, maxInFlightBatches = 2, maxQueueing = 100)).orDie
        write = perEntryWrite(s)
        ferry   <- Ferry.perKey(cfg, (_: Int) => "k")(write.call)(write.outcome)
        results <- ZIO.foreachPar(1 to 20)(ferry(_))
        _       <- write.resume("k")
        record  <- s.read("k")
        rows    <- s.rows("k")
      yield
        val committed = results.collect { case Outcome.Committed(_, cause) => cause }
        assertTrue(
          committed == Vector("throttled"),
          results.count(_.isProjected) == 19,
          rows.sorted == record.log.sorted,
          rows.distinct == rows,
          record.log.sorted == (1 to 20).toVector,
          !record.flag
        )
    },
    test("never writes a row twice when resumes race each other and a batch on a partially projected record") {
      for
        s <- rowStore(seed = Map("k" -> (Entries(Vector(5, 7, 9), flag = true), Vector(5))))
        write = perEntryWrite(s)
        results <- ZIO.foreachPar(1 to 8)(_ => write.resume("k").either) <&> write.call("k", NonEmptyChunk(11))
        rows    <- s.rows("k")
        record  <- s.read("k")
      yield
        val (resumed, outcomes) = results
        assertTrue(
          resumed.forall(_.isRight),
          rows.sorted == Vector(5, 7, 9, 11),
          rows.distinct == rows,
          record == Entries(Vector(5, 7, 9, 11), flag = false),
          outcomes == Map(11 -> Outcome.Projected(Right(3)))
        )
    }
  )
