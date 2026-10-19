package ferry.examples

import ferry.{Ferry, FerryError, FoldWrite, PerKeyConfig, ProjectedWrite}
import ferry.ProjectedWrite.{Outcome, Projection}
import zio.*

/**
 * Clients append entries to per-entity logs, and each log's read-side view is one row per entry, each row its own
 * conditional write. A commit appends to the log, fenced on the log as read, and sets the record's flag; the projection
 * writes each missing row, then clears the flag once every row is in, fenced on the record being as committed. One
 * throttled row leaves the record partially projected: the other entries of the batch are in the view.
 *
 * `ProjectedWrite` answers each input on its own: `Projected` if its row is in the view, `Committed` with why if not. A
 * sweeper finishes the rows that are missing, and logs what each run left.
 *
 * Run with: `examples/runMain ferry.examples.ProjectedRows`
 */
object ProjectedRows extends ZIOAppDefault:

  final case class Entry(id: String, entity: String, amount: Long)

  /** An entity's record: its log, and whether a projection is pending. */
  final case class Record(log: Vector[String], flag: Boolean)

  enum StoreError:
    case Throttled, Unavailable, Conflict

  /**
   * Stands in for the store: one record and one view of rows per entity. It throttles one row write in nine, and its
   * row writes go down for a while: they are refused from the 100th request to the 160th.
   */
  final class Store(state: Ref[Map[String, (Record, Vector[String])]], requests: Ref[Int]):

    private val empty = Record(Vector.empty, flag = false)

    def read(entity: String): UIO[Record] =
      ZIO.sleep(2.millis) *> state.get.map(_.get(entity).fold(empty)(_._1))

    /** Writes the record, if its log is still the one that was read. */
    def commit(entity: String, before: Record, after: Record): IO[StoreError, Unit] =
      ZIO.sleep(5.millis) *> state.modify { all =>
        val (record, rows) = all.getOrElse(entity, (empty, Vector.empty[String]))
        if record.log != before.log then Left(StoreError.Conflict) -> all
        else Right(())                                             -> all.updated(entity, (after, rows))
      }.absolve

    // --8<-- [start:rows]
    /** Writes each missing row, idempotently; clears the flag only when every row is present, fenced on the record. */
    def projectRows(entity: String, record: Record): IO[StoreError, Projection[String, StoreError]] =
      for
        present <- rows(entity)
        results <- ZIO.foreach(record.log.filterNot(present.contains))(id =>
          writeRowIfAbsent(entity, id).either.map(id -> _)
        )
        pending = results.collect { case (id, Left(e)) => id -> e }.toMap
        _ <- clearFlagIfStillHolds(entity, record).when(pending.isEmpty)
      yield Projection(pending)
    // --8<-- [end:rows]

    private def rows(entity: String): UIO[Vector[String]] =
      ZIO.sleep(2.millis) *> state.get.map(_.get(entity).fold(Vector.empty)(_._2))

    /** A row already in the view is present: not written again, and not an error. */
    private def writeRowIfAbsent(entity: String, id: String): IO[StoreError, Unit] =
      ZIO.sleep(1.millis) *> requests.updateAndGet(_ + 1).flatMap { n =>
        if n >= 100 && n < 160 then ZIO.fail(StoreError.Unavailable)
        else if n % 9 == 0 then ZIO.fail(StoreError.Throttled)
        else
          state.update { all =>
            val (record, rows) = all.getOrElse(entity, (empty, Vector.empty[String]))
            if rows.contains(id) then all else all.updated(entity, (record, rows :+ id))
          }
      }

    /** Clears the flag, if the store still holds `record`: a clear for an older record must not clear a newer flag. */
    private def clearFlagIfStillHolds(entity: String, record: Record): IO[StoreError, Unit] =
      ZIO.sleep(2.millis) *> state.modify { all =>
        val (stored, rows) = all.getOrElse(entity, (empty, Vector.empty[String]))
        if stored != record then Left(StoreError.Conflict) -> all
        else Right(())                                     -> all.updated(entity, (stored.copy(flag = false), rows))
      }.absolve

    /** The store's own query for records whose projection is pending, as an index on the flag would answer it. */
    def pendingEntities: UIO[List[String]] =
      state.get.map(_.collect { case (entity, (record, _)) if record.flag => entity }.toList)

    def snapshot: UIO[Map[String, (Record, Vector[String])]] = state.get

  /** An entry already in the log is answered with its position; an invalid one is rejected on its own. */
  def step(record: Record, entry: Entry): (Record, Either[String, Int]) =
    if entry.amount == 0 then record -> Left("amount must not be zero")
    else if record.log.contains(entry.id) then record -> Right(record.log.indexOf(entry.id))
    else Record(record.log :+ entry.id, flag = true)  -> Right(record.log.size)

  // --8<-- [start:projected]
  /** The commit is a `FoldWrite`; `project` writes the rows and answers what it left pending. */
  def write(store: Store): ProjectedWrite[Any, StoreError, String, Entry, String, Record, Either[String, Int]] =
    ProjectedWrite(
      fold = FoldWrite(
        id = _.id,
        read = store.read,
        step = step,
        write = store.commit,
        retry = Schedule.exponential(10.millis).jittered && Schedule.recurs(5)
      ),
      pending = _.flag,
      project = store.projectRows
    )
  // --8<-- [end:projected]

  // --8<-- [start:sweeper]
  /**
   * Finishes every pending projection, every 50 ms, for as long as the scope is open, and logs what each run left
   * pending and why. A failure, even a defect, is logged and waits for the next run.
   */
  def sweeper(
    store: Store,
    write: ProjectedWrite[Any, StoreError, String, Entry, String, Record, ?]
  ): URIO[Scope, Unit] =
    store.pendingEntities
      .flatMap(
        ZIO.foreachDiscard(_)(entity =>
          write
            .resume(entity)
            .foldCauseZIO(
              cause => ZIO.logWarningCause(s"sweeping $entity failed", cause),
              left => ZIO.logInfo(s"$entity: still pending ${left.pending}").unless(left.isComplete)
            )
        )
      )
      .repeat(Schedule.fixed(50.millis))
      .forkScoped
      .unit
  // --8<-- [end:sweeper]

  // --8<-- [start:answer]
  /** What a client is told. `Committed` is a success: the entry is durable, only its row is late, and here is why. */
  def describe(result: Either[FerryError[StoreError], Outcome[StoreError, Either[String, Int]]]): String = result match
    case Right(Outcome.Projected(Right(position)))        => s"stored at $position"
    case Right(Outcome.Committed(Right(position), cause)) => s"stored at $position, row pending ($cause)"
    case Right(Outcome.Projected(Left(reason)))           => s"rejected: $reason"
    case Right(Outcome.Committed(Left(reason), _))        => s"rejected: $reason"
    case Left(error) => error.fold("too busy, retry later", e => s"store failed ($e), retry")
  // --8<-- [end:answer]

  val run =
    ZIO.scoped {
      for
        state    <- Ref.make(Map.empty[String, (Record, Vector[String])])
        requests <- Ref.make(0)
        store     = Store(state, requests)
        projected = write(store)
        config <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 25, maxInFlightBatches = 8, maxQueueing = 1000))
        writer <- Ferry.perKey(config, (_: Entry).entity)(projected.call)(projected.outcome)
        _      <- sweeper(store, projected)
        entries = Chunk.tabulate(300) { n =>
          val entity = if n % 5 == 0 then s"quiet-${n % 3}" else "busy" // most traffic goes to one entity
          Entry(s"entry-$n", entity, amount = if n == 42 then 0 else n.toLong + 1)
        }
        results <- ZIO.foreachPar(entries)(entry => writer(entry).either)
        causes = results.collect { case Right(Outcome.Committed(Right(_), cause)) => cause }.groupBy(identity)
        // Wait for the sweeper to finish what the writers could not.
        _ <- (ZIO.sleep(10.millis) *> store.pendingEntities)
          .repeatUntil(_.isEmpty)
          .timeout(10.seconds)
          .someOrElseZIO(store.pendingEntities.flatMap(left => ZIO.fail(s"still pending after 10 s: $left")))
        snapshot <- store.snapshot
        _        <- Console.printLine(
          s"${entries.size} entries: ${results.count(_.exists(_.value.isRight))} stored, " +
            s"${results.count(_.exists(_.value.isLeft))} rejected, ${results.count(_.isLeft)} failed"
        )
        _ <- Console.printLine(
          s"${causes.values.map(_.size).sum} answered with their row pending, finished by the sweeper: " +
            causes.toList.sortBy(_._1.ordinal).map((cause, all) => s"${all.size} $cause").mkString(", ")
        )
        _ <- Console.printLine(s"entry-42 was told: ${describe(results(42))}")
        _ <- ZIO.foreachDiscard(snapshot.toList.sortBy(_._1)) { case (entity, (record, rows)) =>
          Console.printLine(
            s"$entity: ${record.log.size} entries, ${rows.size} rows, each once: ${rows.distinct.size == rows.size}; " +
              s"rows equal log: ${rows.sorted == record.log.sorted}"
          )
        }
        broken = snapshot.collect {
          case (entity, (record, rows)) if rows.distinct != rows || rows.sorted != record.log.sorted => entity
        }
        _ <- ZIO.fail(s"views whose rows do not equal their logs: ${broken.mkString(", ")}").when(broken.nonEmpty)
      yield ()
    }
