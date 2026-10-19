package ferry.examples

import ferry.{Ferry, FerryError, FoldWrite, PerKeyConfig, ProjectedWrite}
import ferry.ProjectedWrite.Outcome
import zio.*

/**
 * Clients append entries to per-entity logs, and each log has a read-side view: the projection of the log that queries
 * read. The store writes in two steps: a commit appends to the log, fenced on the log as read, and a projection brings
 * the view up to date, fenced on the record being as committed. Each record carries a marker, how much of the log is
 * projected, so a projection that did not happen is visible in the store and any process can finish it.
 *
 * `ProjectedWrite` is the batch call: it commits a key's batch, then projects it. A caller whose projection failed gets
 * `Committed`: the entry is durable, the view will catch up. A sweeper finishes such projections.
 *
 * Run with: `examples/runMain ferry.examples.ProjectedWrites`
 */
object ProjectedWrites extends ZIOAppDefault:

  final case class Entry(id: String, entity: String, amount: Long)

  /** An entity's record: its log, and how much of the log the view holds. */
  final case class Record(log: Vector[String], projectedUpTo: Int)

  enum StoreError:
    case Throttled, Conflict

  /**
   * Stands in for the store: one record and one view per entity. It throttles one commit in seven, and its view goes
   * down for a while: projections are refused from the 20th request to the 80th.
   */
  final class Store(state: Ref[Map[String, (Record, Vector[String])]], requests: Ref[Int]):

    private val empty = Record(Vector.empty, 0)

    def read(entity: String): UIO[Record] =
      ZIO.sleep(2.millis) *> state.get.map(_.get(entity).fold(empty)(_._1))

    /** Appends the new entries, if the log is still the one that was read. Leaves the marker as stored. */
    def commit(entity: String, before: Record, after: Record): IO[StoreError, Unit] =
      ZIO.sleep(5.millis) *> requests.updateAndGet(_ + 1).flatMap { n =>
        if n % 7 == 0 then ZIO.fail(StoreError.Throttled)
        else
          state.modify { all =>
            val (record, view) = all.getOrElse(entity, (empty, Vector.empty[String]))
            if record.log != before.log then Left(StoreError.Conflict) -> all
            else Right(()) -> all.updated(entity, (record.copy(log = after.log), view))
          }.absolve
      }

    /** Brings the view up to `record`, if the store still holds `record`. */
    def project(entity: String, record: Record): IO[StoreError, Unit] =
      ZIO.sleep(3.millis) *> requests.updateAndGet(_ + 1).flatMap { n =>
        if n >= 20 && n < 80 then ZIO.fail(StoreError.Throttled)
        else
          state.modify { all =>
            val (stored, view) = all.getOrElse(entity, (empty, Vector.empty[String]))
            if stored != record then Left(StoreError.Conflict) -> all
            else
              val projected =
                (record.copy(projectedUpTo = record.log.size), view ++ record.log.drop(record.projectedUpTo))
              Right(()) -> all.updated(entity, projected)
          }.absolve
      }

    /** The store's own query for records whose projection is pending, as an index would answer it. */
    def pendingEntities: UIO[List[String]] =
      state.get.map(_.collect { case (entity, (record, _)) if pending(record) => entity }.toList)

    def snapshot: UIO[Map[String, (Record, Vector[String])]] = state.get

  def pending(record: Record): Boolean = record.projectedUpTo < record.log.size

  /** An entry already in the log is answered with its position; an invalid one is rejected on its own. */
  def step(record: Record, entry: Entry): (Record, Either[String, Int]) =
    if entry.amount == 0 then record -> Left("amount must not be zero")
    else if record.log.contains(entry.id) then record -> Right(record.log.indexOf(entry.id))
    else record.copy(log = record.log :+ entry.id)    -> Right(record.log.size)

  // --8<-- [start:projected]
  /**
   * The commit is a `FoldWrite`, exactly as for a one-write store; `ProjectedWrite` adds the projection, the marker
   * test and the outcome that says which of the two writes an entry has been through.
   */
  def write(store: Store): ProjectedWrite[Any, StoreError, String, Entry, String, Record, Either[String, Int]] =
    ProjectedWrite(
      fold = FoldWrite(
        id = _.id,
        read = store.read,
        step = step,
        write = store.commit,
        retry = Schedule.exponential(10.millis).jittered && Schedule.recurs(5)
      ),
      pending = pending,
      project = store.project
    )
  // --8<-- [end:projected]

  // --8<-- [start:sweeper]
  /**
   * Finishes every pending projection, every 50 ms, for as long as the scope is open. A failure, even a defect, is
   * logged and waits for the next run: a sweeper that stops silently would leave views behind for good.
   */
  def sweeper(
    store: Store,
    write: ProjectedWrite[Any, StoreError, String, Entry, String, Record, ?]
  ): URIO[Scope, Unit] =
    store.pendingEntities
      .flatMap(
        ZIO.foreachDiscard(_)(entity =>
          write.resume(entity).catchAllCause(cause => ZIO.logWarningCause(s"sweeping $entity failed", cause))
        )
      )
      .repeat(Schedule.fixed(50.millis))
      .forkScoped
      .unit
  // --8<-- [end:sweeper]

  // --8<-- [start:answer]
  /** What a client is told. `Committed` is a success: the entry is durable, only its view is late. */
  def describe(result: Either[FerryError[StoreError], Outcome[Either[String, Int]]]): String = result match
    case Right(Outcome.Projected(Right(position))) => s"stored at $position"
    case Right(Outcome.Committed(Right(position))) => s"stored at $position, view pending"
    case Right(Outcome.Projected(Left(reason)))    => s"rejected: $reason"
    case Right(Outcome.Committed(Left(reason)))    => s"rejected: $reason"
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
        answers   = results.map(describe)
        committed = results.count(_.exists(!_.isProjected))
        // Wait for the sweeper to finish what the writers could not.
        _        <- (ZIO.sleep(10.millis) *> store.pendingEntities).repeatUntil(_.isEmpty)
        snapshot <- store.snapshot
        _        <- Console.printLine(
          s"${entries.size} entries: ${answers.count(_.startsWith("stored"))} stored, " +
            s"${answers.count(_.startsWith("rejected"))} rejected, ${answers.count(_.endsWith("retry"))} failed"
        )
        _ <- Console.printLine(s"$committed answered with their view pending, finished by the sweeper")
        _ <- ZIO.foreachDiscard(snapshot.toList.sortBy(_._1)) { case (entity, (record, view)) =>
          Console.printLine(
            s"$entity: ${record.log.size} entries, each once: ${record.log.distinct.size == record.log.size}; " +
              s"view equals log: ${view == record.log}"
          )
        }
      yield ()
    }
