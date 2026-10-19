package ferry.examples

import ferry.{Ferry, FerryError, FoldWrite, Order, PerKeyConfig}
import zio.*

/**
 * Clients append entries to per-entity logs in a store with conditional writes. Each entity's writes must be applied in
 * order, and a round trip to the store costs about the same whether it carries one entry or twenty, so a busy entity is
 * limited by round trips, not by entries.
 *
 * `Ferry.perKey` makes this process the only writer of each entity: the entries of an entity that arrive while its
 * previous write is in flight travel together in the next one, and different entities are written in parallel. Entries
 * carry an effective day, and those that wait together are written in effective-day order. The write itself is fenced
 * and idempotent (see `docs/decisions/0005-fenced-single-writer.md`), so retrying it is safe.
 *
 * Run with: `examples/runMain ferry.examples.OrderedWrites`
 */
object OrderedWrites extends ZIOAppDefault:

  final case class Entry(id: String, entity: String, amount: Long, effectiveDay: Long)

  enum StoreError:
    case Throttled, Conflict

  /** Stands in for the store: one log per entity, appends conditional on the log's length, throttling now and then. */
  final class Store(logs: Ref[Map[String, Vector[String]]], requests: Ref[Int]):

    def read(entity: String): UIO[Vector[String]] =
      ZIO.sleep(2.millis) *> logs.get.map(_.getOrElse(entity, Vector.empty))

    def append(entity: String, expectedLength: Int, ids: Chunk[String]): IO[StoreError, Unit] =
      ZIO.sleep(5.millis) *> requests.updateAndGet(_ + 1).flatMap { n =>
        if n % 7 == 0 then ZIO.fail(StoreError.Throttled)
        else
          logs.modify { all =>
            val log = all.getOrElse(entity, Vector.empty)
            if log.size != expectedLength then Left(StoreError.Conflict) -> all
            else Right(())                                               -> all.updated(entity, log ++ ids)
          }.absolve
      }

  // --8<-- [start:step]
  /**
   * Applies one entry to an entity's log as read: an invalid entry is rejected without failing the others, and an entry
   * already in the log (a retried write may have landed) is answered with its position instead of being appended again.
   */
  def step(log: Vector[String], entry: Entry): (Vector[String], Either[String, Int]) =
    if entry.amount == 0 then log -> Left("amount must not be zero")
    else if log.contains(entry.id) then log -> Right(log.indexOf(entry.id))
    else (log :+ entry.id)                  -> Right(log.size)
  // --8<-- [end:step]

  // --8<-- [start:fold]
  /**
   * One round trip per batch: read the log, apply the entries, and append what is new only if the log has not changed
   * since it was read. Only a store error fails, and retries, the whole batch.
   */
  def write(store: Store): FoldWrite[Any, StoreError, String, Entry, String, Vector[String], Either[String, Int]] =
    FoldWrite(
      id = _.id,
      read = store.read,
      step = step,
      write = (entity, before, after) => store.append(entity, before.size, Chunk.fromIterable(after.drop(before.size))),
      retry = Schedule.exponential(10.millis).jittered && Schedule.recurs(5)
    )
  // --8<-- [end:fold]

  val run =
    ZIO.scoped {
      for
        logs     <- Ref.make(Map.empty[String, Vector[String]])
        requests <- Ref.make(0)
        store = Store(logs, requests)
        // --8<-- [start:wiring]
        config <- ZIO.fromEither(
          // No delay: an entity's entries wait only while its previous write is in flight, and a quiet entity's entry is
          // written at once. A small positive delay would let more out-of-order entries meet and be sorted, at that
          // much latency. The weight limit keeps a write under the store's request size limit, in (estimated) bytes.
          PerKeyConfig.make(maxBatchSize = 25, maxInFlightBatches = 8, maxQueueing = 1000, maxBatchWeight = 2000)
        )
        fold = write(store)
        writer <- Ferry.perKey(
          config,
          (_: Entry).entity,
          (e: Entry) => 40L + e.id.length,
          Order.by((_: Entry).effectiveDay)
        )(fold.call)(fold.outcome)
        // --8<-- [end:wiring]
        entries = Chunk.tabulate(300) { n =>
          val entity = if n % 5 == 0 then s"quiet-${n % 3}" else "busy" // most traffic goes to one entity
          val day    = 20000L + n / 10 - (if n % 11 == 0 then 3 else 0) // every eleventh entry is back-dated
          Entry(s"entry-$n", entity, amount = if n == 42 then 0 else n.toLong + 1, effectiveDay = day)
        }
        outcomes <- ZIO.foreachPar(entries)(entry => writer(entry).either)
        rejected = outcomes.collect { case Right(Left(reason)) => reason }
        failed   = outcomes.collect { case Left(error) => describe(error) }
        written <- logs.get
        made    <- requests.get
        _       <- Console.printLine(s"${entries.size} entries, ${rejected.size} rejected, ${failed.size} failed")
        _       <- ZIO.foreachDiscard(written.toList.sortBy(_._1)) { (entity, log) =>
          Console.printLine(
            s"$entity: ${log.size} entries applied, each exactly once: ${log.distinct.size == log.size}"
          )
        }
        _ <- Console.printLine(s"$made write requests to the store, throttled ones included")
        twice = written.collect { case (entity, log) if log.distinct != log => entity }
        _ <- ZIO.fail(s"entries applied more than once: ${twice.mkString(", ")}").when(twice.nonEmpty)
      yield ()
    }

  private def describe(error: FerryError[StoreError]): String =
    error.fold("rejected: too many entries waiting", cause => s"store failed: $cause")
