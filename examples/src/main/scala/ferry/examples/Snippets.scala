package ferry.examples

import ferry.*
import zio.*

/**
 * The code shown on the documentation site. It is compiled with the examples so that the docs cannot drift from the
 * API; it is not meant to be run. Sections between `--8<--` markers are included by `mkdocs.yml`'s snippets extension.
 */
object Snippets:

  final case class Price(amount: BigDecimal)

  trait PricingApi:
    def fetch(countries: NonEmptyChunk[String]): Task[Map[String, Price]]

  // --8<-- [start:first-ferry]
  val program: ZIO[Scope & PricingApi, Throwable, Option[Price]] =
    for
      api    <- ZIO.service[PricingApi]
      config <- ZIO.fromEither(
        FerryConfig.make(
          maxBatchSize = 5,          // send when 5 inputs are waiting
          maxBatchDelay = 20.millis, // or 20 ms after the oldest arrived
          maxInFlightBatches = 2,    // at most 2 batch calls at once
          maxQueueing = 50           // beyond 50 waiting, reject at once
        )
      )
      prices <- Ferry.keyed(config)((countries: NonEmptyChunk[String]) => api.fetch(countries))
      price  <- prices("NL").mapError(_.toException)
    yield price
  // --8<-- [end:first-ferry]

  final case class Query(text: String)
  final case class Hit(query: Query, score: Int)
  final case class SearchError(message: String)

  trait SearchApi:
    def search(queries: NonEmptyChunk[Query]): IO[SearchError, Chunk[Hit]]

  // --8<-- [start:make]
  def searches(api: SearchApi): ZIO[Scope, InvalidConfig, Ferry[Query, SearchError, Option[Hit]]] =
    for
      config <- ZIO.fromEither(
        FerryConfig.make(maxBatchSize = 50, maxBatchDelay = 10.millis, maxInFlightBatches = 4, maxQueueing = 500)
      )
      // The batch call gets every waiting query; each caller picks its own hit out of the batch's answer.
      ferry <- Ferry.make(config)(api.search)((query, hits) => hits.find(_.query == query))
    yield ferry
  // --8<-- [end:make]

  // --8<-- [start:outcomes]
  def describe(ferry: Ferry[Query, SearchError, Option[Hit]], query: Query): UIO[String] =
    ferry(query).fold(
      {
        case FerryError.Rejected      => "too many queries waiting: back off, then retry"
        case FerryError.Failed(error) => s"the batch carrying this query failed: ${error.message}"
      },
      _.fold("no hit")(hit => s"score ${hit.score}")
    )
  // --8<-- [end:outcomes]

  final case class Event(id: String, entity: String, at: Long, payload: String)

  enum StoreError:
    case Unavailable, Conflict

  trait EventStore:
    /** Appends events to an entity's log: one outcome per event id, an error only if nothing was appended. */
    def append(entity: String, events: NonEmptyChunk[Event]): IO[StoreError, Map[String, Either[String, Long]]]

  // --8<-- [start:per-key]
  def writer(store: EventStore): ZIO[Scope, InvalidConfig, Ferry[Event, StoreError, Either[String, Long]]] =
    for
      config <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 25, maxInFlightBatches = 8, maxQueueing = 1000))
      ferry  <- Ferry.perKey(config, (e: Event) => e.entity)(store.append)((event, outcomes) => outcomes(event.id))
    yield ferry
  // --8<-- [end:per-key]

  // --8<-- [start:per-key-tuned]
  def tunedWriter(store: EventStore): ZIO[Scope, InvalidConfig, Ferry[Event, StoreError, Either[String, Long]]] =
    for
      config <- ZIO.fromEither(
        PerKeyConfig.make(
          maxBatchSize = 100,
          maxBatchWeight = 256L * 1024, // the store's request size limit, in bytes
          maxInFlightBatches = 16,
          maxQueueing = 10000,
          maxQueuedWeight = 64L * 1024 * 1024 // bounds the memory held by waiting events
        )
      )
      ferry <- Ferry.perKey(
        config,
        key = (e: Event) => e.entity,
        weigh = (e: Event) => e.payload.length.toLong,
        order = Order.by((e: Event) => e.at)
      )(store.append)((event, outcomes) => outcomes(event.id))
    yield ferry
  // --8<-- [end:per-key-tuned]

  // --8<-- [start:custom-order]
  final case class Change(entity: String, version: Long)

  /** `Order` has a single method, so a lambda implements it. */
  val byVersion: Order[Change] = change => change.version
  // --8<-- [end:custom-order]

  // --8<-- [start:metrics]
  def report(ferry: Ferry[Nothing, Any, Any]): URIO[Scope, Unit] =
    ferry.metrics
      .flatMap(m => ZIO.logInfo(s"waiting=${m.queued} inFlight=${m.inFlightBatches}"))
      .repeat(Schedule.fixed(10.seconds))
      .forkScoped
      .unit
  // --8<-- [end:metrics]

  // --8<-- [start:period]
  final case class Ledger(period: Int, totals: Map[Int, Long])

  enum Posting:
    case Add(amount: Long)
    case Close

  /** Closing a period moves every later posting of the batch, and of later batches, to the next one. */
  def post(ledger: Ledger, posting: Posting): (Ledger, Int) = posting match
    case Posting.Add(amount) =>
      val total = ledger.totals.getOrElse(ledger.period, 0L) + amount
      ledger.copy(totals = ledger.totals.updated(ledger.period, total)) -> ledger.period
    case Posting.Close => ledger.copy(period = ledger.period + 1) -> (ledger.period + 1)
  // --8<-- [end:period]

  // --8<-- [start:dependent]
  /** An account's balance, and the id of the entry that blocked it, if one did. */
  final case class Account(balance: Long, blockedBy: Option[String])

  final case class Debit(id: String, amount: Long)

  enum Rejection:
    case InsufficientFunds
    case NotAttempted(blockedBy: String)

  /**
   * An account's entries depend on the ones before them: once one is rejected, the ones after it are not attempted, in
   * this batch or in a later one, until something unblocks the account. The block is part of the state, so it is
   * written with the batch and survives the batch boundary, a retry and the process.
   */
  def debit(account: Account, entry: Debit): (Account, Either[Rejection, Long]) =
    account.blockedBy match
      case Some(id) if id != entry.id =>
        account -> Left(Rejection.NotAttempted(id)) // the blocking entry, replayed, is judged again
      case _ if entry.amount > account.balance =>
        account.copy(blockedBy = Some(entry.id)) -> Left(Rejection.InsufficientFunds)
      case _ =>
        val balance = account.balance - entry.amount
        account.copy(balance = balance) -> Right(balance)
  // --8<-- [end:dependent]

  // --8<-- [start:shards]
  /**
   * Spreads keys over `n` independent ferries. Every per-key guarantee still holds: a key always maps to the same one.
   */
  def sharded(
    n: Int,
    store: EventStore
  ): ZIO[Scope, InvalidConfig, Event => IO[FerryError[StoreError], Either[String, Long]]] =
    ZIO.foreach(Chunk.range(0, n))(_ => writer(store)).map { shards => event =>
      shards(Math.floorMod(event.entity.hashCode, n))(event)
    }
  // --8<-- [end:shards]
