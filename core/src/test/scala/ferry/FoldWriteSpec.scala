package ferry

import zio.*
import zio.test.*

object FoldWriteSpec extends ZIOSpecDefault:

  /** A store of one log per key, written only if the log is still the one the writer read. */
  final private case class Store(logs: Ref[Map[String, Vector[Int]]], writes: Ref[Int], faults: Ref[List[Fault]]):

    def read(key: String): IO[String, Vector[Int]] = logs.get.map(_.getOrElse(key, Vector.empty))

    def writeIf(key: String, before: Vector[Int], after: Vector[Int]): IO[String, Unit] =
      writes.update(_ + 1) *> faults.modify(f => f.headOption -> f.drop(1)).flatMap {
        case Some(Fault.Conflict) => // another writer got there first
          logs.update(m => m.updated(key, m.getOrElse(key, Vector.empty) :+ -1)) *> ZIO.fail("conflict")
        case Some(Fault.LostResponse) => apply(key, before, after) *> ZIO.fail("timeout")
        case None                     => apply(key, before, after)
      }

    private def apply(key: String, before: Vector[Int], after: Vector[Int]): IO[String, Unit] =
      logs.modify { m =>
        if m.getOrElse(key, Vector.empty) == before then Right(()) -> m.updated(key, after)
        else Left("conflict")                                      -> m
      }.absolve

  private enum Fault:
    case Conflict, LostResponse

  private def store(faults: Fault*) =
    (Ref.make(Map.empty[String, Vector[Int]]) <*> Ref.make(0) <*> Ref.make(faults.toList)).map(Store(_, _, _))

  /** Inputs are numbers; zero is invalid, and a number already in the log is answered with its position. */
  private def step(log: Vector[Int], n: Int): (Vector[Int], Either[String, Int]) =
    if n == 0 then log -> Left("zero")
    else if log.contains(n) then log -> Right(log.indexOf(n))
    else (log :+ n)                  -> Right(log.size)

  private def foldWrite(s: Store): FoldWrite[Any, String, String, Int, Int, Vector[Int], Either[String, Int]] =
    FoldWrite((n: Int) => n, s.read, step, s.writeIf, Schedule.recurs(3))

  /** A per-key ledger of totals by period; closing a period moves every later posting to the next one. */
  final private case class Ledger(period: Int, totals: Map[Int, Long])

  private enum Posting:
    case Add(id: String, amount: Long)
    case Close(id: String)

  private def postingId(posting: Posting): String = posting match
    case Posting.Add(id, _) => id
    case Posting.Close(id)  => id

  /** The outcome of a posting is the period it landed in. */
  private def post(ledger: Ledger, posting: Posting): (Ledger, Int) = posting match
    case Posting.Add(_, amount) =>
      val total = ledger.totals.getOrElse(ledger.period, 0L) + amount
      ledger.copy(totals = ledger.totals.updated(ledger.period, total)) -> ledger.period
    case Posting.Close(_) => ledger.copy(period = ledger.period + 1) -> (ledger.period + 1)

  /** An account whose entries depend on the ones before them: a rejected entry blocks the account. */
  final private case class Account(balance: Long, blockedBy: Option[String])

  final private case class Debit(id: String, amount: Long)

  private enum Rejection:
    case InsufficientFunds
    case NotAttempted(blockedBy: String)

  private def debit(account: Account, entry: Debit): (Account, Either[Rejection, Long]) =
    account.blockedBy match
      case Some(id) if id != entry.id          => account -> Left(Rejection.NotAttempted(id))
      case _ if entry.amount > account.balance =>
        account.copy(blockedBy = Some(entry.id)) -> Left(Rejection.InsufficientFunds)
      case _ =>
        val balance = account.balance - entry.amount
        account.copy(balance = balance) -> Right(balance)

  def spec = suite("FoldWrite")(
    test("applies a batch in order with one read and one write, and answers each input on its own") {
      for
        s        <- store()
        outcomes <- foldWrite(s).call("k", NonEmptyChunk(5, 0, 7))
        log      <- s.read("k")
        writes   <- s.writes.get
      yield assertTrue(
        log == Vector(5, 7),
        writes == 1,
        outcomes == Map(5 -> Right(0), 0 -> Left("zero"), 7 -> Right(1))
      )
    },
    test("rereads and reapplies after a conflict, on top of what the other writer wrote") {
      for
        s        <- store(Fault.Conflict)
        outcomes <- foldWrite(s).call("k", NonEmptyChunk(5, 7))
        log      <- s.read("k")
      yield assertTrue(log == Vector(-1, 5, 7), outcomes == Map(5 -> Right(1), 7 -> Right(2)))
    },
    test("never applies an input twice when a write landed but its response was lost") {
      for
        s        <- store(Fault.LostResponse)
        outcomes <- foldWrite(s).call("k", NonEmptyChunk(5, 7))
        log      <- s.read("k")
        writes   <- s.writes.get
      yield assertTrue(log == Vector(5, 7), writes == 1, outcomes == Map(5 -> Right(0), 7 -> Right(1)))
    },
    test("applies a repeated input once, and gives every copy the first one's outcome") {
      for
        s <- store()
        write = foldWrite(s)
        outcomes <- write.call("k", NonEmptyChunk(5, 5))
        log      <- s.read("k")
      yield assertTrue(log == Vector(5), write.outcome(5, outcomes) == Right(0))
    },
    test("skips the write when nothing changed") {
      for
        s        <- store()
        outcomes <- foldWrite(s).call("k", NonEmptyChunk(0))
        writes   <- s.writes.get
      yield assertTrue(writes == 0, outcomes == Map(0 -> Left("zero")))
    },
    test("fails with the store's error once retries are exhausted") {
      for
        s    <- store(Fault.Conflict, Fault.Conflict, Fault.Conflict, Fault.Conflict)
        exit <- foldWrite(s).call("k", NonEmptyChunk(5)).exit
      yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains("conflict"))
    },
    test("answers each input as of the state at its own position, not the final state") {
      for
        s        <- store()
        outcomes <- foldWrite(s).call("k", NonEmptyChunk(5, 7, 9))
      yield assertTrue(outcomes == Map(5 -> Right(0), 7 -> Right(1), 9 -> Right(2)))
    },
    test("lets an input change how later inputs of the same batch apply, such as closing a period") {
      for
        ledgers <- Ref.make(Map.empty[String, Ledger])
        write = FoldWrite[Any, Nothing, String, Posting, String, Ledger, Int](
          id = postingId,
          read = key => ledgers.get.map(_.getOrElse(key, Ledger(0, Map.empty))),
          step = post,
          write = (key, _, after) => ledgers.update(_.updated(key, after)),
          retry = Schedule.stop
        )
        outcomes <- write.call("k", NonEmptyChunk(Posting.Add("a", 1), Posting.Close("c"), Posting.Add("b", 2)))
        ledger   <- ledgers.get.map(_("k"))
      yield assertTrue(
        ledger == Ledger(period = 1, totals = Map(0 -> 1L, 1 -> 2L)),
        outcomes == Map("a" -> 0, "c" -> 1, "b" -> 1)
      )
    },
    test("lets a rejected input block the ones after it, in its batch and the next, when the state carries the block") {
      for
        accounts <- Ref.make(Map.empty[String, Account])
        write = FoldWrite[Any, Nothing, String, Debit, String, Account, Either[Rejection, Long]](
          id = _.id,
          read = key => accounts.get.map(_.getOrElse(key, Account(10, None))),
          step = debit,
          write = (key, _, after) => accounts.update(_.updated(key, after)),
          retry = Schedule.stop
        )
        first   <- write.call("k", NonEmptyChunk(Debit("a", 4), Debit("b", 8), Debit("c", 1)))
        next    <- write.call("k", NonEmptyChunk(Debit("d", 1)))
        replay  <- write.call("k", NonEmptyChunk(Debit("b", 8)))
        account <- accounts.get.map(_("k"))
      yield assertTrue(
        first == Map(
          "a" -> Right(6L),
          "b" -> Left(Rejection.InsufficientFunds),
          "c" -> Left(Rejection.NotAttempted("b"))
        ),
        next == Map("d" -> Left(Rejection.NotAttempted("b"))),
        replay == Map("b" -> Left(Rejection.InsufficientFunds)),
        account == Account(6, Some("b"))
      )
    },
    test("serves Ferry.perKey as its call and extract") {
      for
        s   <- store()
        cfg <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 10, maxInFlightBatches = 2, maxQueueing = 100)).orDie
        write = foldWrite(s)
        ferry   <- Ferry.perKey(cfg, (_: Int) => "k")(write.call)(write.outcome)
        results <- ZIO.foreachPar(1 to 20)(ferry(_))
        log     <- s.read("k")
      yield assertTrue(log.sorted == (1 to 20).toVector, results.forall(_.isRight))
    }
  )
