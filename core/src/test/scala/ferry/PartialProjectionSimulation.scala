package ferry

import zio.*
import zio.stm.*
import zio.test.*

import ferry.Protocol.Action

import ProjectedWrite.Projection

/**
 * `ProjectedWriterSimulation` for a store whose view is one row per entry, through a per-entry [[ProjectedWrite]]:
 * several nodes commit each key's batches to a shared store, fenced on the log they read, then write each entry's row,
 * idempotently, and clear the record's flag once every row is in, fenced on the record being as committed; a sweeper
 * finishes projections that did not happen or only partly happened. Modelled in `formal/ProjectedWriterPartial.tla`;
 * the decision is `docs/decisions/0015-per-entry-projection.md`.
 *
 * Faults: the store refuses a write (throttling, or a refused clear), applies a write but loses its response, or
 * applies it late, after its sender may have crashed. Row writes draw from a fault queue of their own, so a scenario
 * can name which row fails. Clients may reach a node that does not own the key. A node may crash part way through,
 * right between a commit and its projection, or between two row writes of one projection.
 *
 * The view holds each row once only with idempotent rows, the flag is honest only with a clear that waits for every row
 * and is fenced, and the view is complete only with a sweeper: [[Writer]] and [[Scenario.sweeper]] can switch each off,
 * to show each is needed.
 *
 * Every step is recorded, in one STM log, in the order the steps took effect: the actions of each node's ferry (through
 * its [[Recorder]]) and the store's reads, writes and deliveries, each with who made it, a node or the sweeper.
 * [[PartialProjectionConformance]] checks the recording against the model, key by key.
 */
object PartialProjectionSimulation:

  final case class Command(id: Int, key: Int)
  final case class Submission(command: Command, gapMs: Long, misrouted: Boolean)

  enum Fault:
    case Pass
    case Throttle
    case LostResponse
    case Late(ms: Long)
    case ThrottleRow
    case LostRow
    case RefuseClear

  enum StoreError:
    case Throttled, TimedOut, Conflict

  /**
   * @param conditionalRows
   *   A row write applies only if the row is not already in the view.
   * @param clearWhenComplete
   *   The flag is cleared only when no row is left pending.
   * @param fencedClear
   *   The flag is cleared only if the store still holds the record the projection read.
   */
  final case class Writer(
    conditionalRows: Boolean = true,
    clearWhenComplete: Boolean = true,
    fencedClear: Boolean = true
  )

  /**
   * @param faults
   *   Consumed one per commit or clear.
   * @param rowFaults
   *   Consumed one per row write.
   * @param crashBeforeProject
   *   A node that crashes at its first projection: its commit landed, its projection never runs.
   * @param crashMidProjection
   *   A node that crashes as soon as its first row write lands: the rest of that projection never runs.
   */
  final case class Scenario(
    submissions: List[Submission],
    faults: List[Fault],
    rowFaults: List[Fault],
    crash: Option[(Int, Long)],
    crashBeforeProject: Option[Int],
    crashMidProjection: Option[Int],
    sweeper: Boolean
  )

  /** A key's record: its log, and whether its projection is pending (the marker). */
  final case class Record(log: Vector[Int], flag: Boolean)

  /**
   * A command's answer: its position, whether it came back projected, whether its row was in the view then, and the
   * cause it came back with if it was not projected.
   */
  final case class Ack(position: Int, projected: Boolean, inRows: Boolean, cause: Option[StoreError])

  /** How a row write ended: it landed, the store refused it, or it applied and its answer was lost. */
  enum RowOutcome:
    case Landed, Refused, Lost

  /**
   * A step of the run: an action of node `node`'s ferry, or a step of the store. `who` is the node that made it, or
   * `None` for the sweeper.
   */
  enum Event:
    case Ferry(node: Int, action: Action[Int])
    case Read(node: Int, key: Int)
    case Sweep(key: Int)
    case Commit(node: Int, key: Int, length: Int, ids: Vector[Int])
    case DeliverCommit(node: Int, key: Int, length: Int, ids: Vector[Int], applied: Boolean, answered: Boolean)
    case ReadRows(who: Option[Int], key: Int)
    case Row(who: Option[Int], key: Int, id: Int)
    case DeliverRow(who: Option[Int], key: Int, id: Int, outcome: RowOutcome, applied: Boolean)
    case Clear(who: Option[Int], key: Int, length: Int)
    case DeliverClear(who: Option[Int], key: Int, length: Int, applied: Boolean, answered: Boolean)
    case Throttle(who: Option[Int], key: Int, commit: Boolean, length: Int, ids: Vector[Int])

  final case class Trace(
    records: Map[Int, Record],
    rows: Map[Int, Vector[Int]],
    acks: Map[Int, Ack],
    unanswered: Set[Int],
    overlapping: Boolean,
    earlyClears: List[String],
    crashed: Set[Int],
    events: Chunk[Event]
  )

  val nodes = 2

  private val empty = Record(Vector.empty, flag = false)

  def pending(r: Record): Boolean = r.flag

  /**
   * One record and one view per key. A commit is applied only if the log is still the one its writer read. A row write
   * appends the entry's row to the view, unless it is already there (when rows are conditional). A clear lowers the
   * flag only if the store still holds the record its writer read (when fenced); a clear that lands while a row of the
   * log is missing from the view is recorded. A late write is applied after a delay even if its sender is gone, like a
   * request already on the wire. Each step is recorded in `events` in the transaction that takes it; [[as]] gives a
   * node, or the sweeper, its own handle, so that steps say who made them.
   */
  final class Store(
    state: TRef[Map[Int, (Record, Vector[Int])]],
    faults: TRef[List[Fault]],
    rowFaults: TRef[List[Fault]],
    events: TRef[Chunk[Event]],
    onTheWire: Ref[Int],
    earlyClears: Ref[List[String]],
    writer: Writer
  ):

    private def record(event: Event): USTM[Unit] = events.update(_ :+ event)

    private def recordOf(key: Int): USTM[Record] = state.get.map(_.get(key).fold(empty)(_._1))

    private def rowsOf(key: Int): USTM[Vector[Int]] = state.get.map(_.get(key).fold(Vector.empty)(_._2))

    def rows(key: Int): UIO[Vector[Int]] = rowsOf(key).commit

    def pendingKeys: UIO[List[Int]] =
      state.get.map(_.collect { case (key, (r, _)) if pending(r) => key }.toList).commit

    def snapshot: UIO[Map[Int, (Record, Vector[Int])]] = state.get.commit

    def settled: UIO[Boolean] = onTheWire.get.map(_ == 0)

    def cleared: UIO[List[String]] = earlyClears.get

    /** The store as `who` uses it: a node, or the sweeper (`None`). */
    final class Handle(who: Option[Int]):

      /**
       * Reads the key's record. A node's read is a step of its own; the sweeper's is a step only if there is a
       * projection to finish.
       */
      def read(key: Int): UIO[Record] =
        recordOf(key).tap { r =>
          who match
            case Some(n) => record(Event.Read(n, key))
            case None    => record(Event.Sweep(key)).when(pending(r)).unit
        }.commit

      def commit(key: Int, before: Record, after: Record): IO[StoreError, Unit] =
        val node = who.getOrElse(throw IllegalStateException("the sweeper never commits"))
        val ids  = after.log.drop(before.log.size)
        record(Event.Commit(node, key, before.log.size, ids)).commit *>
          withFault(faults, clearing = false, record(Event.Throttle(who, key, true, before.log.size, ids)))(
            applyCommit(node, key, before, after, _)
          )

      /**
       * Writes each row of `record` the view is missing, then clears the flag when none is left pending. `landed` runs
       * after each row write that lands, for a crash between two row writes.
       */
      def projectRows(
        key: Int,
        record: Record,
        landed: UIO[Unit] = ZIO.unit
      ): IO[StoreError, Projection[Int, StoreError]] =
        for
          present <- rowsOf(key).tap(_ => Store.this.record(Event.ReadRows(who, key))).commit
          results <- ZIO.foreach(record.log.filterNot(present.contains))(id =>
            (writeRow(key, id) *> landed).either.map(id -> _)
          )
          left = results.collect { case (id, Left(e)) => id -> e }.toMap
          _ <- clear(key, record).when(left.isEmpty || !writer.clearWhenComplete)
        yield Projection(left)

      private def writeRow(key: Int, id: Int): IO[StoreError, Unit] =
        record(Event.Row(who, key, id)).commit *>
          withFault(
            rowFaults,
            clearing = false,
            record(Event.DeliverRow(who, key, id, RowOutcome.Refused, applied = false))
          )(
            applyRow(who, key, id, _)
          )

      private def clear(key: Int, r: Record): IO[StoreError, Unit] =
        record(Event.Clear(who, key, r.log.size)).commit *>
          withFault(faults, clearing = true, record(Event.Throttle(who, key, false, r.log.size, Vector.empty)))(
            applyClear(who, key, r, _)
          )

    def as(who: Option[Int]): Handle = Handle(who)

    private def withFault(queue: TRef[List[Fault]], clearing: Boolean, refused: USTM[Unit])(
      apply: Boolean => IO[StoreError, Unit]
    ): IO[StoreError, Unit] =
      queue.modify(fs => fs.headOption -> fs.drop(1)).commit.flatMap {
        case Some(Fault.Throttle | Fault.ThrottleRow) => refused.commit *> ZIO.fail(StoreError.Throttled)
        case Some(Fault.RefuseClear) if clearing      => refused.commit *> ZIO.fail(StoreError.Conflict)
        case Some(Fault.LostResponse | Fault.LostRow) => apply(false).ignore *> ZIO.fail(StoreError.TimedOut)
        case Some(Fault.Late(ms))                     =>
          (onTheWire.update(_ + 1) *> ZIO.sleep(ms.millis) *> apply(true).ensuring(onTheWire.update(_ - 1))).forkDaemon
            .flatMap(_.join)
        case _ => apply(true)
      }

    private def applyCommit(node: Int, key: Int, before: Record, after: Record, answered: Boolean) =
      state
        .modify { m =>
          val (stored, rows) = m.getOrElse(key, (empty, Vector.empty[Int]))
          val applies        = stored.log == before.log
          applies -> (if applies then m.updated(key, (after, rows)) else m)
        }
        .tap { applied =>
          record(
            Event.DeliverCommit(node, key, before.log.size, after.log.drop(before.log.size), applied, answered)
          )
        }
        .commit
        .flatMap(applied => ZIO.fail(StoreError.Conflict).unless(applied).unit)

    private def applyRow(who: Option[Int], key: Int, id: Int, answered: Boolean): UIO[Unit] =
      state
        .modify { m =>
          val (stored, rows) = m.getOrElse(key, (empty, Vector.empty[Int]))
          val applies        = !(writer.conditionalRows && rows.contains(id))
          applies -> (if applies then m.updated(key, (stored, rows :+ id)) else m)
        }
        .flatMap { applied =>
          record(Event.DeliverRow(who, key, id, if answered then RowOutcome.Landed else RowOutcome.Lost, applied))
        }
        .commit

    private def applyClear(who: Option[Int], key: Int, r: Record, answered: Boolean): IO[StoreError, Unit] =
      state
        .modify { m =>
          val (stored, rows) = m.getOrElse(key, (empty, Vector.empty[Int]))
          if !writer.fencedClear || stored == r then
            val missing = stored.log.filterNot(rows.contains)
            Some(missing.map(id => s"key $key's marker cleared early: row $id missing")) ->
              m.updated(key, (stored.copy(flag = false), rows))
          else None -> m
        }
        .tap(early => record(Event.DeliverClear(who, key, r.log.size, early.isDefined, answered)))
        .commit
        .flatMap {
          case Some(early) => earlyClears.update(_ ++ early)
          case None        => ZIO.fail(StoreError.Conflict)
        }

  private val retry = Schedule.recurs(4) && Schedule.exponential(5.millis)

  /** A command already in the log is answered with its position; any other is appended, which sets the flag. */
  private def step(r: Record, command: Command): (Record, Int) =
    if r.log.contains(command.id) then r          -> r.log.indexOf(command.id)
    else Record(r.log :+ command.id, flag = true) -> r.log.size

  def write(store: Store#Handle, project: (Int, Record) => IO[StoreError, Projection[Int, StoreError]]) =
    ProjectedWrite(FoldWrite((_: Command).id, store.read, step, store.commit, retry), pending, project)

  private val one = List(Submission(Command(0, 0), 0, misrouted = false))

  def quiet(submissions: List[Submission]): Scenario =
    Scenario(submissions, Nil, Nil, crash = None, crashBeforeProject = None, crashMidProjection = None, sweeper = true)

  /** Three commands on one key at once; the second row written is throttled. */
  val throttledRow: Scenario =
    quiet(List.tabulate(3)(id => Submission(Command(id, 0), 0, misrouted = false)))
      .copy(rowFaults = List(Fault.Pass, Fault.ThrottleRow))

  /** The only row is throttled: the clear must wait, and only a sweeper writes the row. */
  val throttledOnlyRow: Scenario = quiet(one).copy(rowFaults = List(Fault.ThrottleRow))

  /** The writer's row is late on the wire; meanwhile the sweeper writes the same row. */
  val lateRow: Scenario = quiet(one).copy(rowFaults = List(Fault.Late(30)))

  /**
   * Command 0's clear is late on the wire; meanwhile command 1, sent to the other node, commits, and its row is later
   * still. The stale clear lands while row 1 is missing. No sweeper, which would otherwise finish row 1 first.
   */
  val staleClear: Scenario =
    quiet(List(Submission(Command(0, 0), 0, misrouted = false), Submission(Command(1, 0), 5, misrouted = true)))
      .copy(faults = List(Fault.Pass, Fault.Late(30)), rowFaults = List(Fault.Pass, Fault.Late(60)), sweeper = false)

  /** Two commands; the node crashes once its first row lands. */
  val crashMidProjection: Scenario =
    quiet(List.tabulate(2)(id => Submission(Command(id, 0), 0, misrouted = false))).copy(crashMidProjection = Some(0))

  /** The node's commit is throttled until its retries are exhausted: the call fails, and the client sends again. */
  val failedCommit: Scenario = quiet(one).copy(faults = List.fill(5)(Fault.Throttle))

  def scenario: Gen[Any, Scenario] =
    def fault(kinds: Gen[Any, Fault]*) =
      Gen.listOfBounded(0, 12)(
        Gen.oneOf((Gen.const(Fault.Pass) +: Gen.long(1, 12).map(n => Fault.Late(n * 5)) +: kinds)*)
      )
    for
      submissions <- Gen.listOfBounded(1, 20)(
        Gen.int(0, 2) <*> Gen.long(0, 2).map(_ * 5) <*> Gen.weighted(Gen.const(true) -> 1.0, Gen.const(false) -> 3.0)
      )
      faults <- fault(Gen.const(Fault.Throttle), Gen.const(Fault.LostResponse), Gen.const(Fault.RefuseClear))
      rows   <- fault(Gen.const(Fault.ThrottleRow), Gen.const(Fault.LostRow))
      // At most one node crashes: at a random time, between its writes, or between two rows, so that one is left.
      crash   <- Gen.option(Gen.int(0, nodes - 1) <*> Gen.long(0, 30).map(_ * 5))
      between <-
        if crash.isDefined then Gen.const(None)
        else Gen.option(Gen.int(0, nodes - 1) <*> Gen.boolean)
    yield Scenario(
      submissions.zipWithIndex.map { case ((key, gap, misrouted), id) => Submission(Command(id, key), gap, misrouted) },
      faults,
      rows,
      crash,
      between.collect { case (node, true) => node },
      between.collect { case (node, false) => node },
      sweeper = true
    )

  def run(s: Scenario, writer: Writer = Writer()): UIO[Trace] =
    for
      state       <- TRef.make(Map.empty[Int, (Record, Vector[Int])]).commit
      faults      <- TRef.make(s.faults).commit
      rowFaults   <- TRef.make(s.rowFaults).commit
      events      <- TRef.make(Chunk.empty[Event]).commit
      onTheWire   <- Ref.make(0)
      earlyClears <- Ref.make(List.empty[String])
      store = Store(state, faults, rowFaults, events, onTheWire, earlyClears, writer)
      // A batch takes a key's whole lane, as a node of the model takes its whole inbox.
      cfg      <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 32, maxInFlightBatches = 4, maxQueueing = 64)).orDie
      inUse    <- Ref.make(Map.empty[(Int, Int), Int])
      overlap  <- Ref.make(false)
      crashed  <- Ref.make(Set.empty[Int])
      armed    <- Ref.make(s.crashBeforeProject.toSet)
      armedMid <- Ref.make(s.crashMidProjection.toSet)
      scopes   <- ZIO.foreach(0 until nodes)(_ => Scope.make)
      crash   = (node: Int) => crashed.update(_ + node) *> scopes(node).close(Exit.unit)
      crashIf = (trigger: Ref[Set[Int]], node: Int) =>
        trigger.modify(a => a(node) -> (a - node)).flatMap(now => ZIO.when(now)(crash(node).forkDaemon *> ZIO.never))
      ferries <- ZIO.foreach(scopes.zipWithIndex) { (scope, node) =>
        // A node armed to crash between the writes dies at its first projection; its commit has landed. A node armed
        // to crash mid-projection dies once its first row lands.
        val handle  = store.as(Some(node))
        val project =
          (key: Int, r: Record) => crashIf(armed, node) *> handle.projectRows(key, r, crashIf(armedMid, node).unit)
        val projected = write(handle, project)
        val recorder  = new Recorder[Int]:
          def record(actions: Chunk[Action[Int]]): USTM[Unit] = events.update(_ ++ actions.map(Event.Ferry(node, _)))
        // Ferry.perKey, with a recorder; the rank is the command's id, which names each caller's command in the trace.
        scope.extend(
          Engine.make(
            Ferry.perKeyLimits(cfg),
            cfg.maxInFlightBatches,
            (_: Command).key,
            _ => 1L,
            (c: Command) => c.id.toLong,
            recorder
          ) { (key, commands: NonEmptyChunk[Command]) =>
            val slot = node -> key
            inUse
              .modify(m => m.getOrElse(slot, 0) -> m.updated(slot, m.getOrElse(slot, 0) + 1))
              .flatMap(n => overlap.set(true).when(n > 0)) *>
              projected.call(key, commands).ensuring(inUse.update(m => m.updated(slot, m(slot) - 1)))
          }(projected.outcome)
        )
      }
      acks <- Ref.make(Map.empty[Int, Ack])
      done <- Ref.make(Set.empty[Int])
      client = (sub: Submission) =>
        def attempt(n: Int): UIO[Unit] =
          crashed.get.flatMap { down =>
            val owner = sub.command.key % nodes
            val node  = if (sub.misrouted && n == 0) || down(owner) then (owner + 1) % nodes else owner
            ferries(node)(sub.command).exit.flatMap {
              case Exit.Success(outcome) =>
                store.rows(sub.command.key).flatMap { rows =>
                  val cause = outcome match
                    case ProjectedWrite.Outcome.Committed(_, e) => Some(e)
                    case ProjectedWrite.Outcome.Projected(_)    => None
                  acks.update(
                    _ + (sub.command.id -> Ack(
                      outcome.value,
                      outcome.isProjected,
                      rows.contains(sub.command.id),
                      cause
                    ))
                  )
                }
              case Exit.Failure(_) => ZIO.when(n < 30)(ZIO.sleep(20.millis) *> attempt(n + 1)).unit
            }
          }
        attempt(0).ensuring(done.update(_ + sub.command.id))
      // Clients and the sweeper outlive the fiber that starts them, so they belong to a scope of their own.
      background <- Scope.make
      sweeper = write(store.as(None), store.as(None).projectRows(_, _))
      _ <- ZIO.when(s.sweeper)(
        (ZIO.sleep(20.millis) *> store.pendingKeys.flatMap(ZIO.foreachDiscard(_)(sweeper.resume(_).ignore))).forever
          .forkIn(background)
      )
      _ <- ZIO.foreachDiscard(s.submissions)(sub => ZIO.sleep(sub.gapMs.millis) *> client(sub).forkIn(background)).fork
      _ <- ZIO.foreachDiscard(s.crash)((node, atMs) => (ZIO.sleep(atMs.millis) *> crash(node)).fork)
      finished = done.get.map(_.size == s.submissions.size) && store.settled &&
        (if s.sweeper then store.pendingKeys.map(_.isEmpty) else ZIO.succeed(true))
      _          <- VirtualTime.settle(finished)
      _          <- ZIO.foreachDiscard(background +: scopes)(_.close(Exit.unit))
      _          <- TestClock.adjust(1.minute) // let late writes still on the wire land
      snapshot   <- store.snapshot
      acked      <- acks.get
      overlapped <- overlap.get
      early      <- store.cleared
      down       <- crashed.get
      recorded   <- events.get.commit
    yield Trace(
      snapshot.view.mapValues(_._1).toMap,
      snapshot.view.mapValues(_._2).toMap,
      acked,
      s.submissions.map(_.command.id).toSet -- acked.keySet,
      overlapped,
      early,
      down,
      recorded
    )

  def violations(s: Scenario, t: Trace): List[String] =
    val keyOf = s.submissions.map(sub => sub.command.id -> sub.command.key).toMap
    List(
      t.records.collect {
        case (key, r) if r.log.distinct.size != r.log.size => s"key $key applied a command twice: ${r.log}"
      },
      t.rows.collect {
        case (key, rows) if rows.distinct.size != rows.size => s"key $key wrote a row twice: $rows"
      },
      t.rows.collect {
        case (key, rows) if !rows.forall(t.records(key).log.contains) =>
          s"key $key has rows $rows that are not all in its log ${t.records(key).log}"
      },
      t.earlyClears,
      t.records.collect {
        case (key, r) if pending(r) =>
          s"key $key is still pending: rows ${r.log.filterNot(t.rows.getOrElse(key, Vector.empty).contains)} missing"
      },
      t.acks.collect {
        case (id, ack) if !t.records.get(keyOf(id)).flatMap(_.log.lift(ack.position)).contains(id) =>
          s"command $id was answered with position ${ack.position}, but the log of key ${keyOf(id)} is ${t.records.get(keyOf(id))}"
      },
      t.acks.collect {
        case (id, ack) if ack.projected && !ack.inRows =>
          s"command $id was answered Projected before its row was in the view"
      },
      Option.when(t.unanswered.nonEmpty)(s"commands never answered: ${t.unanswered}"),
      Option.when(t.overlapping)("a node had two batches of one key in flight at once")
    ).flatten
