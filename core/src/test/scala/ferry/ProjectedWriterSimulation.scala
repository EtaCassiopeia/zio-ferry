package ferry

import zio.*
import zio.test.*

import ProjectedWrite.Outcome

/**
 * `FencedWriterSimulation` for a store that writes in two steps, through [[ProjectedWrite]]: several nodes commit each
 * key's batches to a shared store, fenced on the log they read, then project the record into a view, fenced on the
 * record being as committed; a sweeper finishes projections that did not happen. Modelled in
 * `formal/ProjectedWriter.tla`; the decision is `docs/decisions/0014-commit-then-project.md`.
 *
 * Faults: the store refuses a write (throttling, or a refused projection), applies a write but loses its response, or
 * applies it late, after its sender may have crashed. Clients may reach a node that does not own the key. A node may
 * crash part way through, or right between a commit and its projection.
 *
 * The projection is correct only with its fence, the view is complete only with a sweeper, and `Projected` is true only
 * if it is answered after the projection: [[Writer]] and [[Scenario.sweeper]] can switch each off, to show each is
 * needed.
 */
object ProjectedWriterSimulation:

  final case class Command(id: Int, key: Int)
  final case class Submission(command: Command, gapMs: Long, misrouted: Boolean)

  enum Fault:
    case Pass
    case Throttle
    case LostResponse
    case Late(ms: Long)
    case RefuseProject

  enum StoreError:
    case Throttled, TimedOut, Conflict

  /**
   * @param earlyAnswer
   *   Answer `Projected` as soon as the commit lands, leaving the projection to the sweeper: a writer that cuts
   *   corners.
   */
  final case class Writer(fencedProject: Boolean = true, earlyAnswer: Boolean = false)

  /**
   * @param crashBeforeProject
   *   A node that crashes at its first projection: its commit landed, its projection never runs.
   */
  final case class Scenario(
    submissions: List[Submission],
    faults: List[Fault],
    crash: Option[(Int, Long)],
    crashBeforeProject: Option[Int],
    sweeper: Boolean
  )

  /** A key's record: its log, and how much of it is projected (the marker). */
  final case class Record(log: Vector[Int], projectedUpTo: Int)

  /** A command's answer: its position, whether it came back projected, and whether it was in the view then. */
  final case class Ack(position: Int, projected: Boolean, inView: Boolean)

  final case class Trace(
    records: Map[Int, Record],
    views: Map[Int, Vector[Int]],
    acks: Map[Int, Ack],
    unanswered: Set[Int],
    overlapping: Boolean
  )

  val nodes = 2

  private val empty = Record(Vector.empty, 0)

  def pending(r: Record): Boolean = r.projectedUpTo < r.log.size

  /**
   * One record and one view per key. A commit is applied only if the log is still the one its writer read, and leaves
   * the marker as stored. A projection appends the record's unprojected entries to the view and advances the marker,
   * only if the store still holds the record its writer read, unless the writer is unfenced. A late write is applied
   * after a delay even if its sender is gone, like a request already on the wire.
   */
  final class Store(
    state: Ref[Map[Int, (Record, Vector[Int])]],
    faults: Ref[List[Fault]],
    onTheWire: Ref[Int],
    fencedProject: Boolean
  ):

    def read(key: Int): UIO[Record] = state.get.map(_.get(key).fold(empty)(_._1))

    def view(key: Int): UIO[Vector[Int]] = state.get.map(_.get(key).fold(Vector.empty)(_._2))

    def pendingKeys: UIO[List[Int]] = state.get.map(_.collect { case (key, (r, _)) if pending(r) => key }.toList)

    def snapshot: UIO[Map[Int, (Record, Vector[Int])]] = state.get

    def settled: UIO[Boolean] = onTheWire.get.map(_ == 0)

    def commit(key: Int, before: Record, after: Record): IO[StoreError, Unit] =
      withFault(projection = false)(applyCommit(key, before, after))

    def project(key: Int, s: Record): IO[StoreError, Unit] =
      withFault(projection = true)(applyProject(key, s))

    private def withFault(projection: Boolean)(apply: IO[StoreError, Unit]): IO[StoreError, Unit] =
      faults.modify(fs => fs.headOption -> fs.drop(1)).flatMap {
        case Some(Fault.Throttle)                    => ZIO.fail(StoreError.Throttled)
        case Some(Fault.RefuseProject) if projection => ZIO.fail(StoreError.Conflict)
        case Some(Fault.LostResponse)                => apply.ignore *> ZIO.fail(StoreError.TimedOut)
        case Some(Fault.Late(ms))                    =>
          (onTheWire.update(_ + 1) *> ZIO.sleep(ms.millis) *> apply.ensuring(onTheWire.update(_ - 1))).forkDaemon
            .flatMap(_.join)
        case _ => apply
      }

    private def applyCommit(key: Int, before: Record, after: Record): IO[StoreError, Unit] =
      state.modify { m =>
        val (record, view) = m.getOrElse(key, (empty, Vector.empty[Int]))
        if record.log == before.log then Right(()) -> m.updated(key, (record.copy(log = after.log), view))
        else Left(StoreError.Conflict)             -> m
      }.absolve

    private def applyProject(key: Int, s: Record): IO[StoreError, Unit] =
      state.modify { m =>
        val (record, view) = m.getOrElse(key, (empty, Vector.empty[Int]))
        if !fencedProject || record == s then
          Right(()) -> m.updated(key, (record.copy(projectedUpTo = s.log.size), view ++ s.log.drop(s.projectedUpTo)))
        else Left(StoreError.Conflict) -> m
      }.absolve

  private val retry = Schedule.recurs(4) && Schedule.exponential(5.millis)

  /** A command already in the log is answered with its position; any other is appended. */
  private def step(r: Record, command: Command): (Record, Int) =
    if r.log.contains(command.id) then r   -> r.log.indexOf(command.id)
    else r.copy(log = r.log :+ command.id) -> r.log.size

  def write(store: Store, project: (Int, Record) => IO[StoreError, Unit]) =
    ProjectedWrite(FoldWrite((_: Command).id, store.read, step, store.commit, retry), pending, project)

  def scenario: Gen[Any, Scenario] =
    for
      submissions <- Gen.listOfBounded(1, 20)(
        Gen.int(0, 2) <*> Gen.long(0, 2).map(_ * 5) <*> Gen.weighted(Gen.const(true) -> 1.0, Gen.const(false) -> 3.0)
      )
      faults <- Gen.listOfBounded(0, 12)(
        Gen.oneOf(
          Gen.const(Fault.Pass),
          Gen.const(Fault.Throttle),
          Gen.const(Fault.LostResponse),
          Gen.const(Fault.RefuseProject),
          Gen.long(1, 12).map(n => Fault.Late(n * 5))
        )
      )
      // At most one node crashes, at a random time or between its writes, so that one is always left to answer.
      crash              <- Gen.option(Gen.int(0, nodes - 1) <*> Gen.long(0, 30).map(_ * 5))
      crashBeforeProject <-
        if crash.isDefined then Gen.const(None) else Gen.option(Gen.int(0, nodes - 1))
    yield Scenario(
      submissions.zipWithIndex.map { case ((key, gap, misrouted), id) => Submission(Command(id, key), gap, misrouted) },
      faults,
      crash,
      crashBeforeProject,
      sweeper = true
    )

  def run(s: Scenario, writer: Writer = Writer()): UIO[Trace] =
    for
      state     <- Ref.make(Map.empty[Int, (Record, Vector[Int])])
      faults    <- Ref.make(s.faults)
      onTheWire <- Ref.make(0)
      store = Store(state, faults, onTheWire, writer.fencedProject)
      cfg     <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 8, maxInFlightBatches = 4, maxQueueing = 64)).orDie
      inUse   <- Ref.make(Map.empty[(Int, Int), Int])
      overlap <- Ref.make(false)
      crashed <- Ref.make(Set.empty[Int])
      armed   <- Ref.make(s.crashBeforeProject.toSet)
      scopes  <- ZIO.foreach(0 until nodes)(_ => Scope.make)
      crash = (node: Int) => crashed.update(_ + node) *> scopes(node).close(Exit.unit)
      ferries <- ZIO.foreach(scopes.zipWithIndex) { (scope, node) =>
        // A node armed to crash between the writes dies at its first projection; its commit has landed.
        val project = (key: Int, r: Record) =>
          armed.modify(a => a(node) -> (a - node)).flatMap { crashNow =>
            if crashNow then crash(node).forkDaemon *> ZIO.never else store.project(key, r)
          }
        val projected = write(store, project)
        scope.extend(Ferry.perKey(cfg, (_: Command).key) { (key, commands: NonEmptyChunk[Command]) =>
          val slot = node -> key
          inUse
            .modify(m => m.getOrElse(slot, 0) -> m.updated(slot, m.getOrElse(slot, 0) + 1))
            .flatMap(n => overlap.set(true).when(n > 0)) *>
            (if writer.earlyAnswer then
               projected.fold.call(key, commands).map(_.view.mapValues(Outcome.Projected(_)).toMap)
             else projected.call(key, commands)).ensuring(inUse.update(m => m.updated(slot, m(slot) - 1)))
        }(projected.outcome))
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
                store.view(sub.command.key).flatMap { view =>
                  acks.update(
                    _ + (sub.command.id -> Ack(outcome.value, outcome.isProjected, view.contains(sub.command.id)))
                  )
                }
              case Exit.Failure(_) => ZIO.when(n < 30)(ZIO.sleep(20.millis) *> attempt(n + 1)).unit
            }
          }
        attempt(0).ensuring(done.update(_ + sub.command.id))
      // Clients and the sweeper outlive the fiber that starts them, so they belong to a scope of their own.
      background <- Scope.make
      sweeper = write(store, store.project)
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
    yield Trace(
      snapshot.view.mapValues(_._1).toMap,
      snapshot.view.mapValues(_._2).toMap,
      acked,
      s.submissions.map(_.command.id).toSet -- acked.keySet,
      overlapped
    )

  def violations(s: Scenario, t: Trace): List[String] =
    val keyOf = s.submissions.map(sub => sub.command.id -> sub.command.key).toMap
    List(
      t.records.collect {
        case (key, r) if r.log.distinct.size != r.log.size => s"key $key applied a command twice: ${r.log}"
      },
      t.views.collect {
        case (key, view) if view.distinct.size != view.size => s"key $key projected an entry twice: $view"
      },
      t.views.collect {
        case (key, view) if t.records(key).log.take(view.size) != view =>
          s"key $key has a view $view that is not a projection of its log ${t.records(key).log}"
      },
      t.records.collect {
        case (key, r) if pending(r) => s"key $key is still pending: ${r.log.size - r.projectedUpTo} entries unprojected"
      },
      t.acks.collect {
        case (id, ack) if !t.records.get(keyOf(id)).flatMap(_.log.lift(ack.position)).contains(id) =>
          s"command $id was answered with position ${ack.position}, but the log of key ${keyOf(id)} is ${t.records.get(keyOf(id))}"
      },
      t.acks.collect {
        case (id, ack) if ack.projected && !ack.inView =>
          s"command $id was answered Projected before it was in the view"
      },
      Option.when(t.unanswered.nonEmpty)(s"commands never answered: ${t.unanswered}"),
      Option.when(t.overlapping)("a node had two batches of one key in flight at once")
    ).flatten
