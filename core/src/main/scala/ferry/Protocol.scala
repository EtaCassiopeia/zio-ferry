package ferry

import scala.collection.immutable.{Queue, TreeMap}

import zio.{Chunk, NonEmptyChunk}

/**
 * The protocol behind every Ferry, as a pure state machine: all of its shared state, and one method per step.
 *
 * Each method is one named step, and returns the next state together with the [[Protocol.Action]]s it took. The ZIO
 * shell ([[Engine]]) applies each step in one STM transaction and records its actions inside it, so a recorded run is
 * the sequence of steps the protocol took, in the order they took effect. Because the protocol is pure and its only
 * choices are its environment's, it is checked directly: the tests explore every state of small instances, and replay
 * recorded runs of the engine through it, with its properties checked after every step.
 *
 * Inputs wait in a lane per key, in ascending rank, ties in arrival order. A lane is in exactly one of three states:
 *   - `Waiting(deadline)`: holds a partial batch whose oldest input is not yet due;
 *   - `Ready`: in line to be taken, because it is full or due;
 *   - `Blocked`: its key has a call in flight and allows only one at a time.
 *
 * Ready lanes are served first come, first served; a lane that still holds inputs after a batch is taken, or once its
 * key's call completes, joins the back of the line, so a key that always has more never starves the others.
 *
 * Nothing here reads a clock or runs an effect: `now` is a parameter, and what the shell must do (send a batch, answer
 * a caller) is in the result.
 */
final private[ferry] case class Protocol[K, X](
  limits: Protocol.Limits,
  open: Boolean,
  nextCaller: Long,
  pending: Map[Long, X],
  lanes: Map[K, Protocol.Lane[X]],
  ready: Queue[K],
  waiting: TreeMap[Long, Vector[K]],
  busy: Map[K, Int],
  batches: Map[Int, Protocol.Batch[K, X]],
  direct: Map[Long, K],
  queued: Int,
  weight: Long
):
  import Protocol.*

  /**
   * A caller submits `value` for `key`. It is refused once admission is closed; otherwise it claims an idle key for a
   * direct call (when direct calls are allowed), joins the key's lane if the bounds allow, or is rejected.
   */
  def submit(key: K, value: X, weight: Long, rank: Long, now: Long): Step[K, X, Admission] =
    val caller = nextCaller
    val item   = Item(caller, value, weight, now, rank)
    val ticket = copy(nextCaller = caller + 1)
    if !open then Step(Admission.Refused(caller), ticket, Chunk.single(Action.Refuse(caller, key, weight, rank)))
    else if limits.direct && idle(key) then
      Step(
        Admission.Claimed(caller),
        ticket.copy(
          pending = pending.updated(caller, value),
          busy = busy.updated(key, 1),
          direct = direct.updated(caller, key)
        ),
        Chunk.single(Action.Claim(caller, key, weight, rank))
      )
    else if queued >= limits.maxQueueing || weight > limits.maxQueuedWeight - this.weight then
      Step(Admission.Rejected(caller), ticket, Chunk.single(Action.Reject(caller, key, weight, rank)))
    else
      val lane = lanes.getOrElse(key, Lane(Vector.empty[Item[X]], 0L, Schedule.Blocked))
      Step(
        Admission.Queued(caller),
        ticket
          .copy(pending = pending.updated(caller, value), queued = queued + 1, weight = this.weight + weight)
          .place(key, lane.inserted(item), lane.weight + weight),
        Chunk.single(Action.Enqueue(caller, key, weight, rank))
      )

  /**
   * `worker` takes the next batch at time `now`: first every waiting lane that is due by `now` joins the ready line
   * (one `Due` each, in deadline order), then the first ready lane gives the longest prefix of its inputs that fits in
   * a batch, and its key becomes busy. Without a batch to take, returns the next deadline, if any lane is waiting for
   * one. Nothing is taken once admission is closed.
   */
  def take(worker: Int, now: Long): Either[Option[Long], Step[K, X, (K, NonEmptyChunk[Item[X]])]] =
    require(!batches.contains(worker), s"worker $worker already holds a batch")
    if !open then Left(None)
    else
      val (promoted, dues) = promote(now)
      promoted.ready.dequeueOption match
        case None              => Left(promoted.waiting.headOption.map(_._1))
        case Some((key, line)) =>
          val lane                       = promoted.lanes(key)
          val (batch, remaining, weight) = lane.firstBatch(limits)
          val taken                      = promoted
            .copy(
              ready = line,
              lanes = promoted.lanes - key,
              busy = promoted.busy.updated(key, promoted.busy.getOrElse(key, 0) + 1),
              batches = promoted.batches.updated(worker, Batch(key, batch)),
              queued = promoted.queued - batch.size,
              weight = promoted.weight - weight
            )
            .place(key, remaining, lane.weight - weight)
          Right(Step((key, batch), taken, dues :+ Action.Take(worker, key, batch.map(_.caller).toChunk)))

  /**
   * `worker`'s batch call is over, with `outcome` for each of its callers: every caller of the batch still pending is
   * answered, and the key is freed. Returns the callers to answer; `None` if `worker` holds no batch.
   */
  def complete(worker: Int, outcome: Long => Outcome): Option[Step[K, X, Chunk[(Long, X)]]] =
    batches.get(worker).map { case Batch(key, batch) =>
      val outcomes = batch.toChunk.map(item => item.caller -> outcome(item.caller))
      val answered = outcomes.flatMap((caller, _) => pending.get(caller).map(caller -> _))
      Step(
        answered,
        copy(pending = pending -- batch.map(_.caller), batches = batches - worker).free(key),
        Chunk.single(Action.Complete(worker, outcomes))
      )
    }

  /** A direct caller's call is over: the caller is answered if still pending, and the key is freed. */
  def completeDirect(caller: Long, outcome: Outcome): Option[Step[K, X, Option[X]]] =
    direct.get(caller).map { key =>
      Step(
        pending.get(caller),
        copy(pending = pending - caller, direct = direct - caller).free(key),
        Chunk.single(Action.CompleteDirect(caller, outcome))
      )
    }

  /**
   * A pending caller gives up: it is answered "interrupted" and, if its input is still waiting in a lane, the input
   * leaves the lane at once. An input already sent stays in its call. `None` if the caller is not pending.
   */
  def cancel(caller: Long): Option[Step[K, X, X]] =
    pending.get(caller).map { value =>
      val left = lanes.collectFirst {
        case (key, lane) if lane.items.exists(_.caller == caller) =>
          val (gone, kept) = lane.items.partition(_.caller == caller)
          copy(queued = queued - 1, weight = weight - gone.head.weight)
            .place(key, kept, lane.weight - gone.head.weight)
      }
      Step(value, left.getOrElse(this).copy(pending = pending - caller), Chunk.single(Action.Cancel(caller)))
    }

  /** Admission closes, and every input still waiting leaves its lane; its caller is to be interrupted. */
  def close: Option[Step[K, X, Chunk[X]]] =
    Option.when(open) {
      val drained = Chunk.fromIterable(lanes.values.flatMap(_.items)).sortBy(_.caller)
      Step(
        drained.map(_.value),
        copy(
          open = false,
          pending = pending -- drained.map(_.caller),
          lanes = Map.empty,
          ready = Queue.empty,
          waiting = TreeMap.empty,
          queued = 0,
          weight = 0L
        ),
        Chunk.single(Action.Close(drained.map(_.caller)))
      )
    }

  /** Calls in flight: batches and direct calls. */
  def inFlight: Int = batches.size + direct.size

  /** No inputs waiting and no call in flight for `key`. */
  def idle(key: K): Boolean = !lanes.contains(key) && !busy.contains(key)

  private def blocked(key: K): Boolean = limits.oneCallPerKey && busy.contains(key)

  /** One call for `key` is over; a lane blocked behind it goes back in line once the key is free. */
  private def free(key: K): Protocol[K, X] =
    val left  = busy.getOrElse(key, 0) - 1
    val freed = copy(busy = if left <= 0 then busy - key else busy.updated(key, left))
    freed.lanes.get(key) match
      case Some(lane) if lane.schedule == Schedule.Blocked && !freed.blocked(key) =>
        freed.place(key, lane.items, lane.weight)
      case _ => freed

  /**
   * Stores `key`'s lane as `items` and schedules it, from its previous state:
   *   - away, if it holds nothing;
   *   - blocked, while its key has a call in flight and allows only one;
   *   - still ready, in the same place in line, if it was ready;
   *   - still waiting for the same deadline, if it was waiting and is not full;
   *   - ready at the back of the line, if it is full;
   *   - otherwise waiting for its oldest input's deadline.
   */
  private def place(key: K, items: Vector[Item[X]], weight: Long): Protocol[K, X] =
    val before = lanes.get(key).map(_.schedule)
    val after  =
      if items.isEmpty then None
      else if blocked(key) then Some(Schedule.Blocked)
      else
        val full = items.size >= limits.maxBatchSize || weight >= limits.maxBatchWeight
        before match
          case Some(Schedule.Ready)                     => Some(Schedule.Ready)
          case Some(waiting: Schedule.Waiting) if !full => Some(waiting)
          case _ if full                                => Some(Schedule.Ready)
          case _                                        =>
            Some(Schedule.Waiting(saturatingAdd(items.iterator.map(_.submittedAt).min, limits.maxBatchDelayNanos)))
    val unscheduled = before match
      case Some(waiting @ Schedule.Waiting(deadline)) if !after.contains(waiting) => unwait(key, deadline)
      case Some(Schedule.Ready) if !after.contains(Schedule.Ready) => copy(ready = ready.filterNot(_ == key))
      case _                                                       => this
    after match
      case None           => unscheduled.copy(lanes = unscheduled.lanes - key)
      case Some(schedule) =>
        val stored = unscheduled.copy(lanes = unscheduled.lanes.updated(key, Lane(items, weight, schedule)))
        (before, schedule) match
          case (Some(previous), now) if previous == now => stored
          case (_, Schedule.Ready)                      => stored.copy(ready = stored.ready :+ key)
          case (_, Schedule.Waiting(deadline))          =>
            stored.copy(waiting =
              stored.waiting.updated(deadline, stored.waiting.getOrElse(deadline, Vector.empty) :+ key)
            )
          case (_, Schedule.Blocked) => stored

  private def unwait(key: K, deadline: Long): Protocol[K, X] =
    val others = waiting.getOrElse(deadline, Vector.empty).filterNot(_ == key)
    copy(waiting = if others.isEmpty then waiting - deadline else waiting.updated(deadline, others))

  /**
   * The deadline of `key`'s waiting lane has passed: the lane joins the ready line. `None` if the lane is not waiting.
   * [[take]] applies it to every lane due by its `now`; it is a step of its own because the passing of time is an event
   * of the environment, which exploring the protocol's states applies at any moment, and a replay applies when the
   * recorded run did.
   */
  def due(key: K): Option[Step[K, X, Unit]] =
    lanes.get(key).collect { case lane @ Lane(_, _, Schedule.Waiting(deadline)) =>
      Step(
        (),
        unwait(key, deadline).copy(
          lanes = lanes.updated(key, lane.copy(schedule = Schedule.Ready)),
          ready = ready :+ key
        ),
        Chunk.single(Action.Due(key))
      )
    }

  /** Every lane whose deadline is due by `now` joins the ready line, earliest deadline first. */
  private def promote(now: Long): (Protocol[K, X], Chunk[Action[K]]) =
    waiting.rangeTo(now).values.flatten.foldLeft((this, Chunk.empty[Action[K]])) { case ((p, actions), key) =>
      val s = p.due(key).get
      (s.next, actions ++ s.actions)
    }

object Protocol:

  /**
   * @param direct
   *   An input for an idle key is sent at once by its own caller, without a worker.
   * @param oneCallPerKey
   *   A key with a call in flight is never taken again until that call completes.
   */
  final case class Limits(
    maxBatchSize: Int,
    maxBatchWeight: Long,
    maxBatchDelayNanos: Long,
    maxQueueing: Int,
    maxQueuedWeight: Long,
    direct: Boolean,
    oneCallPerKey: Boolean
  )

  final case class Item[X](caller: Long, value: X, weight: Long, submittedAt: Long, rank: Long)

  enum Schedule:
    case Waiting(deadline: Long)
    case Ready
    case Blocked

  final case class Lane[X](items: Vector[Item[X]], weight: Long, schedule: Schedule):

    /** `items` with `item` after every item of a rank up to its own. Inputs mostly arrive in rank order. */
    def inserted(item: Item[X]): Vector[Item[X]] =
      val at = items.lastIndexWhere(_.rank <= item.rank) + 1
      if at == items.size then items :+ item else items.patch(at, Vector(item), 0)

    /** The first items that fit in one batch: always at least one, even if it alone exceeds the weight limit. */
    def firstBatch(limits: Limits): (NonEmptyChunk[Item[X]], Vector[Item[X]], Long) =
      @annotation.tailrec
      def loop(size: Int, weight: Long): (Int, Long) =
        if size < items.size && size < limits.maxBatchSize && items(size).weight <= limits.maxBatchWeight - weight then
          loop(size + 1, weight + items(size).weight)
        else (size, weight)
      val (size, weight) = loop(1, items.head.weight)
      val (batch, rest)  = items.splitAt(size)
      (NonEmptyChunk.fromIterable(batch.head, batch.tail), rest, weight)

  final case class Batch[K, X](key: K, items: NonEmptyChunk[Item[X]])

  /** How a submission was received; every caller gets a number, refused or not. */
  enum Admission:
    case Refused(caller: Long)
    case Rejected(caller: Long)
    case Queued(caller: Long)
    case Claimed(caller: Long)

  enum Outcome:
    case Succeeded, Failed, Interrupted

  /**
   * The protocol's steps, as recorded: one case per kind of step, with its parameters.
   */
  enum Action[+K]:
    case Enqueue(caller: Long, key: K, weight: Long, rank: Long)
    case Claim(caller: Long, key: K, weight: Long, rank: Long)
    case Reject(caller: Long, key: K, weight: Long, rank: Long)
    case Refuse(caller: Long, key: K, weight: Long, rank: Long)
    case Due(key: K)
    case Take(worker: Int, key: K, batch: Chunk[Long])
    case Complete(worker: Int, outcomes: Chunk[(Long, Outcome)])
    case CompleteDirect(caller: Long, outcome: Outcome)
    case Cancel(caller: Long)
    case Close(drained: Chunk[Long])

  /** The result of one step: what the shell must act on, the next state, and the actions the step took. */
  final case class Step[K, X, +R](result: R, next: Protocol[K, X], actions: Chunk[Action[K]])

  def empty[K, X](limits: Limits): Protocol[K, X] =
    Protocol(limits, true, 1L, Map.empty, Map.empty, Queue.empty, TreeMap.empty, Map.empty, Map.empty, Map.empty, 0, 0L)

  private def saturatingAdd(a: Long, b: Long): Long =
    if b > 0 && a > Long.MaxValue - b then Long.MaxValue else a + b
