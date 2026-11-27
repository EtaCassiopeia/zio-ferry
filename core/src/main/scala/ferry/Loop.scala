package ferry

import scala.collection.immutable.SortedSet
import scala.util.control.NonFatal

import zio.*

import ferry.Engine.{Entry, Rules}
import ferry.Protocol.{Admission, Outcome}

/**
 * The single-owner shell: the alternative to [[Engine]]'s one STM transaction per step.
 *
 * One fiber owns the protocol's state. Everything that changes it is a command in one queue: a caller arrives or gives
 * up, a call is over, the ferry closes. The owner takes commands one at a time, applies the step, records its actions,
 * and only then acts on what the step decided: it answers callers, starts calls, and waits for the next deadline. So
 * every step is atomic because nobody else can see the state, the trace is the order the owner processed commands in,
 * and nothing ever retries.
 *
 * Calls run on fibers of their own and report back with a command. Closing keeps [[Engine]]'s order: admission closes
 * first (which empties every lane), then batch calls are interrupted, then direct calls.
 */
private[ferry] object Loop:

  private enum Command[K, I, E, B, A]:
    case Submit(
      input: I,
      key: K,
      weight: Long,
      rank: Long,
      answer: Promise[Nothing, Exit[FerryError[E], A]],
      admitted: Promise[Nothing, Admission],
      over: Promise[Nothing, Unit],
      refs: FiberRefs
    )
    case Cancel(caller: Long)
    case Done(worker: Int, batch: NonEmptyChunk[Protocol.Item[Entry[I, E, A]]], exit: Exit[E, B], interrupted: Boolean)
    case DirectDone(caller: Long, input: I, exit: Exit[E, B], interrupted: Boolean)
    case Close(done: Promise[Nothing, Unit])
    case Metrics(reply: Promise[Nothing, Ferry.Metrics])
    case Tick()

  def make[R, E, K, I, B, A](
    limits: Protocol.Limits,
    workers: Int,
    key: I => K,
    weigh: I => Long,
    rank: I => Long,
    recorder: Recorder[K] = Recorder.off,
    rules: Rules = Rules.standard
  )(call: (K, NonEmptyChunk[I]) => ZIO[R, E, B])(extract: (I, B) => A): URIO[R & Scope, Ferry[I, E, A]] =
    for
      env   <- ZIO.environment[R]
      queue <- Queue.unbounded[Command[K, I, E, B, A]]
      live = Live(
        queue,
        recorder,
        rules,
        key,
        weigh,
        rank,
        (k: K, in: NonEmptyChunk[I]) => call(k, in).provideEnvironment(env),
        extract
      )
      // The owner outlives the scope: once admission has closed it refuses every late caller, as a closed protocol
      // does, and it goes away with the ferry when nothing refers to the queue any more.
      _ <- live.run(Owner(Protocol.empty(limits), SortedSet.from(1 to workers), Map.empty, Map.empty)).forkDaemon
      _ <- ZIO.addFinalizer(live.close)
    yield live

  /** The owner's state: the protocol, the workers without a batch, and the calls in flight. */
  final private case class Owner[K, X](
    p: Protocol[K, X],
    idle: SortedSet[Int],
    calls: Map[Int, Fiber[Nothing, Unit]],
    directs: Map[Long, (Fiber[Nothing, Unit], Promise[Nothing, Unit])],
    timer: Option[(Long, Fiber[Nothing, Unit])] = None,
    replies: Chunk[UIO[Unit]] = Chunk.empty
  )

  final private class Live[E, K, I, B, A](
    queue: Queue[Command[K, I, E, B, A]],
    recorder: Recorder[K],
    rules: Rules,
    key: I => K,
    weigh: I => Long,
    rank: I => Long,
    call: (K, NonEmptyChunk[I]) => IO[E, B],
    extract: (I, B) => A
  ) extends Ferry[I, E, A]:

    private type X      = Entry[I, E, A]
    private type Answer = Exit[FerryError[E], A]
    private type State  = Owner[K, X]
    private type Cmd    = Command[K, I, E, B, A]

    private val interruptedAnswer: Answer = Exit.interrupt(FiberId.None)

    private def record(actions: Chunk[Protocol.Action[K]]): UIO[Unit] = recorder.record(actions).commit

    override def apply(input: I): IO[FerryError[E], A] =
      ZIO.uninterruptibleMask { restore =>
        for
          k        <- ZIO.succeed(key(input))
          r        <- ZIO.succeed(rank(input))
          w        <- ZIO.succeed(weigh(input) max 0L)
          answer   <- Promise.make[Nothing, Answer]
          admitted <- Promise.make[Nothing, Admission]
          over     <- Promise.make[Nothing, Unit]
          refs     <- ZIO.getFiberRefs
          _        <- queue.offer(Command.Submit(input, k, w, r, answer, admitted, over, refs))
          // Every submission is answered, even after close; the wait cannot be interrupted, so neither is lost.
          admission <- admitted.await
          a         <- admission match
            case Admission.Refused(_)  => ZIO.interrupt
            case Admission.Rejected(_) => ZIO.fail(FerryError.Rejected)
            case Admission.Queued(c)   => await(c, answer, restore, ZIO.unit)
            // The direct call is the caller's own: a caller that gives up stops it, and waits until it is over.
            case Admission.Claimed(c) => await(c, answer, restore, over.await)
        yield a
      }

    private def await(
      caller: Long,
      answer: Promise[Nothing, Answer],
      restore: ZIO.InterruptibilityRestorer,
      stopped: UIO[Unit]
    ): IO[FerryError[E], A] =
      restore(answer.await).onInterrupt(queue.offer(Command.Cancel(caller)) *> stopped).flatMap(exit => exit)

    override def metrics: UIO[Ferry.Metrics] =
      Promise.make[Nothing, Ferry.Metrics].flatMap { reply =>
        queue.offer(Command.Metrics(reply)) *>
          reply.await
      }

    def close: UIO[Unit] =
      Promise.make[Nothing, Unit].flatMap(done => queue.offer(Command.Close(done)) *> done.await)

    /** The owner: one command at a time, forever. */
    /**
     * One command at a time. The owner always waits on the queue itself: a timed-out `take` can lose a command that was
     * handed to it at the moment it timed out. The next deadline is a timer fiber that sends a [[Command.Tick]].
     */
    def run(o: State): UIO[Nothing] =
      Clock.nanoTime.flatMap { now =>
        dispatch(o, now).flatMap((o, _) => arm(o, now)).flatMap(flush).flatMap { o =>
          queue.take.flatMap(cmd => Clock.nanoTime.flatMap(handle(o, cmd, _))).flatMap(run)
        }
      }

    /**
     * Tells waiting callers how they were admitted, once the step is fully applied: batches dispatched and the next
     * deadline armed. A caller that sees its admission sees a ferry that has already acted on it.
     */
    private def flush(o: State): UIO[State] =
      ZIO.collectAllDiscard(o.replies).as(o.copy(replies = Chunk.empty))

    /** Keeps one timer for the earliest deadline that matters: a waiting lane, while some worker is idle. */
    private def arm(o: State, now: Long): UIO[State] =
      val wanted = if o.idle.nonEmpty then o.p.waiting.headOption.map(_._1) else None
      if wanted == o.timer.map(_._1) then ZIO.succeed(o)
      else
        val cancel = o.timer.fold(ZIO.unit)(_._2.interrupt.unit)
        val start  = ZIO.foreach(wanted) { d =>
          (Clock.sleep(Duration.fromNanos((d - now) max 0L)) *> queue.offer(Command.Tick()).unit).fork.map(d -> _)
        }
        cancel *> start.map(t => o.copy(timer = t))

    /** Gives a batch to every idle worker that can take one. */
    private def dispatch(o: State, now: Long): UIO[(State, Boolean)] =
      o.idle.headOption match
        case None    => ZIO.succeed((o, false))
        case Some(w) =>
          rules.take(o.p, w, now) match
            case Left(_)  => ZIO.succeed((o, false))
            case Right(s) =>
              val (k, batch) = s.result
              record(s.actions) *> runBatch(w, k, batch).flatMap { fiber =>
                dispatch(o.copy(p = s.next, idle = o.idle - w, calls = o.calls.updated(w, fiber)), now)
                  .map(_._1 -> true)
              }

    private def handle(o: State, cmd: Cmd, now: Long): UIO[State] =
      cmd match
        case Command.Submit(input, k, w, r, answer, admitted, over, refs) =>
          val s     = rules.submit(o.p, k, Entry(input, answer), w, r, now)
          val reply = o.replies :+ admitted.succeed(s.result).unit
          record(s.actions) *> (s.result match
            case Admission.Claimed(c) =>
              runDirect(c, k, input, refs)
                .map(f => o.copy(p = s.next, directs = o.directs.updated(c, f -> over), replies = reply))
            case _ => ZIO.succeed(o.copy(p = s.next, replies = reply)))

        case Command.Cancel(c) =>
          val stop = o.directs.get(c).fold(ZIO.unit)((fiber, _) => fiber.interrupt.forkDaemon.unit)
          rules.cancel(o.p, c) match
            case Some(s) =>
              record(s.actions) *> s.result.answer.succeed(interruptedAnswer) *> stop.as(o.copy(p = s.next))
            case None => stop.as(o)

        case Command.Done(w, batch, exit, wasInterrupted) =>
          val answers = batch.map(item => item.caller -> answerOf(item.value.input, exit, wasInterrupted)).toMap
          val freed   = o.copy(idle = o.idle + w, calls = o.calls - w)
          rules.complete(o.p, w, c => outcomeOf(answers(c), wasInterrupted)) match
            case Some(s) =>
              record(s.actions) *>
                ZIO.foreachDiscard(s.result)((c, entry) => entry.answer.succeed(answers(c))).as(freed.copy(p = s.next))
            case None => ZIO.succeed(freed)

        case Command.DirectDone(c, input, exit, wasInterrupted) =>
          val result = answerOf(input, exit, wasInterrupted)
          val over   = o.directs.get(c).fold(ZIO.unit)(_._2.succeed(()).unit)
          val gone   = o.copy(directs = o.directs - c)
          rules.completeDirect(o.p, c, outcomeOf(result, wasInterrupted)) match
            case Some(s) =>
              record(s.actions) *> ZIO.foreachDiscard(s.result)(_.answer.succeed(result)) *> over.as(
                gone.copy(p = s.next)
              )
            case None => over.as(gone)

        case Command.Metrics(reply) => reply.succeed(Ferry.Metrics(o.p.queued, o.p.inFlight)).as(o)

        // A deadline has passed: the timer is spent, and `run` takes whatever is due.
        case Command.Tick() => ZIO.succeed(o.copy(timer = None))

        case Command.Close(done) =>
          val closed = rules.close(o.p) match
            case Some(s) =>
              record(s.actions) *>
                ZIO.foreachDiscard(s.result)(_.answer.succeed(interruptedAnswer)).as(o.copy(p = s.next))
            case None => ZIO.succeed(o)
          // Then batch calls are stopped, then direct calls; each reports back, and is processed, before `done`.
          closed
            .flatMap(o => ZIO.foreachDiscard(o.calls.values)(_.interrupt).as(o))
            .flatMap(settle(_, now))
            .flatMap(o => ZIO.foreachDiscard(o.directs.values)(_._1.interrupt).as(o))
            .flatMap(settle(_, now))
            .flatMap(flush)
            .flatMap(o => done.succeed(()).as(o))

    /** Processes every command already waiting, without blocking. */
    private def settle(o: State, now: Long): UIO[State] =
      queue.takeAll.flatMap(cmds => ZIO.foldLeft(cmds)(o)(handle(_, _, now)))

    /**
     * Starts a call on a fiber of its own, which always reports back. The fiber is forked uninterruptible, so that an
     * interruption before it starts cannot skip the report and strand the batch's callers; only the call itself is
     * interruptible.
     */
    private def start(call: IO[E, B], report: (Exit[E, B], Boolean) => Cmd): UIO[Fiber[Nothing, Unit]] =
      ZIO.uninterruptible(
        call.interruptible.exit.flatMap(exit => interrupted.flatMap(i => queue.offer(report(exit, i)).unit)).fork
      )

    private def runBatch(w: Int, k: K, batch: NonEmptyChunk[Protocol.Item[X]]): UIO[Fiber[Nothing, Unit]] =
      start(call(k, batch.map(_.value.input)), Command.Done(w, batch, _, _))

    /** A direct call runs with its caller's fiber-local values, as if the caller had made it. */
    private def runDirect(c: Long, k: K, input: I, refs: FiberRefs): UIO[Fiber[Nothing, Unit]] =
      start(ZIO.setFiberRefs(refs) *> call(k, NonEmptyChunk.single(input)), Command.DirectDone(c, input, _, _))

    private val interrupted: UIO[Boolean] = ZIO.descriptorWith(d => ZIO.succeed(d.interrupters.nonEmpty))

    private def answerOf(input: I, exit: Exit[E, B], wasInterrupted: Boolean): Answer =
      exit match
        case Exit.Success(b) =>
          try Exit.succeed(extract(input, b))
          catch case NonFatal(t) => Exit.die(t)
        case Exit.Failure(cause) if wasInterrupted && cause.isInterruptedOnly => interruptedAnswer
        case Exit.Failure(cause) => Exit.failCause(cause.map(FerryError.Failed(_)))

    private def outcomeOf(answer: Answer, wasInterrupted: Boolean): Outcome =
      answer match
        case Exit.Success(_)                                                  => Outcome.Succeeded
        case Exit.Failure(cause) if wasInterrupted && cause.isInterruptedOnly => Outcome.Interrupted
        case Exit.Failure(_)                                                  => Outcome.Failed
