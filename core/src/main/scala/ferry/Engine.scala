package ferry

import scala.util.control.NonFatal

import zio.*
import zio.stm.*

import ferry.Protocol.{Admission, Outcome}

/**
 * The ZIO shell around [[Protocol]], shared by [[Ferry.make]] and [[Ferry.perKey]].
 *
 * The rule it follows: '''every change to the protocol's state is one STM transaction that applies one protocol step,
 * answers the callers that step settles, and records the step's actions'''. Nothing else touches the state. The
 * application's functions (`call`, `key`, `weigh`, `rank`, `extract`) run outside transactions, which may retry.
 *
 * Fibers:
 *   - '''callers''' submit, then wait for their answer, a `TPromise`; a caller interrupted while it waits cancels;
 *   - '''workers''', one per batch slot, take a batch, run it and complete it, forever;
 *   - '''direct calls''', one per claimed idle key, run the caller's input on its own and complete it.
 *
 * Closing the scope closes admission first (which empties every lane), then interrupts the workers, then the direct
 * calls; an interrupted call completes with the outcome `Interrupted`.
 */
private[ferry] object Engine:

  final case class Entry[I, E, A](input: I, answer: TPromise[Nothing, Exit[FerryError[E], A]])

  /**
   * The steps the shell applies. The standard rules are the protocol's own; tests substitute deliberately wrong rules
   * to show that a drift between the code and the model is caught.
   */
  trait Rules:
    def submit[K, X](p: Protocol[K, X], key: K, value: X, weight: Long, rank: Long, now: Long) =
      p.submit(key, value, weight, rank, now)
    def take[K, X](p: Protocol[K, X], worker: Int, now: Long)                    = p.take(worker, now)
    def complete[K, X](p: Protocol[K, X], worker: Int, outcome: Long => Outcome) = p.complete(worker, outcome)
    def completeDirect[K, X](p: Protocol[K, X], caller: Long, outcome: Outcome)  = p.completeDirect(caller, outcome)
    def cancel[K, X](p: Protocol[K, X], caller: Long)                            = p.cancel(caller)
    def close[K, X](p: Protocol[K, X])                                           = p.close

  object Rules:
    val standard: Rules = new Rules {}

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
      state  <- TRef.make(Protocol.empty[K, Entry[I, E, A]](limits)).commit
      direct <- Scope.make
      // Finalizers run in reverse order: admission closes first, then the workers stop, then the direct calls.
      _   <- ZIO.addFinalizerExit(direct.close(_))
      env <- ZIO.environment[R]
      live = Live(
        state,
        direct,
        recorder,
        rules,
        key,
        weigh,
        rank,
        (k: K, inputs: NonEmptyChunk[I]) => call(k, inputs).provideEnvironment(env),
        extract
      )
      _ <- ZIO.foreachDiscard(1 to workers)(live.worker(_).forkScoped)
      _ <- ZIO.addFinalizer(live.close)
    yield live

  final private class Live[E, K, I, B, A](
    state: TRef[Protocol[K, Entry[I, E, A]]],
    directScope: Scope,
    recorder: Recorder[K],
    rules: Rules,
    key: I => K,
    weigh: I => Long,
    rank: I => Long,
    call: (K, NonEmptyChunk[I]) => IO[E, B],
    extract: (I, B) => A
  ) extends Ferry[I, E, A]:

    private type X       = Entry[I, E, A]
    private type Answer  = Exit[FerryError[E], A]
    private type Machine = Protocol[K, X]

    /** One protocol step, in the current transaction: the new state and the step's actions, together. */
    private def step[T](f: Machine => Protocol.Step[K, X, T]): USTM[T] =
      state.get.flatMap { p =>
        val s = f(p)
        state.set(s.next) *> recorder.record(s.actions).as(s.result)
      }

    /** A step that may not be enabled: nothing changes and nothing is recorded if it is not. */
    private def stepIfEnabled[T](f: Machine => Option[Protocol.Step[K, X, T]]): USTM[Option[T]] =
      state.get.flatMap { p =>
        f(p) match
          case Some(s) => state.set(s.next) *> recorder.record(s.actions).as(Some(s.result))
          case None    => STM.none
      }

    private val interruptedAnswer: Answer = Exit.interrupt(FiberId.None)

    override def apply(input: I): IO[FerryError[E], A] =
      ZIO.uninterruptibleMask { restore =>
        for
          k      <- ZIO.succeed(key(input))
          r      <- ZIO.succeed(rank(input))
          w      <- ZIO.succeed(weigh(input) max 0L)
          answer <- TPromise.make[Nothing, Answer].commit
          now    <- Clock.nanoTime
          a      <- step(rules.submit(_, k, Entry(input, answer), w, r, now)).commit.flatMap {
            case Admission.Refused(_)  => ZIO.interrupt
            case Admission.Rejected(_) => ZIO.fail(FerryError.Rejected)
            case Admission.Queued(c)   => await(c, answer, restore, ZIO.unit)
            case Admission.Claimed(c)  =>
              // The direct call is the caller's own: a caller that gives up stops it, and waits until it is over.
              sendDirect(c, k, input, answer)
                .forkIn(directScope)
                .flatMap(call => await(c, answer, restore, call.interrupt.unit))
          }
        yield a
      }

    private def await(
      caller: Long,
      answer: TPromise[Nothing, Answer],
      restore: ZIO.InterruptibilityRestorer,
      stop: UIO[Unit]
    ): IO[FerryError[E], A] =
      restore(answer.await.commit).onInterrupt(cancel(caller) *> stop).flatMap(exit => exit)

    private def cancel(caller: Long): UIO[Unit] =
      stepIfEnabled(rules.cancel(_, caller)).flatMap {
        case Some(entry) => entry.answer.succeed(interruptedAnswer).unit
        case None        => STM.unit
      }.commit

    override def metrics: UIO[Ferry.Metrics] =
      state.get.map(p => Ferry.Metrics(p.queued, p.inFlight)).commit

    def close: UIO[Unit] =
      stepIfEnabled(rules.close).flatMap {
        case Some(drained) => STM.foreachDiscard(drained)(_.answer.succeed(interruptedAnswer))
        case None          => STM.unit
      }.commit

    /**
     * Sends a claimed idle key's input on its own, and completes it. Forked uninterruptible: only the call is not. The
     * call is interrupted when the scope closes, or when its caller gives up.
     */
    private def sendDirect(caller: Long, k: K, input: I, answer: TPromise[Nothing, Answer]): UIO[Unit] =
      // Forked from the caller's uninterruptible region, whose status it inherits: only the call is made interruptible.
      call(k, NonEmptyChunk.single(input)).interruptible.exit.flatMap { exit =>
        interrupted.flatMap { wasInterrupted =>
          val result = answerOf(input, exit, wasInterrupted)
          stepIfEnabled(rules.completeDirect(_, caller, outcomeOf(result, wasInterrupted))).flatMap {
            case Some(Some(_)) => answer.succeed(result).unit
            case _             => STM.unit
          }.commit
        }
      }

    /** Takes and runs one batch at a time, forever. Only waiting and the call itself are interruptible. */
    def worker(id: Int): UIO[Unit] =
      ZIO.uninterruptibleMask { restore =>
        take(id, restore).flatMap { (k, batch) =>
          restore(call(k, batch.map(_.value.input))).exit.flatMap(complete(id, batch, _))
        }
      }.forever

    private def take(
      id: Int,
      restore: ZIO.InterruptibilityRestorer
    ): UIO[(K, NonEmptyChunk[Protocol.Item[X]])] =
      Clock.nanoTime.flatMap { now =>
        state.get
          .flatMap { p =>
            rules.take(p, id, now) match
              case Right(s)       => state.set(s.next) *> recorder.record(s.actions).as(Right(s.result))
              case Left(deadline) => STM.succeed(Left(deadline -> p))
          }
          .commit
          .flatMap {
            case Right(batch)           => ZIO.succeed(batch)
            case Left((deadline, seen)) =>
              // Wait until the state changes or the earliest waiting lane falls due, whichever comes first.
              val changed = state.get.retryUntil(_ ne seen).commit.unit
              restore(deadline.fold(changed)(d => changed.timeout(Duration.fromNanos(d - now)).unit)) *>
                take(id, restore)
          }
      }

    private def complete(id: Int, batch: NonEmptyChunk[Protocol.Item[X]], exit: Exit[E, B]): UIO[Unit] =
      interrupted.flatMap { wasInterrupted =>
        val answers = batch.map(item => item.caller -> answerOf(item.value.input, exit, wasInterrupted)).toMap
        stepIfEnabled(rules.complete(_, id, caller => outcomeOf(answers(caller), wasInterrupted))).flatMap {
          case Some(answered) => STM.foreachDiscard(answered)((caller, entry) => entry.answer.succeed(answers(caller)))
          case None           => STM.unit
        }.commit
      }

    private val interrupted: UIO[Boolean] = ZIO.descriptorWith(d => ZIO.succeed(d.interrupters.nonEmpty))

    /** One caller's answer: its share of the result, the call's failure, or an interruption of the call by close. */
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
