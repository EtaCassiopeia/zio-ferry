package ferry

import zio.*

/**
 * Coalesces individual calls into batched calls to an external system, and limits the load put on it.
 *
 * Each input submitted through `apply` is queued. Queued inputs are dispatched together in a single batch call when
 * `maxBatchSize` inputs have accumulated, or at the latest `maxBatchDelay` after the oldest of them was submitted. At
 * most `maxInFlightBatches` batch calls run at the same time, and at most `maxQueueing` inputs wait for a batch: beyond
 * that, calls are rejected immediately rather than piling up.
 *
 * Every caller receives its own share of the batch result, or the batch call's failure.
 */
trait Ferry[-I, +E, +A]:

  /**
   * Submits `input` for inclusion in the next batch and waits for its result.
   *
   * Fails with [[FerryError.Rejected]] when the queue is full, or with [[FerryError.Failed]] when the batch call
   * carrying `input` fails. A defect in the batch call is propagated as a defect. Interrupting the caller before its
   * batch is dispatched removes `input` from the batch. Calls made after the Ferry's scope has closed are interrupted.
   */
  def apply(input: I): IO[FerryError[E], A]

  /** Current load, for monitoring. */
  def metrics: UIO[Ferry.Metrics]

object Ferry:

  /**
   * @param queued
   *   Inputs waiting in the queue. Inputs already taken into the batch being assembled are not counted.
   * @param inFlightBatches
   *   Batch calls currently running, including a `perKey` ferry's direct calls for idle keys.
   */
  final case class Metrics(queued: Int, inFlightBatches: Int)

  /**
   * Creates a Ferry whose lifetime is bound to the enclosing `Scope`.
   *
   * @param call
   *   The batch call. It receives the inputs in submission order, including duplicates.
   * @param extract
   *   Picks one caller's result out of the batch result. If it throws, that caller gets the exception as a defect.
   */
  def make[R, E, I, B, A](config: FerryConfig)(call: NonEmptyChunk[I] => ZIO[R, E, B])(
    extract: (I, B) => A
  ): URIO[R & Scope, Ferry[I, E, A]] =
    for
      queue     <- Queue.dropping[Entry[I, E, A]](config.maxQueueing)
      doorbell  <- Queue.sliding[Unit](1)
      accepting <- Ref.Synchronized.make(true)
      inFlight  <- Ref.make(0)
      forming   <- Semaphore.make(1)
      ferry = Live(config, call, extract, queue, doorbell, accepting, inFlight, forming)
      // Finalizers run in reverse order: workers are interrupted first, then whatever is still queued is released.
      _ <- ZIO.addFinalizer(ferry.close)
      _ <- ZIO.foreachDiscard(1 to config.maxInFlightBatches)(_ => ferry.worker.forkScoped)
    yield ferry

  /**
   * Creates a Ferry for the common case of a batch call answering with one result per distinct input.
   *
   * The batch call receives each distinct input once. A caller whose input is absent from the returned map gets `None`.
   */
  def keyed[R, E, I, A](config: FerryConfig)(
    call: NonEmptyChunk[I] => ZIO[R, E, Map[I, A]]
  ): URIO[R & Scope, Ferry[I, E, Option[A]]] =
    make(config)((inputs: NonEmptyChunk[I]) => call(distinct(inputs)))((input, results) => results.get(input))

  /**
   * Creates a Ferry that batches each key's inputs separately, and keeps each key's batches in order.
   *
   * Inputs with the same `key` are dispatched in `order` (by default, submission order), in batches of that key only,
   * and a key never has more than one batch in flight: its next batch is dispatched only once the previous one has
   * completed. Different keys are batched independently and run concurrently, up to `maxInFlightBatches` in total. Keys
   * with work waiting take turns, so a key that always has more never starves the others.
   *
   * Only keys that are busy are batched. With the default `maxBatchDelay` of zero, an input for an idle key (nothing
   * waiting, nothing in flight) is sent at once by its own caller, as a batch of one, without queueing or waiting for a
   * worker: occasional traffic pays no batching cost and never waits behind busy keys. Inputs that arrive for the key
   * while that call is in flight are batched behind it, so a key switches to batching exactly when it has concurrent
   * work, and back when it is idle again. These direct calls are not counted against `maxInFlightBatches`, which bounds
   * the batches of busy keys; at most one runs per idle key. `PerKeyConfig.directIdleKeys` turns them off.
   *
   * This suits writes that must be applied in order per entity, such as appends to a per-entity log guarded by a
   * conditional write: one writer per entity removes write contention, and each round trip carries everything that
   * arrived while the previous one was in flight.
   *
   * To report outcomes per input, let `call` answer with one outcome per input and fail only when the whole batch did
   * not happen. An input rejected on its own merits then affects only its own caller.
   *
   * @param key
   *   The entity an input belongs to.
   * @param weigh
   *   The weight of an input, such as its size in bytes, for `maxBatchWeight` and `maxQueuedWeight`. Defaults to 1.
   * @param order
   *   The order in which a key's waiting inputs are sent: see [[Order]]. Defaults to submission order.
   * @param call
   *   The batch call, given the key and that key's inputs in `order`. [[FoldWrite]] implements the common read-once,
   *   apply, conditional-write call, and [[ProjectedWrite]] the same with a second write that projects the result.
   * @param extract
   *   Picks one caller's result out of the batch result. If it throws, that caller gets the exception as a defect.
   */
  def perKey[R, E, K, I, B, A](
    config: PerKeyConfig,
    key: I => K,
    weigh: I => Long = (_: Any) => 1L,
    order: Order[I] = Order.arrival
  )(
    call: (K, NonEmptyChunk[I]) => ZIO[R, E, B]
  )(extract: (I, B) => A): URIO[R & Scope, Ferry[I, E, A]] =
    PerKeyFerry.make(config, key, weigh, order)(call)(extract)

  private def distinct[I](inputs: NonEmptyChunk[I]): NonEmptyChunk[I] =
    NonEmptyChunk.fromIterable(inputs.head, inputs.tail.distinct.filterNot(_ == inputs.head))

  final private case class Entry[I, E, A](input: I, result: Promise[FerryError[E], A], submittedAt: Long)

  private enum Admission:
    case Admitted, QueueFull, Closed

  final private class Live[R, E, I, B, A](
    config: FerryConfig,
    call: NonEmptyChunk[I] => ZIO[R, E, B],
    extract: (I, B) => A,
    queue: Queue[Entry[I, E, A]],
    doorbell: Queue[Unit],
    accepting: Ref.Synchronized[Boolean],
    inFlight: Ref[Int],
    forming: Semaphore
  ) extends Ferry[I, E, A]:

    override def apply(input: I): IO[FerryError[E], A] =
      ZIO.uninterruptibleMask { restore =>
        for
          result      <- Promise.make[FerryError[E], A]
          submittedAt <- Clock.nanoTime
          _           <- enqueue(Entry(input, result, submittedAt))
          a           <- restore(result.await).onInterrupt(result.interrupt)
        yield a
      }

    override def metrics: UIO[Metrics] =
      queue.size.zipWith(inFlight.get)(Metrics(_, _))

    // Admission is serialised with `close` so nothing can enter the queue after it has been drained.
    private def enqueue(entry: Entry[I, E, A]): IO[FerryError[E], Unit] =
      accepting
        .modifyZIO { isAccepting =>
          if isAccepting then
            queue.offer(entry).map(offered => (if offered then Admission.Admitted else Admission.QueueFull) -> true)
          else ZIO.succeed(Admission.Closed -> false)
        }
        .flatMap {
          case Admission.Admitted  => doorbell.offer(()).unit
          case Admission.QueueFull => ZIO.fail(FerryError.Rejected)
          case Admission.Closed    => ZIO.interrupt
        }

    def close: UIO[Unit] =
      accepting.set(false) *> queue.takeAll.flatMap(ZIO.foreachDiscard(_)(_.result.interrupt))

    /**
     * Assembles and runs one batch at a time, forever. One worker runs per batch slot; `forming` lets only one of them
     * assemble a batch at a time, so a worker that is busy running a batch never splits the next one.
     */
    def worker: URIO[R, Nothing] =
      Ref
        .make(Chunk.empty[Entry[I, E, A]])
        .flatMap { taken =>
          (forming.withPermit(assemble(taken)) flatMap run)
            // An entry taken off the queue is owned by this worker until its result is set; never strand a caller.
            .onInterrupt(taken.get.flatMap(ZIO.foreachDiscard(_)(_.result.interrupt)))
        }
        .forever

    private def assemble(taken: Ref[Chunk[Entry[I, E, A]]]): UIO[NonEmptyChunk[Entry[I, E, A]]] =
      ZIO
        .uninterruptibleMask(restore => restore(queue.take).tap(first => taken.set(Chunk.single(first))))
        .flatMap(first => fill(NonEmptyChunk.single(first), first.submittedAt + config.maxBatchDelay.toNanos, taken))

    private def fill(
      batch: NonEmptyChunk[Entry[I, E, A]],
      deadline: Long,
      taken: Ref[Chunk[Entry[I, E, A]]]
    ): UIO[NonEmptyChunk[Entry[I, E, A]]] =
      queue
        .takeUpTo(config.maxBatchSize - batch.size)
        .tap(more => taken.update(_ ++ more))
        .uninterruptible
        .flatMap { more =>
          val grown = batch ++ more
          if grown.size >= config.maxBatchSize then ZIO.succeed(grown)
          else
            Clock.nanoTime.flatMap { now =>
              if now >= deadline then ZIO.succeed(grown)
              // The doorbell rings on every admission; a stale ring only costs one extra empty `takeUpTo`.
              else doorbell.take.timeout(Duration.fromNanos(deadline - now)) *> fill(grown, deadline, taken)
            }
        }

    private def run(batch: NonEmptyChunk[Entry[I, E, A]]): URIO[R, Unit] =
      // Callers interrupted while their input was waiting have already completed their promise.
      ZIO.filterNot(batch)(_.result.isDone).map(NonEmptyChunk.fromChunk).flatMap {
        case None          => ZIO.unit
        case Some(entries) =>
          ZIO.acquireReleaseWith(inFlight.update(_ + 1))(_ => inFlight.update(_ - 1)) { _ =>
            call(entries.map(_.input)).exit.flatMap {
              case Exit.Success(b) =>
                ZIO.foreachDiscard(entries)(entry => entry.result.complete(ZIO.succeed(extract(entry.input, b))))
              case Exit.Failure(cause) =>
                ZIO.foreachDiscard(entries)(_.result.failCause(cause.map(FerryError.Failed(_))))
            }
          }
      }
