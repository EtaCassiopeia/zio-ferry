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
    Engine.make(makeLimits(config), config.maxInFlightBatches, (_: I) => (), (_: I) => 1L, (_: I) => 0L)(
      (_: Unit, inputs: NonEmptyChunk[I]) => call(inputs)
    )(extract)

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
    Engine.make(perKeyLimits(config), config.maxInFlightBatches, key, weigh, order.rank)(call)(extract)

  /**
   * `Ferry.make` is the protocol for a single key, whose batches may run concurrently and are never called directly.
   */
  private[ferry] def makeLimits(config: FerryConfig): Protocol.Limits =
    Protocol.Limits(
      maxBatchSize = config.maxBatchSize,
      maxBatchWeight = Long.MaxValue,
      maxBatchDelayNanos = config.maxBatchDelay.toNanos,
      maxQueueing = config.maxQueueing,
      maxQueuedWeight = Long.MaxValue,
      direct = false,
      oneCallPerKey = false
    )

  private[ferry] def perKeyLimits(config: PerKeyConfig): Protocol.Limits =
    Protocol.Limits(
      maxBatchSize = config.maxBatchSize,
      maxBatchWeight = config.maxBatchWeight,
      maxBatchDelayNanos = config.maxBatchDelay.toNanos,
      maxQueueing = config.maxQueueing,
      maxQueuedWeight = config.maxQueuedWeight,
      direct = config.directIdleKeys && config.maxBatchDelay.toNanos == 0L,
      oneCallPerKey = true
    )

  private def distinct[I](inputs: NonEmptyChunk[I]): NonEmptyChunk[I] =
    NonEmptyChunk.fromIterable(inputs.head, inputs.tail.distinct.filterNot(_ == inputs.head))
