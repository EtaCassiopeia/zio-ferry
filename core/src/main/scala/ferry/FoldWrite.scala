package ferry

import zio.*

/**
 * A batch call for inputs that must be applied in order to one key's state, in a store with conditional writes: read
 * the state once, apply the batch's inputs to it in order, in memory, and write the result once, only if the store
 * still holds the state that was read. A failed read or write, a conflict included, retries the whole round trip.
 *
 * Each input gets its own outcome, computed when it is applied, from the state at that point: an input that changes how
 * later ones apply, such as one closing a period, does so inside the fold. An input rejected on its own merits fails no
 * other. A store that writes the result in two steps, a commit and a projection, uses [[ProjectedWrite]]. `step` sees
 * the state as read, which includes inputs that an earlier attempt already applied (a write can land and its response
 * be lost): it should answer such an input without applying it again, so that retrying is safe (see
 * `docs/decisions/0005`).
 *
 * Use it as the call and the extract of [[Ferry.perKey]]:
 * {{{
 * val write = FoldWrite(id = (e: Entry) => e.id, read = store.read, step = apply, write = store.writeIf, retry = policy)
 * Ferry.perKey(config, (e: Entry) => e.entity)(write.call)(write.outcome)
 * }}}
 *
 * @param id
 *   Identifies an input. Inputs of one batch with the same id are applied once, and share the first one's outcome.
 * @param read
 *   Reads a key's state, including whatever makes the write conditional, such as a version.
 * @param step
 *   Applies one input to the state: the next state and the input's outcome. It must be pure; it runs again on retry.
 * @param write
 *   Writes `after` for `key` if the store still holds `before`, and fails otherwise. Called only when the batch changed
 *   the state (`after != before`).
 * @param retry
 *   When to retry the round trip after `read` or `write` failed.
 */
final case class FoldWrite[-R, E, K, I, Id, S, O](
  id: I => Id,
  read: K => ZIO[R, E, S],
  step: (S, I) => (S, O),
  write: (K, S, S) => ZIO[R, E, Any],
  retry: Schedule[R, E, Any]
):

  /** The batch call: one outcome per distinct input id. */
  def call(key: K, inputs: NonEmptyChunk[I]): ZIO[R, E, Map[Id, O]] = roundTrip(key, inputs).map(_._2)

  /** One input's share of the batch result. */
  def outcome(input: I, outcomes: Map[Id, O]): O = outcomes(id(input))

  /** The retried round trip, with the state it left: the state the last attempt wrote, or read if nothing changed. */
  private[ferry] def roundTrip(key: K, inputs: NonEmptyChunk[I]): ZIO[R, E, (S, Map[Id, O])] =
    val once =
      for
        before <- read(key)
        (after, outcomes) = apply(before, inputs)
        _ <- write(key, before, after).unless(after == before)
      yield (after, outcomes)
    once.retry(retry)

  private def apply(state: S, inputs: NonEmptyChunk[I]): (S, Map[Id, O]) =
    inputs.foldLeft((state, Map.empty[Id, O])) { case ((state, outcomes), input) =>
      val key = id(input)
      if outcomes.contains(key) then (state, outcomes)
      else
        val (next, outcome) = step(state, input)
        (next, outcomes.updated(key, outcome))
    }
