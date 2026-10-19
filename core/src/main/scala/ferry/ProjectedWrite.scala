package ferry

import zio.*

/**
 * A batch call for a store that writes a key's record in two dependent conditional writes: commit the folded state
 * (`fold.write`, fenced on the state read), then project it into its read-side view (`project`, fenced on the committed
 * record being as committed). The record says whether its projection is pending (`pending`), so a projection that did
 * not happen, because the process failed or died between the writes, is finished later by [[resume]], from the store,
 * by any process. Nothing is kept between calls: the marker lives in the store (see `docs/decisions/0014`).
 *
 * Every outcome of one call is [[ProjectedWrite.Outcome.Projected]] if the view is known to include the call's inputs
 * when it returns: its own projection landed, or a re-read found nothing pending. Otherwise it is
 * [[ProjectedWrite.Outcome.Committed]]: the inputs are durable, and the view may or may not include them yet. The call
 * fails only when the commit is not known to have happened, and retrying it is then safe, because `step` answers inputs
 * already applied; a failed projection is reported, not failed, because the inputs are durable (see
 * `docs/decisions/0006`).
 *
 * Use it as the call and the extract of [[Ferry.perKey]], like [[FoldWrite]]:
 * {{{
 * val write = ProjectedWrite(FoldWrite(_.id, store.read, apply, store.commit, policy), pending, store.project)
 * Ferry.perKey(config, (e: Entry) => e.entity)(write.call)(write.outcome)
 * }}}
 *
 * @param fold
 *   The commit phase: `read` reads the record, including its fence and its projection marker; `step` returns the state
 *   the commit stores, for which `pending` holds exactly when there is something to project; `write` is the commit. The
 *   commit must never move the stored marker backwards: either it leaves the marker as stored, or it is fenced on the
 *   whole record read, marker included. `retry` applies to the commit, and to [[resume]] after a first projection
 *   attempt failed; that first attempt's error is not passed to it.
 * @param pending
 *   Whether a state, as read or as committed, has a projection to write. It must be pure.
 * @param project
 *   Writes the view of `state` and clears its marker, only if the store still holds `state`, fence and marker alike,
 *   and fails otherwise. The marker alone refuses a second projection of one record; the fence keeps a projection of a
 *   stale record from clearing the marker of a later commit.
 */
final case class ProjectedWrite[-R, E, K, I, Id, S, O](
  fold: FoldWrite[R, E, K, I, Id, S, O],
  pending: S => Boolean,
  project: (K, S) => ZIO[R, E, Any]
):
  import ProjectedWrite.Outcome

  /** The batch call: one outcome per distinct input id. Fails only when the commit is not known to have happened. */
  def call(key: K, inputs: NonEmptyChunk[I]): ZIO[R, E, Map[Id, Outcome[O]]] =
    fold.roundTrip(key, inputs).flatMap { (after, outcomes) =>
      projected(key, after).map(known => outcomes.map((id, o) => id -> Outcome(known, o)))
    }

  /** One input's share of the batch result. */
  def outcome(input: I, outcomes: Map[Id, Outcome[O]]): Outcome[O] = outcomes(fold.id(input))

  /**
   * Projects the key's record if its projection is pending, and does nothing otherwise; retried with `fold.retry`. Safe
   * to run anywhere, at any time, concurrently with the key's batches and with other calls of `resume`: the fence on
   * `project` lets at most one projection of a record land. For sweepers, which find pending keys with a query of the
   * store's own.
   */
  def resume(key: K): ZIO[R, E, Unit] =
    fold.read(key).flatMap(state => project(key, state).when(pending(state))).unit.retry(fold.retry)

  /**
   * Whether the view is known to include the state just committed. The first attempt projects that state without a
   * read, since it is what the store holds; any failure falls back to [[resume]]. A failure of both is an answer, not
   * an error: the inputs are already durable.
   */
  private def projected(key: K, committed: S): URIO[R, Boolean] =
    project(key, committed).when(pending(committed)).unit.orElse(resume(key)).isSuccess

object ProjectedWrite:

  /**
   * Where an input stands when its call returns. Both are durable; they differ in whether the view is known to have it.
   */
  enum Outcome[+O]:

    /**
     * Committed, projection not confirmed: `provisional` is the answer as of the commit. The view may already include
     * it; if not, [[ProjectedWrite.resume]], run by a sweeper, or the key's next batch finishes it.
     */
    case Committed(provisional: O)

    /** Committed, and the view is known to include it: `result` is the final answer. */
    case Projected(result: O)

    /** The answer, final or provisional. */
    def value: O = this match
      case Committed(o) => o
      case Projected(o) => o

    def isProjected: Boolean = this match
      case Projected(_) => true
      case Committed(_) => false

  object Outcome:

    /** `Projected(o)` if the view is known to include it, `Committed(o)` otherwise. */
    def apply[O](projected: Boolean, o: O): Outcome[O] = if projected then Projected(o) else Committed(o)
