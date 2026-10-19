package ferry

import zio.*

/**
 * A batch call for a store that writes a key's record in two dependent conditional writes: commit the folded state
 * (`fold.write`, fenced on the state read), then project it into its read-side view (`project`, fenced on the committed
 * record being as committed). The record says whether its projection is pending (`pending`), so a projection that did
 * not happen, because the process failed or died between the writes, is finished later by [[resume]], from the store,
 * by any process. Nothing is kept between calls: the marker lives in the store (see `docs/decisions/0014`).
 *
 * The view may be one object per record, written whole by `project`, or one row per entry, each its own write (see
 * `docs/decisions/0015`). In either case `project` answers with what it left pending: nothing, or the entries whose row
 * is not in the view, each with why. An input whose row is in the view when the call returns is
 * [[ProjectedWrite.Outcome.Projected]]; any other is [[ProjectedWrite.Outcome.Committed]], carrying why: it is durable,
 * and the view may or may not include it yet. The call fails only when the commit is not known to have happened, and
 * retrying it is then safe, because `step` answers inputs already applied; a failed projection is reported, not failed,
 * because the inputs are durable (see `docs/decisions/0006`).
 *
 * Use it as the call and the extract of [[Ferry.perKey]], like [[FoldWrite]]:
 * {{{
 * val write = ProjectedWrite(FoldWrite(_.id, store.read, apply, store.commit, policy), pending, store.projectRows)
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
 *   Writes the view of `state` and clears its marker. For a view of one object: writes it only if the store still holds
 *   `state`, fence and marker alike, fails otherwise, and answers `Projection.complete`; see
 *   [[ProjectedWrite.wholeRecord]]. For a view of one row per entry: writes each missing row, each write idempotent (a
 *   row already present is present, not an error); clears the marker only when every row is present, and only if the
 *   store still holds `state`; answers the entries still pending, each with why. It derives rows and ids from `state`
 *   alone: [[resume]] has no inputs. A failure means no verdict: the call re-reads and retries it with `fold.retry`. A
 *   verdict with entries pending is final for the call; the store owns its per-row retries.
 */
final case class ProjectedWrite[-R, E, K, I, Id, S, O](
  fold: FoldWrite[R, E, K, I, Id, S, O],
  pending: S => Boolean,
  project: (K, S) => ZIO[R, E, ProjectedWrite.Projection[Id, E]]
):
  import ProjectedWrite.{Outcome, Projection}

  /** The batch call: one outcome per distinct input id. Fails only when the commit is not known to have happened. */
  def call(key: K, inputs: NonEmptyChunk[I]): ZIO[R, E, Map[Id, Outcome[E, O]]] =
    fold.roundTrip(key, inputs).flatMap { (after, outcomes) =>
      projected(key, after).map(verdict => outcomes.map((id, o) => id -> Outcome(verdict, id, o)))
    }

  /** One input's share of the batch result. */
  def outcome(input: I, outcomes: Map[Id, Outcome[E, O]]): Outcome[E, O] = outcomes(fold.id(input))

  /**
   * Projects the key's record if its projection is pending, and does nothing otherwise; retried with `fold.retry`.
   * Answers what is still pending: nothing, or the entries whose row is still missing, each with why. Safe to run
   * anywhere, at any time, concurrently with the key's batches and with other calls of `resume`: rows are idempotent
   * and the fence on the marker's clear refuses a stale projection. For sweepers, which find pending keys with a query
   * of the store's own.
   */
  def resume(key: K): ZIO[R, E, Projection[Id, E]] =
    fold.read(key).flatMap(projectIfPending(key, _)).retry(fold.retry)

  /**
   * What the view is known to include of the state just committed. The first attempt projects that state without a
   * read, since it is what the store holds; a failure falls back to [[resume]]. A failure of both is an answer, not an
   * error: `Left(e)` makes every input of the call `Committed(_, e)`, because the inputs are already durable.
   */
  private def projected(key: K, committed: S): URIO[R, Either[E, Projection[Id, E]]] =
    projectIfPending(key, committed).orElse(resume(key)).either

  private def projectIfPending(key: K, state: S): ZIO[R, E, Projection[Id, E]] =
    if pending(state) then project(key, state) else ZIO.succeed(Projection.complete)

object ProjectedWrite:

  /**
   * A `ProjectedWrite` for a store whose view is one object per record: `project` writes it and clears the marker, only
   * if the store still holds `state`, fence and marker alike, and fails otherwise. Nothing is ever left pending.
   */
  def wholeRecord[R, E, K, I, Id, S, O](
    fold: FoldWrite[R, E, K, I, Id, S, O],
    pending: S => Boolean,
    project: (K, S) => ZIO[R, E, Any]
  ): ProjectedWrite[R, E, K, I, Id, S, O] =
    ProjectedWrite(fold, pending, (key, state) => project(key, state).as(Projection.complete))

  /**
   * What a projection attempt left pending: for each entry whose row is not known to be in the view, why. Empty when
   * the view includes the whole record; the marker is then cleared.
   */
  final case class Projection[Id, +E](pending: Map[Id, E]):
    def isComplete: Boolean = pending.isEmpty

  object Projection:
    def complete[Id, E]: Projection[Id, E] = Projection(Map.empty)

  /**
   * Where an input stands when its call returns. Both are durable; they differ in whether the view is known to have it.
   */
  enum Outcome[+E, +O]:

    /**
     * Committed, projection of this input not confirmed: `provisional` is the answer as of the commit, `cause` why the
     * view is not known to include it: its own row's error, or the error the record's projection ended on. The view may
     * already include it; if not, [[ProjectedWrite.resume]], run by a sweeper, or the key's next batch finishes it.
     */
    case Committed(provisional: O, cause: E)

    /** Committed, and the view is known to include it: `result` is the final answer. */
    case Projected(result: O)

    /** The answer, final or provisional. */
    def value: O = this match
      case Committed(o, _) => o
      case Projected(o)    => o

    def isProjected: Boolean = this match
      case Projected(_)    => true
      case Committed(_, _) => false

  object Outcome:

    /** An input's outcome from the projection's verdict: its own row's cause, the call's, or none. */
    private[ferry] def apply[Id, E, O](verdict: Either[E, Projection[Id, E]], id: Id, o: O): Outcome[E, O] =
      verdict match
        case Left(e)                    => Committed(o, e)
        case Right(Projection(pending)) => pending.get(id).fold(Projected(o))(Committed(o, _))
