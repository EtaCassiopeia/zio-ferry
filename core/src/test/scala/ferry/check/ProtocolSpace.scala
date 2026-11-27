package ferry.check

import zio.Chunk

import ferry.{Engine, Protocol}
import ferry.Protocol.{Action, Admission, Outcome}
import ferry.testkit.check.StateSpace

/**
 * The protocol checked directly: the code is the model.
 *
 * Two uses share one set of steps and one set of properties:
 *   - '''exploration''' ([[check]]): every state of a small instance. The environment does whatever it may (callers
 *     arrive, deadlines pass, calls return with any outcome, callers give up, the ferry closes), and every step is
 *     applied by the real [[Protocol]] through [[Engine.Rules]], so a deliberately wrong rule set plugs in exactly as
 *     it does for the engine. Time is an event: a waiting lane may become due at any moment, and `take` never promotes
 *     a lane on its own;
 *   - '''replay''' ([[replay]]): a run recorded from the real, concurrent engine, step by step through the pure
 *     protocol. Each step must produce exactly the action the engine recorded, and the properties must hold after it.
 *
 * Alongside the protocol's state, both keep what an observer outside it would see: who was let in, in what order per
 * key, what was sent, who was answered. The properties are stated over those observations, not over the protocol's
 * internals, so a protocol that computes the wrong thing cannot also hide it.
 */
object ProtocolSpace:

  /** The bounds of one exploration: callers, keys, workers and limits of a small instance. */
  final case class Config(
    callers: List[String],
    keys: List[String],
    workers: List[Int],
    maxBatch: Int,
    maxWeight: Long,
    maxQueue: Int,
    maxQueuedWeight: Long,
    direct: Boolean,
    oneCallPerKey: Boolean,
    lowRank: Set[String] = Set.empty,
    heavy: Set[String] = Set.empty
  ):
    def rank(c: String): Long   = if lowRank(c) then 0 else 1
    def weight(c: String): Long = if heavy(c) then 2 else 1
    def limits: Protocol.Limits =
      Protocol.Limits(maxBatch, maxWeight, 1L, maxQueue, maxQueuedWeight, direct, oneCallPerKey)

  object Config:
    private val three = List("c1", "c2", "c3")

    /** `perKey` with direct calls, two keys, a low-rank and a heavy caller. */
    val perKey = Config(three, List("k1", "k2"), List(1, 2), 2, 3, 2, 4, true, true, Set("c2"), Set("c3"))

    /** `perKey`, every input batched. */
    val batchOnly = perKey.copy(direct = false)

    /** `make`: one key, batches of it may run at once. */
    val make = Config(three, List("k1"), List(1, 2), 2, 3, 2, 4, false, false)

    /** `perKey`, every input batched, with a weight limit that a light and a heavy input together exceed. */
    val weighted = batchOnly.copy(maxWeight = 2)

  enum Status:
    case Queued, Direct, Rejected, Refused

  /** The protocol, and what an observer has seen of it. */
  final case class State(
    p: Protocol[String, String],
    status: Map[String, Status] = Map.empty,
    rank: Map[String, Long] = Map.empty,
    answer: Map[String, Outcome] = Map.empty,
    ids: Map[Long, String] = Map.empty,
    admitted: Map[String, Vector[String]] = Map.empty,
    log: Map[String, Vector[String]] = Map.empty,
    gone: Map[String, Set[String]] = Map.empty,
    sent: Map[String, Int] = Map.empty,
    cancelledWaiting: Set[String] = Set.empty,
    readyOrder: Vector[String] = Vector.empty,
    unfair: Boolean = false
  ):
    def pending: Set[String] =
      status.collect { case (c, Status.Queued | Status.Direct) if !answer.contains(c) => c }.toSet

    def idOf(c: String): Option[Long] = ids.collectFirst { case (id, `c`) => id }

    def show: String =
      val lanes = p.lanes.toList.sortBy(_._1).map((k, l) => s"$k:${l.items.map(_.value).mkString("[", ",", "]")}")
      val calls = p.batches.toList.sortBy(_._1).map((w, b) => s"w$w:${b.items.map(_.value).toList.mkString(",")}") ++
        p.direct.toList.map((id, k) => s"${ids(id)}→$k")
      s"open=${p.open} lanes=${lanes.mkString(" ")} ready=${p.ready.mkString(",")} calls=${calls.mkString(" ")} " +
        s"answered=${answer.toList.sortBy(_._1).map((c, o) => s"$c:$o").mkString(",")}"

  def init(limits: Protocol.Limits): State = State(Protocol.empty(limits))

  /** One step: the actions the protocol took, and the state after it, observations included. */
  final case class Moved(actions: Chunk[Action[String]], next: State)

  /** The protocol's steps, applied through `rules`, each keeping the observations up to date. */
  final class Steps(rules: Engine.Rules):

    def submit(s: State, c: String, key: String, weight: Long, rank: Long): Moved =
      val step = rules.submit(s.p, key, c, weight, rank, 0L)
      val base = s.copy(p = step.next, rank = s.rank.updated(c, rank))
      val t    = step.result match
        case Admission.Refused(_)  => base.copy(status = base.status.updated(c, Status.Refused))
        case Admission.Rejected(_) => base.copy(status = base.status.updated(c, Status.Rejected))
        case Admission.Queued(id)  => admit(base, c, id, key, Status.Queued)
        case Admission.Claimed(id) => sent(key, List(c), admit(base, c, id, key, Status.Direct))
      Moved(step.actions, observe(s, t))

    def due(s: State, key: String): Option[Moved] =
      s.p.due(key).map(step => Moved(step.actions, observe(s, s.copy(p = step.next))))

    def take(s: State, worker: Int): Option[Moved] =
      Option.when(!s.p.batches.contains(worker))(rules.take(s.p, worker, Long.MinValue).toOption).flatten.map { step =>
        val (key, batch) = step.result
        Moved(step.actions, observe(s, sent(key, batch.map(i => s.ids(i.caller)).toList, s.copy(p = step.next))))
      }

    def complete(s: State, worker: Int, outcome: Long => Outcome): Option[Moved] =
      rules.complete(s.p, worker, outcome).map { step =>
        val answered = step.result.map((id, _) => s.ids(id) -> outcome(id))
        Moved(step.actions, observe(s, s.copy(p = step.next, answer = s.answer ++ answered)))
      }

    def completeDirect(s: State, id: Long, outcome: Outcome): Option[Moved] =
      rules.completeDirect(s.p, id, outcome).map { step =>
        val answered = step.result.map(_ => s.ids(id) -> outcome)
        Moved(step.actions, observe(s, s.copy(p = step.next, answer = s.answer ++ answered)))
      }

    def cancel(s: State, id: Long): Option[Moved] =
      rules.cancel(s.p, id).map { step =>
        val c       = s.ids(id)
        val waiting = s.p.lanes.values.exists(_.items.exists(_.caller == id))
        val t       = s.copy(
          p = step.next,
          answer = s.answer.updated(c, Outcome.Interrupted),
          cancelledWaiting = if waiting then s.cancelledWaiting + c else s.cancelledWaiting
        )
        Moved(step.actions, observe(s, t))
      }

    def close(s: State): Option[Moved] =
      rules.close(s.p).map { step =>
        Moved(
          step.actions,
          observe(s, s.copy(p = step.next, answer = s.answer ++ step.result.map(_ -> Outcome.Interrupted)))
        )
      }

  private def sent(key: String, cs: Seq[String], t: State) =
    t.copy(
      log = t.log.updated(key, t.log.getOrElse(key, Vector.empty) ++ cs),
      sent = cs.foldLeft(t.sent)((m, c) => m.updated(c, m.getOrElse(c, 0) + 1))
    )

  private def admit(s: State, c: String, id: Long, key: String, status: Status) =
    s.copy(
      status = s.status.updated(c, status),
      ids = s.ids.updated(id, c),
      admitted = s.admitted.updated(key, s.admitted.getOrElse(key, Vector.empty) :+ c),
      gone = s.gone.updated(c, s.log.getOrElse(key, Vector.empty).toSet)
    )

  /**
   * What an observer learns from one step about the order lanes are served in: keys join `readyOrder` when their lane
   * is seen to become ready, and leave it when it no longer is; a step that takes a key while another has been ready
   * longer is unfair.
   */
  private def observe(before: State, after: State): State =
    def ready(st: State) = st.p.lanes.collect { case (k, l) if l.schedule == Protocol.Schedule.Ready => k }.toSet
    val (was, now)       = (ready(before), ready(after))
    val taken            = after.p.batches.collect { case (w, b) if !before.p.batches.contains(w) => b.key }.headOption
    val unfair           = taken.exists(k => before.readyOrder.headOption.exists(_ != k))
    val kept             = before.readyOrder.filter(now)
    val order            = kept ++ (now -- was -- kept).toVector.sorted
    after.copy(readyOrder = order, unfair = before.unfair || unfair)

  // ---------------------------------------------------------------------------------------------------------------
  // Exploration

  /** Every step the environment and the protocol can take from `s`, labelled. */
  def next(config: Config, rules: Engine.Rules)(s: State): List[(String, State)] =
    val steps = Steps(rules)
    import s.p
    def name(id: Long)          = s.ids(id)
    def outcomes(open: Boolean) = if open then List(Outcome.Succeeded, Outcome.Failed) else Outcome.values.toList

    val submits = for
      c <- config.callers if !s.status.contains(c)
      k <- config.keys
    yield s"Submit($c, $k)" -> steps.submit(s, c, k, config.weight(c), config.rank(c)).next

    val dues = for
      k <- config.keys
      m <- steps.due(s, k).toList
    yield s"Due($k)" -> m.next

    val takes = for
      w <- config.workers
      m <- steps.take(s, w).toList
    yield
      val batch = m.next.p.batches(w)
      s"Take($w, ${batch.key}, ${batch.items.map(i => name(i.caller)).toList.mkString(",")})" -> m.next

    val completes = for
      (w, batch) <- p.batches.toList
      assignment <- assignments(batch.items.map(_.caller).toList, outcomes(p.open))
      m          <- steps.complete(s, w, assignment).toList
    yield s"Complete($w, ${assignment.toList.sortBy(_._1).map((id, o) => s"${name(id)}:$o").mkString(",")})" -> m.next

    val directs = for
      (id, _) <- p.direct.toList
      outcome <- outcomes(p.open && s.pending(name(id)))
      m       <- steps.completeDirect(s, id, outcome).toList
    yield s"CompleteDirect(${name(id)}, $outcome)" -> m.next

    val cancels = for
      c <- s.pending.toList.sorted
      m <- steps.cancel(s, s.idOf(c).get).toList
    yield s"Cancel($c)" -> m.next

    val close = steps.close(s).toList.map(m => "Close" -> m.next)

    submits ++ dues ++ takes ++ completes ++ directs ++ cancels ++ close

  private def assignments(callers: List[Long], outcomes: List[Outcome]): List[Map[Long, Outcome]] =
    callers.foldLeft(List(Map.empty[Long, Outcome]))((acc, c) => for m <- acc; o <- outcomes yield m.updated(c, o))

  /** The steps that are the system's to take; the environment's (arrivals, giving up, closing) need not happen. */
  def progress(label: String): Boolean =
    List("Due(", "Take(", "Complete(", "CompleteDirect(").exists(label.startsWith)

  // ---------------------------------------------------------------------------------------------------------------
  // Properties

  type Property = (String, State => Boolean)

  /** The protocol's safety properties, stated over what an observer saw; the limits are the protocol's own. */
  val invariants: List[Property] =
    def before(s: Vector[String], a: String, b: String) = s.indexOf(a) >= 0 && s.indexOf(a) < s.indexOf(b)
    List(
      "OneInFlightPerKey" -> { s =>
        !s.p.limits.oneCallPerKey || {
          val calls = s.p.batches.values.map(_.key).toList ++ s.p.direct.values
          calls.groupBy(identity).values.forall(_.size <= 1)
        }
      },
      "InOrderPerKey" -> { s =>
        s.log.forall { (k, log) =>
          val admitted = s.admitted.getOrElse(k, Vector.empty)
          log.indices.forall { i =>
            (i + 1 until log.size).forall { j =>
              val (a, b) = (log(i), log(j))
              s.rank(a) < s.rank(b) || (s.rank(a) == s.rank(b) && before(admitted, a, b)) ||
              s.gone.getOrElse(b, Set.empty)(a)
            }
          }
        }
      },
      "AtMostOnce"          -> (s => s.sent.values.forall(_ <= 1)),
      "TurnedAwayNeverSent" -> { s =>
        s.status.forall((c, st) => !(st == Status.Rejected || st == Status.Refused) || !s.sent.contains(c))
      },
      "BatchBounded" -> { s =>
        s.p.batches.values.forall { b =>
          b.items.size <= s.p.limits.maxBatchSize &&
          (b.items.size == 1 || b.items.map(_.weight).sum <= s.p.limits.maxBatchWeight)
        }
      },
      "QueueBounded" -> { s =>
        val queued = s.p.lanes.values.flatMap(_.items).toList
        queued.size <= s.p.limits.maxQueueing && queued.map(_.weight).sum <= s.p.limits.maxQueuedWeight
      },
      "NoneStrandedAfterClose" -> { s =>
        s.p.open || s.pending.forall { c =>
          s.p.batches.values.exists(_.items.exists(i => s.ids(i.caller) == c)) ||
          s.p.direct.keys.exists(id => s.ids(id) == c)
        }
      }
    )

  /**
   * The contract beyond those invariants, which a description of the protocol's actions would only imply: an input
   * whose caller gave up while it waited is never sent, and ready lanes are served first come, first served.
   */
  val reference: List[Property] =
    invariants ++ List(
      "CancelledNeverSent"   -> (s => s.cancelledWaiting.forall(c => !s.sent.contains(c))),
      "FirstComeFirstServed" -> (s => !s.unfair)
    )

  /** Settled: nobody let in is still waiting for an answer. */
  def settled(s: State): Boolean = s.pending.isEmpty

  def check(
    config: Config,
    rules: Engine.Rules = Engine.Rules.standard,
    properties: List[Property] = reference
  ): StateSpace.Verdict[State, String] =
    StateSpace.check(init(config.limits), next(config, rules), properties, progress, settled)

  // ---------------------------------------------------------------------------------------------------------------
  // Replay of a recorded run

  enum Replay:
    case Conforms(steps: Int)
    case Diverged(step: Int, recorded: Action[String], produced: Option[Chunk[Action[String]]])
    case Violated(step: Int, property: String, recorded: Action[String])

    def conforms: Boolean = this match
      case Conforms(_) => true
      case _           => false

  /**
   * Replays a run recorded from the engine through the pure protocol, one recorded action at a time. Each must be
   * exactly what the protocol does from the state the run had reached, and the properties must hold after it. A
   * divergence means the engine took a step the protocol would not take; a violation means the protocol itself broke a
   * property on a path the engine really took.
   */
  def replay(
    limits: Protocol.Limits,
    trace: Chunk[Action[String]],
    rules: Engine.Rules = Engine.Rules.standard,
    properties: List[Property] = reference
  ): Replay =
    val steps                                                  = Steps(rules)
    def name(id: Long)                                         = s"c$id"
    def apply(s: State, action: Action[String]): Option[Moved] = action match
      case Action.Enqueue(c, k, w, r)  => Some(steps.submit(s, name(c), k, w, r))
      case Action.Claim(c, k, w, r)    => Some(steps.submit(s, name(c), k, w, r))
      case Action.Reject(c, k, w, r)   => Some(steps.submit(s, name(c), k, w, r))
      case Action.Refuse(c, k, w, r)   => Some(steps.submit(s, name(c), k, w, r))
      case Action.Due(k)               => steps.due(s, k)
      case Action.Take(w, _, _)        => steps.take(s, w)
      case Action.Complete(w, os)      => steps.complete(s, w, os.toMap)
      case Action.CompleteDirect(c, o) => steps.completeDirect(s, c, o)
      case Action.Cancel(c)            => steps.cancel(s, c)
      case Action.Close(_)             => steps.close(s)

    @annotation.tailrec
    def loop(s: State, i: Int): Replay =
      if i == trace.size then Replay.Conforms(i)
      else
        apply(s, trace(i)) match
          case Some(Moved(actions, next)) if actions == Chunk.single(trace(i)) =>
            properties.collectFirst { case (name, p) if !p(next) => name } match
              case Some(property) => Replay.Violated(i + 1, property, trace(i))
              case None           => loop(next, i + 1)
          case other => Replay.Diverged(i + 1, trace(i), other.map(_.actions))

    loop(init(limits), 0)
