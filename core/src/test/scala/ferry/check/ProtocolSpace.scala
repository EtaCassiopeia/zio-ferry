package ferry.check

import ferry.{Engine, Protocol}
import ferry.Protocol.{Admission, Outcome}
import ferry.testkit.check.StateSpace

/**
 * The protocol's whole state space, explored directly: the code is the model.
 *
 * The environment does what it may do in `formal/Ferry.tla` (callers arrive, deadlines pass, calls return with any
 * outcome, callers give up, the ferry closes), and every step is applied by the real [[Protocol]] through
 * [[Engine.Rules]], so a deliberately wrong rule set plugs in exactly as it does for the engine. Time is an event: a
 * waiting lane may become due at any moment, as in the model, and `take` never promotes a lane on its own.
 *
 * Alongside the protocol's state, the explorer keeps what an observer outside it would see: who was let in, in what
 * order per key, what was sent, who was answered. The properties are stated over those observations, not over the
 * protocol's internals, so a protocol that computes the wrong thing cannot also hide it.
 */
object ProtocolSpace:

  /** The bounds of one exploration, as in a TLC configuration of `Ferry.tla`. */
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

    /** `Ferry.cfg`: `perKey` with direct calls. */
    val perKey = Config(three, List("k1", "k2"), List(1, 2), 2, 3, 2, 4, true, true, Set("c2"), Set("c3"))

    /** `FerryBatchOnly.cfg`: `perKey`, every input batched. */
    val batchOnly = perKey.copy(direct = false)

    /** `FerryMake.cfg`: `make`, one key, batches of it may run at once. */
    val make = Config(three, List("k1"), List(1, 2), 2, 3, 2, 4, false, false)

    /** `perKey`, every input batched, with a weight limit that a light and a heavy input together exceed. */
    val weighted = batchOnly.copy(maxWeight = 2)

  enum Status:
    case Idle, Queued, Direct, Rejected, Refused

  /** The protocol, and what an observer has seen of it. */
  final case class State(
    p: Protocol[String, String],
    status: Map[String, Status],
    answer: Map[String, Outcome],
    ids: Map[Long, String],
    admitted: Map[String, Vector[String]],
    log: Map[String, Vector[String]],
    gone: Map[String, Set[String]],
    sent: Map[String, Int],
    cancelledWaiting: Set[String],
    readyOrder: Vector[String] = Vector.empty,
    unfair: Boolean = false
  ):
    def pending: Set[String] =
      status.collect { case (c, Status.Queued | Status.Direct) if !answer.contains(c) => c }.toSet

    def show: String =
      val lanes = p.lanes.toList.sortBy(_._1).map((k, l) => s"$k:${l.items.map(_.value).mkString("[", ",", "]")}")
      val calls = p.batches.toList.sortBy(_._1).map((w, b) => s"w$w:${b.items.map(_.value).toList.mkString(",")}") ++
        p.direct.toList.map((id, k) => s"${ids(id)}→$k")
      s"open=${p.open} lanes=${lanes.mkString(" ")} ready=${p.ready.mkString(",")} calls=${calls.mkString(" ")} " +
        s"answered=${answer.toList.sortBy(_._1).map((c, o) => s"$c:$o").mkString(",")}"

  def init(config: Config): State =
    State(
      Protocol.empty(config.limits),
      config.callers.map(_ -> Status.Idle).toMap,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Set.empty
    )

  /** Every step the environment and the protocol can take from `s`, labelled as the model's actions. */
  def next(config: Config, rules: Engine.Rules)(s: State): List[(String, State)] =
    import s.p
    def name(id: Long)                                  = s.ids(id)
    def sentAll(key: String, cs: Seq[String], t: State) =
      t.copy(
        log = t.log.updated(key, t.log.getOrElse(key, Vector.empty) ++ cs),
        sent = cs.foldLeft(t.sent)((m, c) => m.updated(c, m.getOrElse(c, 0) + 1))
      )
    def outcomes(open: Boolean) =
      if open then List(Outcome.Succeeded, Outcome.Failed) else Outcome.values.toList

    val submits = for
      c <- config.callers if s.status(c) == Status.Idle
      k <- config.keys
    yield
      val step = rules.submit(p, k, c, config.weight(c), config.rank(c), 0L)
      val base = s.copy(p = step.next)
      val t    = step.result match
        case Admission.Refused(_)  => base.copy(status = base.status.updated(c, Status.Refused))
        case Admission.Rejected(_) => base.copy(status = base.status.updated(c, Status.Rejected))
        case Admission.Queued(id)  => admit(base, c, id, k, Status.Queued)
        case Admission.Claimed(id) => sentAll(k, List(c), admit(base, c, id, k, Status.Direct))
      s"Submit($c, $k)" -> t

    val dues = for
      k    <- config.keys
      step <- p.due(k).toList
    yield s"Due($k)" -> s.copy(p = step.next)

    val takes = for
      w    <- config.workers if !p.batches.contains(w)
      step <- rules.take(p, w, Long.MinValue).toOption.toList
    yield
      val (key, batch) = step.result
      s"Take($w, $key, ${batch.map(i => name(i.caller)).toList.mkString(",")})" ->
        sentAll(key, batch.map(i => name(i.caller)).toList, s.copy(p = step.next))

    val completes = for
      (w, batch) <- p.batches.toList
      assignment <- assignments(batch.items.map(_.caller).toList, outcomes(p.open))
      step       <- rules.complete(p, w, assignment).toList
    yield
      val answered = step.result.map((id, _) => name(id) -> assignment(id))
      s"Complete($w, ${assignment.toList.sortBy(_._1).map((id, o) => s"${name(id)}:$o").mkString(",")})" ->
        s.copy(p = step.next, answer = s.answer ++ answered)

    val directs = for
      (id, _) <- p.direct.toList
      outcome <- outcomes(p.open && s.pending(name(id)))
      step    <- rules.completeDirect(p, id, outcome).toList
    yield
      val answered = step.result.map(_ => name(id) -> outcome)
      s"CompleteDirect(${name(id)}, $outcome)" -> s.copy(p = step.next, answer = s.answer ++ answered)

    val cancels = for
      c <- s.pending.toList.sorted
      id = s.ids.collectFirst { case (id, `c`) => id }.get
      step <- rules.cancel(p, id).toList
    yield
      val waiting = p.lanes.values.exists(_.items.exists(_.caller == id))
      s"Cancel($c)" -> s.copy(
        p = step.next,
        answer = s.answer.updated(c, Outcome.Interrupted),
        cancelledWaiting = if waiting then s.cancelledWaiting + c else s.cancelledWaiting
      )

    val close = rules.close(p).toList.map { step =>
      val drained = step.result.toList
      "Close" -> s.copy(p = step.next, answer = s.answer ++ drained.map(_ -> Outcome.Interrupted))
    }

    (submits ++ dues ++ takes ++ completes ++ directs ++ cancels ++ close).map((label, t) => label -> observe(s, t))

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

  private def admit(s: State, c: String, id: Long, key: String, status: Status) =
    s.copy(
      status = s.status.updated(c, status),
      ids = s.ids.updated(id, c),
      admitted = s.admitted.updated(key, s.admitted.getOrElse(key, Vector.empty) :+ c),
      gone = s.gone.updated(c, s.log.getOrElse(key, Vector.empty).toSet)
    )

  private def assignments(callers: List[Long], outcomes: List[Outcome]): List[Map[Long, Outcome]] =
    callers.foldLeft(List(Map.empty[Long, Outcome]))((acc, c) => for m <- acc; o <- outcomes yield m.updated(c, o))

  /** The steps that are the system's to take; the environment's (arrivals, giving up, closing) need not happen. */
  def progress(label: String): Boolean =
    List("Due(", "Take(", "Complete(", "CompleteDirect(").exists(label.startsWith)

  /** The safety properties of `Ferry.tla`, stated over what an observer saw. */
  def invariants(config: Config): List[(String, State => Boolean)] =
    def before(s: Vector[String], a: String, b: String) = s.indexOf(a) >= 0 && s.indexOf(a) < s.indexOf(b)
    List(
      "OneInFlightPerKey" -> { s =>
        !config.oneCallPerKey || config.keys.forall { k =>
          s.p.batches.values.count(_.key == k) + s.p.direct.values.count(_ == k) <= 1
        }
      },
      "InOrderPerKey" -> { s =>
        s.log.forall { (k, log) =>
          val admitted = s.admitted.getOrElse(k, Vector.empty)
          log.indices.forall { i =>
            (i + 1 until log.size).forall { j =>
              val (a, b) = (log(i), log(j))
              config.rank(a) < config.rank(b) ||
              (config.rank(a) == config.rank(b) && before(admitted, a, b)) ||
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
          b.items.size <= config.maxBatch && (b.items.size == 1 || b.items.map(_.weight).sum <= config.maxWeight)
        }
      },
      "QueueBounded" -> { s =>
        val queued = s.p.lanes.values.flatMap(_.items).toList
        queued.size <= config.maxQueue && queued.map(_.weight).sum <= config.maxQueuedWeight
      },
      "NoneStrandedAfterClose" -> { s =>
        s.p.open || s.pending.forall { c =>
          s.p.batches.values.exists(_.items.exists(i => s.ids(i.caller) == c)) ||
          s.p.direct.keys.exists(id => s.ids(id) == c)
        }
      }
    )

  /**
   * What the contract says beyond the model's invariants, which the model's actions imply but never state: an input
   * whose caller gave up while it waited is never sent, and ready lanes are served first come, first served.
   */
  def reference(config: Config): List[(String, State => Boolean)] =
    invariants(config) ++ List(
      "CancelledNeverSent"   -> (s => s.cancelledWaiting.forall(c => !s.sent.contains(c))),
      "FirstComeFirstServed" -> (s => !s.unfair)
    )

  /** Settled: nobody let in is still waiting for an answer. */
  def settled(s: State): Boolean = s.pending.isEmpty

  def check(
    config: Config,
    rules: Engine.Rules = Engine.Rules.standard,
    invariants: Config => List[(String, State => Boolean)] = reference
  ): StateSpace.Verdict[State, String] =
    StateSpace.check(init(config), next(config, rules), invariants(config), progress, settled)
