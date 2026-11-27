package ferry.check

import zio.*
import zio.test.*

import ferry.{Engine, Protocol}
import ferry.Protocol.{Action, Admission, Step}
import ferry.check.ProtocolSpace.Config
import ferry.testkit.check.StateSpace.Verdict

/**
 * The protocol checked directly, without a second description: every state of small instances, every safety property,
 * and liveness once the environment stops. The drifts planted in the engine's rules are in [[ferry.DriftSpec]].
 */
object ProtocolSpaceSpec extends ZIOSpecDefault:

  /**
   * Applies every step with `oneCallPerKey` off: a busy key's lane is never blocked (the model's `BusyGuard = FALSE`).
   */
  object NoBusyGuard extends Engine.Rules:
    private def off[K, X](p: Protocol[K, X]) = p.copy(limits = p.limits.copy(oneCallPerKey = false))
    private def back[K, X, R](p: Protocol[K, X], s: Step[K, X, R]) = s.copy(next = s.next.copy(limits = p.limits))
    override def submit[K, X](p: Protocol[K, X], key: K, value: X, weight: Long, rank: Long, now: Long) =
      back(p, off(p).submit(key, value, weight, rank, now))
    override def take[K, X](p: Protocol[K, X], worker: Int, now: Long) = off(p).take(worker, now).map(back(p, _))
    override def complete[K, X](p: Protocol[K, X], worker: Int, outcome: Long => Protocol.Outcome) =
      off(p).complete(worker, outcome).map(back(p, _))

  /** Inputs join the back of their lane, whatever their rank (the model's `Sorted = FALSE`). */
  object Unsorted extends Engine.Rules:
    override def submit[K, X](p: Protocol[K, X], key: K, value: X, weight: Long, rank: Long, now: Long) =
      val s = p.submit(key, value, weight, rank, now)
      s.result match
        case Admission.Queued(_) =>
          val lane = s.next.lanes(key)
          s.copy(next = s.next.copy(lanes = s.next.lanes.updated(key, lane.copy(items = lane.items.sortBy(_.caller)))))
        case _ => s

  /** A key is claimed for a direct call even when it is not idle (the model's `DirectGuard = FALSE`). */
  object NoDirectGuard extends Engine.Rules:
    override def submit[K, X](p: Protocol[K, X], key: K, value: X, weight: Long, rank: Long, now: Long) =
      if p.open && p.limits.direct && !p.idle(key) then
        val caller = p.nextCaller
        Step(
          Admission.Claimed(caller),
          p.copy(
            nextCaller = caller + 1,
            pending = p.pending.updated(caller, value),
            busy = p.busy.updated(key, p.busy.getOrElse(key, 0) + 1),
            direct = p.direct.updated(caller, key)
          ),
          Chunk.single(Action.Claim(caller, key, weight, rank))
        )
      else p.submit(key, value, weight, rank, now)

  /** A finished call frees its key but leaves the lane blocked behind it: its callers are never served. */
  object ForgetBlockedLane extends Engine.Rules:
    override def complete[K, X](p: Protocol[K, X], worker: Int, outcome: Long => Protocol.Outcome) =
      p.complete(worker, outcome).map { s =>
        val key = p.batches(worker).key
        s.next.lanes.get(key) match
          case Some(lane) if p.lanes.get(key).exists(_.schedule == Protocol.Schedule.Blocked) =>
            s.copy(next =
              s.next.copy(
                lanes = s.next.lanes.updated(key, lane.copy(schedule = Protocol.Schedule.Blocked)),
                ready = s.next.ready.filterNot(_ == key),
                waiting = s.next.waiting.map((d, ks) => d -> ks.filterNot(_ == key)).filter(_._2.nonEmpty)
              )
            )
          case _ => s
      }

  /** Closing leaves the lanes as they are (the model's `DrainOnClose = FALSE`). */
  object NoDrain extends Engine.Rules:
    override def close[K, X](p: Protocol[K, X]) =
      Option.when(p.open)(Step(Chunk.empty[X], p.copy(open = false), Chunk.single(Action.Close(Chunk.empty))))

  private val oneKey = Config(List("c1", "c2", "c3"), List("k1"), List(1, 2), 2, 3, 3, 4, false, true)

  private def timed(label: String, config: Config, rules: Engine.Rules = Engine.Rules.standard) =
    ZIO.succeed(java.lang.System.nanoTime()).flatMap { start =>
      ZIO.succeed(ProtocolSpace.check(config, rules)).tap { v =>
        val ms = (java.lang.System.nanoTime() - start) / 1_000_000
        ZIO.debug(s"[$label] ${v.describe(_.show).linesIterator.take(40).mkString("\n")}  (${ms} ms)")
      }
    }

  private def violates(property: String)(v: Verdict[?, ?]) = v match
    case Verdict.Violated(`property`, _, _) => true
    case _                                  => false

  def spec = suite("The protocol, checked directly")(
    suite("small instances of every configuration hold")(
      test("perKey with direct calls")(timed("perKey", Config.perKey).map(v => assertTrue(v.holds))),
      test("perKey, every input batched")(timed("batchOnly", Config.batchOnly).map(v => assertTrue(v.holds))),
      test("make")(timed("make", Config.make).map(v => assertTrue(v.holds))),
      test("perKey with a tight weight limit")(timed("weighted", Config.weighted).map(v => assertTrue(v.holds)))
    ),
    suite("each safeguard, removed in code, is caught")(
      test("rank order in the lane") {
        timed("Unsorted", oneKey.copy(lowRank = Set("c2")), Unsorted).map(v => assertTrue(violates("InOrderPerKey")(v)))
      },
      test("blocking a busy key's lane") {
        timed("NoBusyGuard", oneKey, NoBusyGuard).map(v => assertTrue(violates("OneInFlightPerKey")(v)))
      },
      test("claiming only an idle key") {
        timed("NoDirectGuard", oneKey.copy(callers = List("c1", "c2"), direct = true), NoDirectGuard)
          .map(v => assertTrue(violates("OneInFlightPerKey")(v)))
      },
      test("emptying the lanes on close") {
        timed("NoDrain", oneKey.copy(callers = List("c1", "c2")), NoDrain)
          .map(v => assertTrue(violates("NoneStrandedAfterClose")(v)))
      }
    ),
    suite("liveness")(
      test("a lane left blocked after its key is freed, by bounded liveness") {
        timed("ForgetBlockedLane", Config.batchOnly, ForgetBlockedLane).map {
          case _: Verdict.Unsettled[?, ?] => assertCompletes
          case other                      => assertNever(other.describe())
        }
      }
    )
  ) @@ TestAspect.sequential
