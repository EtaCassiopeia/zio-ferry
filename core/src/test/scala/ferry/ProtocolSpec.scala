package ferry

import zio.*
import zio.test.*

import ferry.Protocol.{Action, Admission, Outcome, Schedule}

/** The pure protocol, step by step: each step's result, next state and recorded actions. */
object ProtocolSpec extends ZIOSpecDefault:

  private def limits(
    maxBatchSize: Int = 2,
    maxBatchWeight: Long = Long.MaxValue,
    delay: Long = 10L,
    maxQueueing: Int = 8,
    maxQueuedWeight: Long = Long.MaxValue,
    direct: Boolean = false,
    oneCallPerKey: Boolean = true
  ) = Protocol.Limits(maxBatchSize, maxBatchWeight, delay, maxQueueing, maxQueuedWeight, direct, oneCallPerKey)

  private type P = Protocol[String, String]

  extension (p: P)
    def add(key: String, value: String, weight: Long = 1L, rank: Long = 0L, now: Long = 0L): P =
      p.submit(key, value, weight, rank, now).next
    def taken(worker: Int, now: Long): (String, Chunk[String], P, Chunk[Action[String]]) =
      p.take(worker, now) match
        case Right(s) => (s.result._1, s.result._2.map(_.value).toChunk, s.next, s.actions)
        case Left(d)  => throw new AssertionError(s"nothing to take; next deadline $d")
    def values(key: String): Vector[String] = p.lanes.get(key).fold(Vector.empty)(_.items.map(_.value))

  private val empty: P = Protocol.empty(limits())

  def spec = suite("Protocol")(
    suite("submit")(
      test("numbers every caller, and records the arrival with its weight and rank") {
        val s = empty.submit("k", "a", 3L, 7L, 0L)
        assertTrue(
          s.result == Admission.Queued(1L),
          s.actions == Chunk(Action.Enqueue(1L, "k", 3L, 7L)),
          s.next.nextCaller == 2L,
          s.next.pending == Map(1L -> "a")
        )
      },
      test("refuses once closed, rejects beyond the count or weight bound") {
        val closed  = empty.close.get.next.submit("k", "a", 1L, 0L, 0L)
        val bounded = Protocol.empty[String, String](limits(maxQueueing = 1, maxQueuedWeight = 5))
        val full    = bounded.add("k", "a").submit("k", "b", 1L, 0L, 0L)
        val heavy   = bounded.submit("k", "c", 6L, 0L, 0L)
        assertTrue(
          closed.result == Admission.Refused(1L),
          closed.actions.head.isInstanceOf[Action.Refuse[?]],
          full.result == Admission.Rejected(2L),
          heavy.result == Admission.Rejected(1L)
        )
      },
      test("claims only an idle key for a direct call") {
        val p      = Protocol.empty[String, String](limits(direct = true))
        val first  = p.submit("k", "a", 1L, 0L, 0L)
        val second = first.next.submit("k", "b", 1L, 0L, 0L)
        assertTrue(
          first.result == Admission.Claimed(1L),
          first.next.busy == Map("k" -> 1),
          second.result == Admission.Queued(2L),
          second.next.lanes("k").schedule == Schedule.Blocked
        )
      },
      test("keeps a lane in rank order, ties in arrival order") {
        val p = empty.add("k", "a", rank = 1).add("k", "b", rank = 2).add("k", "c", rank = 0).add("k", "d", rank = 1)
        assertTrue(p.values("k") == Vector("c", "a", "d", "b"))
      }
    ),
    suite("take")(
      test("waits for the oldest input's deadline, then promotes and takes, recording Due before Take") {
        val p = empty.add("k", "a", now = 5L)
        assertTrue(p.take(1, 14L) == Left(Some(15L))) && {
          val (key, batch, next, actions) = p.taken(1, 15L)
          assertTrue(
            key == "k",
            batch == Chunk("a"),
            actions == Chunk(Action.Due("k"), Action.Take(1, "k", Chunk(1L))),
            next.busy == Map("k" -> 1),
            next.batches.keySet == Set(1)
          )
        }
      },
      test("takes a full lane at once, and serves ready lanes first come, first served") {
        val p              = empty.add("a", "a1").add("b", "b1").add("b", "b2").add("a", "a2")
        val (k1, _, p1, _) = p.taken(1, 0L)
        val (k2, _, _, _)  = p1.taken(2, 0L)
        assertTrue(k1 == "b", k2 == "a")
      },
      test("takes the longest prefix within the weight limit, and a heavier input on its own") {
        val p             = Protocol.empty[String, String](limits(maxBatchSize = 5, maxBatchWeight = 5))
        val loaded        = p.add("k", "a", weight = 2).add("k", "b", weight = 2).add("k", "c", weight = 2)
        val (_, b1, _, _) = loaded.taken(1, 100L)
        val (_, b2, _, _) = p.add("k", "x", weight = 9).add("k", "y").taken(1, 100L)
        assertTrue(b1 == Chunk("a", "b"), b2 == Chunk("x"))
      },
      test("blocks the rest of a one-call key, and lets a shared key's rest go on") {
        val (_, _, one, _)    = empty.add("k", "a").add("k", "b").add("k", "c").taken(1, 0L)
        val shared            = Protocol.empty[String, String](limits(oneCallPerKey = false))
        val (_, _, many, _)   = shared.add("k", "a").add("k", "b").add("k", "c").add("k", "d").taken(1, 0L)
        val (_, b2, after, _) = many.taken(2, 0L)
        assertTrue(
          one.lanes("k").schedule == Schedule.Blocked,
          one.take(2, 100L) == Left(None),
          b2 == Chunk("c", "d"),
          after.busy == Map("k" -> 2)
        )
      },
      test("takes nothing once closed") {
        assertTrue(empty.add("k", "a").add("k", "b").close.get.next.take(1, 0L) == Left(None))
      }
    ),
    suite("complete")(
      test("answers the pending callers of the batch with their outcomes, and frees the key") {
        val (_, _, p, _) = empty.add("k", "a").add("k", "b").add("k", "c").taken(1, 0L)
        val s            = p.complete(1, c => if c == 1L then Outcome.Succeeded else Outcome.Failed).get
        assertTrue(
          s.result == Chunk(1L -> "a", 2L -> "b"),
          s.actions == Chunk(Action.Complete(1, Chunk(1L -> Outcome.Succeeded, 2L -> Outcome.Failed))),
          s.next.busy.isEmpty,
          s.next.pending.keySet == Set(3L),
          s.next.lanes("k").schedule == Schedule.Waiting(10L)
        )
      },
      test("does not answer a caller that already gave up") {
        val (_, _, p, _) = empty.add("k", "a").add("k", "b").taken(1, 0L)
        val s            = p.cancel(1L).get.next.complete(1, _ => Outcome.Succeeded).get
        assertTrue(s.result == Chunk(2L -> "b"))
      },
      test("is not enabled for a worker without a batch") {
        assertTrue(empty.complete(1, _ => Outcome.Succeeded).isEmpty)
      }
    ),
    suite("cancel")(
      test("takes a waiting input out of its lane at once, and the lane away if it empties") {
        val p = empty.add("k", "a").add("j", "b")
        val s = p.cancel(1L).get
        assertTrue(
          s.result == "a",
          s.actions == Chunk(Action.Cancel(1L)),
          !s.next.lanes.contains("k"),
          s.next.queued == 1,
          s.next.waiting.values.flatten.toSet == Set("j"),
          s.next.cancel(1L).isEmpty
        )
      }
    ),
    suite("close")(
      test("closes admission and drains every lane, once") {
        val (_, _, p, _) = empty.add("k", "a").add("k", "b").add("k", "c").add("j", "d").taken(1, 0L)
        val s            = p.close.get
        assertTrue(
          s.result.toSet == Set("c", "d"),
          s.actions == Chunk(Action.Close(Chunk(3L, 4L))),
          s.next.lanes.isEmpty,
          s.next.pending.keySet == Set(1L, 2L), // the batch in flight is still to be answered
          s.next.close.isEmpty
        )
      }
    )
  )
