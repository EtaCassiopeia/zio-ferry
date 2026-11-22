package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.Protocol.{Action, Limits}
import ferry.testkit.tla.{Alter, Tlc, TraceValidation, Value}

/**
 * The mapping between the code's [[Protocol.Action]]s and the model's `act` records, and trace validation of a recorded
 * run against `formal/Ferry.tla` (through `formal/FerryTrace.tla`).
 *
 * This file, the recorder and `FerryTrace.tla` are all the glue the code↔model check needs: the field names here are
 * the record fields of `act` in the model, one case per action.
 */
object Conformance:

  /** A recorded run: the ferry's limits and workers, and every action it took, in order. */
  final case class Recorded[K](limits: Limits, workers: Int, actions: Chunk[Action[K]], key: K => String)

  /** TLC computes with 32-bit integers: unbounded limits are written as this, and weights must stay well below it. */
  val Unbounded: Long = 100_000_000L

  def validate[K](name: String, run: Recorded[K], alter: Alter = Alter.none): Tlc.Verdict =
    Tlc.formal match
      case None         => Tlc.Verdict.Skipped("no formal/ directory above the working directory")
      case Some(formal) => TraceValidation.check(formal, "FerryTrace", trace(name, run), alter)

  def trace[K](name: String, run: Recorded[K]): TraceValidation.Trace =
    val arrivals = run.actions.collect {
      case Action.Enqueue(c, k, w, r) => (c, k, w, r)
      case Action.Claim(c, k, w, r)   => (c, k, w, r)
      case Action.Reject(c, k, w, r)  => (c, k, w, r)
      case Action.Refuse(c, k, w, r)  => (c, k, w, r)
    }
    // The model compares ranks only, so they are renumbered densely to fit TLC's integers.
    val ranks    = arrivals.map(_._4).distinct.sorted.zipWithIndex.toMap
    val rankOf   = arrivals.map(a => a._1 -> ranks(a._4).toLong).toMap
    val weightOf = arrivals.map(a => a._1 -> (a._3 min Unbounded)).toMap
    val keys  = (arrivals.map(a => run.key(a._2)) ++ run.actions.collect { case Action.Due(k) => run.key(k) }).distinct
    val l     = run.limits
    val names = Names[K](Value.int, worker, k => Value.str(run.key(k)), (c, _) => weightOf(c), (c, _) => rankOf(c))
    TraceValidation.Trace(
      name,
      definitions = ListMap(
        "TraceLog"     -> Value.Seq(run.actions.map(encode(_, names)).toVector),
        "TraceCallers" -> Value.Set(arrivals.map(a => Value.int(a._1)).toSet),
        "TraceKeys"    -> Value.Set((if keys.isEmpty then Chunk("unused") else keys).map(Value.str).toSet),
        "TraceWorkers" -> Value.Set((1 to run.workers).map(w => worker(w)).toSet),
        "TraceWeight"  -> function(weightOf),
        "TraceRank"    -> function(rankOf)
      ),
      substitutions = ListMap(
        "Callers"  -> "TraceCallers",
        "Keys"     -> "TraceKeys",
        "Workers"  -> "TraceWorkers",
        "WeightOf" -> "TraceWeight",
        "RankOf"   -> "TraceRank"
      ),
      assignments = ListMap(
        "MaxBatch"        -> l.maxBatchSize.toString,
        "MaxWeight"       -> (l.maxBatchWeight min Unbounded).toString,
        "MaxQueue"        -> l.maxQueueing.toString,
        "MaxQueuedWeight" -> (l.maxQueuedWeight min Unbounded).toString,
        "Direct"          -> tla(l.direct),
        "OneCallPerKey"   -> tla(l.oneCallPerKey),
        "Sorted"          -> "TRUE",
        "BusyGuard"       -> "TRUE",
        "DirectGuard"     -> "TRUE",
        "AtomicAdmission" -> "TRUE",
        "DrainOnClose"    -> "TRUE",
        "LowRank"         -> "{}",
        "Heavy"           -> "{}"
      )
    )

  /**
   * How the code's identities are written in the model: callers, workers and keys, and the weight and rank of a caller.
   * Trace validation writes callers as integers and workers as `"w1"`…; a replay of the model's behaviours writes them
   * as the model values the model uses (`c1`, `w1`, `k1`).
   */
  final case class Names[K](
    caller: Long => Value,
    worker: Int => Value,
    key: K => Value,
    weight: (Long, Long) => Long = (_, w) => w,
    rank: (Long, Long) => Long = (_, r) => r
  )

  /** One action as the model's `act` record. This is the whole mapping from the code's vocabulary to the model's. */
  def encode[K](action: Action[K], names: Names[K]): Value =
    def arrival(name: String, c: Long, k: K, w: Long, r: Long) =
      Value.rec(
        "name"   -> Value.str(name),
        "caller" -> names.caller(c),
        "key"    -> names.key(k),
        "weight" -> Value.int(names.weight(c, w)),
        "rank"   -> Value.int(names.rank(c, r))
      )
    action match
      case Action.Enqueue(c, k, w, r) => arrival("Enqueue", c, k, w, r)
      case Action.Claim(c, k, w, r)   => arrival("Claim", c, k, w, r)
      case Action.Reject(c, k, w, r)  => arrival("Reject", c, k, w, r)
      case Action.Refuse(c, k, w, r)  => arrival("Refuse", c, k, w, r)
      case Action.Due(k)              => Value.rec("name" -> Value.str("Due"), "key" -> names.key(k))
      case Action.Take(w, k, batch)   =>
        Value.rec(
          "name"   -> Value.str("Take"),
          "worker" -> names.worker(w),
          "key"    -> names.key(k),
          "batch"  -> Value.Seq(batch.map(names.caller).toVector)
        )
      case Action.Complete(w, outcomes) =>
        Value.rec(
          "name"     -> Value.str("Complete"),
          "worker"   -> names.worker(w),
          "outcomes" -> Value.Fn(ListMap.from(outcomes.map((c, o) => names.caller(c) -> Value.str(o.toString))))
        )
      case Action.CompleteDirect(c, o) =>
        Value.rec(
          "name"    -> Value.str("CompleteDirect"),
          "caller"  -> names.caller(c),
          "outcome" -> Value.str(o.toString)
        )
      case Action.Cancel(c)      => Value.rec("name" -> Value.str("Cancel"), "caller" -> names.caller(c))
      case Action.Close(drained) =>
        Value.rec("name" -> Value.str("Close"), "drained" -> Value.Set(drained.map(names.caller).toSet))

  /** The model's name for each action: the vocabulary, as the code knows it. */
  def name(action: Action[?]): String = action.productPrefix

  def worker(w: Int): Value = Value.str(s"w$w")

  private def function(m: Map[Long, Long]): Value =
    if m.isEmpty then Value.Fn(ListMap.empty)
    else Value.Fn(ListMap.from(m.toVector.sortBy(_._1).map((c, v) => Value.int(c) -> Value.int(v))))

  private def tla(b: Boolean): String = if b then "TRUE" else "FALSE"
