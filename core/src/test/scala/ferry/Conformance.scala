package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.Protocol.{Action, Limits}
import ferry.testkit.tla.{Tlc, TraceValidation, Value}

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

  def validate[K](name: String, run: Recorded[K]): Tlc.Verdict =
    Tlc.formal match
      case None         => Tlc.Verdict.Skipped("no formal/ directory above the working directory")
      case Some(formal) => TraceValidation.check(formal, "FerryTrace", trace(name, run))

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
    val keys = (arrivals.map(a => run.key(a._2)) ++ run.actions.collect { case Action.Due(k) => run.key(k) }).distinct
    val l    = run.limits
    TraceValidation.Trace(
      name,
      definitions = ListMap(
        "TraceLog"     -> Value.Seq(run.actions.map(encode(_, run.key, weightOf, rankOf)).toVector),
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

  /** One action as the model's `act` record. */
  def encode[K](action: Action[K], key: K => String, weightOf: Map[Long, Long], rankOf: Map[Long, Long]): Value =
    def arrival(name: String, c: Long, k: K) =
      Value.rec(
        "name"   -> Value.str(name),
        "caller" -> Value.int(c),
        "key"    -> Value.str(key(k)),
        "weight" -> Value.int(weightOf(c)),
        "rank"   -> Value.int(rankOf(c))
      )
    action match
      case Action.Enqueue(c, k, _, _) => arrival("Enqueue", c, k)
      case Action.Claim(c, k, _, _)   => arrival("Claim", c, k)
      case Action.Reject(c, k, _, _)  => arrival("Reject", c, k)
      case Action.Refuse(c, k, _, _)  => arrival("Refuse", c, k)
      case Action.Due(k)              => Value.rec("name" -> Value.str("Due"), "key" -> Value.str(key(k)))
      case Action.Take(w, k, batch)   =>
        Value.rec(
          "name"   -> Value.str("Take"),
          "worker" -> worker(w),
          "key"    -> Value.str(key(k)),
          "batch"  -> Value.Seq(batch.map(Value.int).toVector)
        )
      case Action.Complete(w, outcomes) =>
        Value.rec(
          "name"     -> Value.str("Complete"),
          "worker"   -> worker(w),
          "outcomes" -> Value.Fn(ListMap.from(outcomes.map((c, o) => Value.int(c) -> Value.str(o.toString))))
        )
      case Action.CompleteDirect(c, o) =>
        Value.rec("name" -> Value.str("CompleteDirect"), "caller" -> Value.int(c), "outcome" -> Value.str(o.toString))
      case Action.Cancel(c)      => Value.rec("name" -> Value.str("Cancel"), "caller" -> Value.int(c))
      case Action.Close(drained) =>
        Value.rec("name" -> Value.str("Close"), "drained" -> Value.Set(drained.map(Value.int).toSet))

  /** The model's name for each action: the vocabulary, as the code knows it. */
  def name(action: Action[?]): String = action.productPrefix

  def worker(w: Int): Value = Value.str(s"w$w")

  private def function(m: Map[Long, Long]): Value =
    if m.isEmpty then Value.Fn(ListMap.empty)
    else Value.Fn(ListMap.from(m.toVector.sortBy(_._1).map((c, v) => Value.int(c) -> Value.int(v))))

  private def tla(b: Boolean): String = if b then "TRUE" else "FALSE"
