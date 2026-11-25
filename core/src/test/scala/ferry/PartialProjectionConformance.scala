package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.PartialProjectionSimulation.{Event, RowOutcome, Writer}
import ferry.WriterTraces.{node, sequence, Scan}
import ferry.testkit.tla.{Alter, Tlc, Value}

/**
 * The mapping from a recorded run of [[PartialProjectionSimulation]] to the steps of
 * `formal/ProjectedWriterPartial.tla`, and trace validation of each key's steps against it (through
 * `formal/ProjectedWriterPartialTrace.tla`). The ferry's actions map as [[WriterTraces]] says, with a failed call as
 * `Fail`; the store's steps are the model's own, the sweeper's included.
 */
object PartialProjectionConformance:

  private def step(name: String, fields: (String, Value)*): Value = Value.rec(("name" -> Value.str(name)) +: fields*)

  private def from(who: Option[Int]): Value = who.fold(Value.str("sweeper"))(node)

  /** Each key's steps, as the model's `act` records. */
  def traces(events: Chunk[Event], keys: Set[Int]): Map[Int, Chunk[Value]] =
    events
      .foldLeft(Scan()) { (s, event) =>
        event match
          case Event.Ferry(n, action)          => WriterTraces.ferry(s, n, action, keys, failed = "Fail")
          case Event.Read(n, k)                => s.emit(k, step("Read", "node" -> node(n)))
          case Event.Sweep(k)                  => s.emit(k, step("Sweep"))
          case Event.Commit(n, k, length, ids) =>
            s.emit(k, step("Commit", "node" -> node(n), "length" -> Value.int(length), "ids" -> sequence(ids)))
          case Event.DeliverCommit(n, k, length, ids, applied, answered) =>
            s.emit(
              k,
              step(
                "DeliverCommit",
                "from"     -> node(n),
                "length"   -> Value.int(length),
                "ids"      -> sequence(ids),
                "applied"  -> Value.Bool(applied),
                "answered" -> Value.Bool(answered)
              )
            )
          case Event.ReadRows(who, k) => s.emit(k, step("ReadRows", "from" -> from(who)))
          case Event.Row(who, k, id)  => s.emit(k, step("Row", "from" -> from(who), "row" -> Value.int(id)))
          case Event.DeliverRow(who, k, id, outcome, applied) =>
            val how = outcome match
              case RowOutcome.Landed  => "landed"
              case RowOutcome.Refused => "refused"
              case RowOutcome.Lost    => "lost"
            s.emit(
              k,
              step(
                "DeliverRow",
                "from"    -> from(who),
                "row"     -> Value.int(id),
                "outcome" -> Value.str(how),
                "applied" -> Value.Bool(applied)
              )
            )
          case Event.Clear(who, k, length) =>
            s.emit(k, step("Clear", "from" -> from(who), "length" -> Value.int(length)))
          case Event.DeliverClear(who, k, length, applied, answered) =>
            s.emit(
              k,
              step(
                "DeliverClear",
                "from"     -> from(who),
                "length"   -> Value.int(length),
                "applied"  -> Value.Bool(applied),
                "answered" -> Value.Bool(answered)
              )
            )
          case Event.Throttle(who, k, commit, length, ids) =>
            s.emit(
              k,
              step(
                "Throttle",
                "from"   -> from(who),
                "kind"   -> Value.str(if commit then "commit" else "clear"),
                "length" -> Value.int(length),
                "ids"    -> sequence(ids)
              )
            )
      }
      .byKey(keys)

  /**
   * Checks one key's trace against the model with the writer's switches: the code's [[Writer]] flags are the model's
   * `ConditionalRows`, `ClearWhenComplete` and `FencedClear`. The code always resumes, so `Resume` holds.
   */
  def validate(
    name: String,
    acts: Chunk[Value],
    commands: Set[Int],
    nodes: Int,
    model: Writer,
    alter: Alter = Alter.none
  ): Tlc.Verdict =
    WriterTraces.validate(
      "ProjectedWriterPartialTrace",
      name,
      acts,
      commands,
      nodes,
      ListMap(
        "MaxCrashes"        -> Conformance.Unbounded.toString,
        "MaxFaults"         -> Conformance.Unbounded.toString,
        "MaxGiveUps"        -> Conformance.Unbounded.toString,
        "ConditionalRows"   -> WriterTraces.bool(model.conditionalRows),
        "ClearWhenComplete" -> WriterTraces.bool(model.clearWhenComplete),
        "FencedClear"       -> WriterTraces.bool(model.fencedClear),
        "Resume"            -> "TRUE"
      ),
      alter
    )

  /** Every key's trace of a run, checked: the verdicts by key. */
  def check(
    name: String,
    scenario: PartialProjectionSimulation.Scenario,
    trace: PartialProjectionSimulation.Trace,
    model: Writer = Writer()
  ): Map[Int, Tlc.Verdict] =
    val byKey = scenario.submissions.groupMap(_.command.key)(_.command.id).view.mapValues(_.toSet).toMap
    traces(trace.events, byKey.keySet).map { (k, acts) =>
      k -> validate(s"${name}_key$k", acts, byKey(k), PartialProjectionSimulation.nodes, model)
    }
