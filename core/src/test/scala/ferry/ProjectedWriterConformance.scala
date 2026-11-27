package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.ProjectedWriterSimulation.{Event, Writer}
import ferry.WriterTraces.{node, sequence, Scan}
import ferry.testkit.tla.{Alter, Tlc, Value}

/**
 * The mapping from a recorded run of [[ProjectedWriterSimulation]] to the steps of `formal/ProjectedWriter.tla`, and
 * trace validation of each key's steps against it (through `formal/ProjectedWriterTrace.tla`). The ferry's actions map
 * as [[WriterTraces]] says, with a failed call as `Fail`; the store's steps are the model's own, the sweeper's
 * included.
 */
object ProjectedWriterConformance:

  private def write(name: String, fields: (String, Value)*): Value = Value.rec(("name" -> Value.str(name)) +: fields*)

  private def from(who: Option[Int]): Value = who.fold(Value.str("sweeper"))(node)

  private def projection(length: Int, marker: Int, ids: Vector[Int]) =
    Seq("length" -> Value.int(length), "marker" -> Value.int(marker), "ids" -> sequence(ids))

  /** Each key's steps, as the model's `act` records. */
  def traces(events: Chunk[Event], keys: Set[Int]): Map[Int, Chunk[Value]] =
    events
      .foldLeft(Scan()) { (s, event) =>
        event match
          case Event.Ferry(n, action)          => WriterTraces.ferry(s, n, action, keys, failed = "Fail")
          case Event.Read(n, k)                => s.emit(k, write("Read", "node" -> node(n)))
          case Event.Commit(n, k, length, ids) =>
            s.emit(k, write("Commit", "node" -> node(n), "length" -> Value.int(length), "ids" -> sequence(ids)))
          case Event.Project(n, k, length, marker, ids) =>
            s.emit(k, write("Project", ("node" -> node(n)) +: projection(length, marker, ids)*))
          case Event.Sweep(k, length, marker, ids) => s.emit(k, write("Sweep", projection(length, marker, ids)*))
          case Event.DeliverCommit(n, k, length, ids, applied, answered) =>
            s.emit(
              k,
              write(
                "DeliverCommit",
                "from"     -> node(n),
                "length"   -> Value.int(length),
                "ids"      -> sequence(ids),
                "applied"  -> Value.Bool(applied),
                "answered" -> Value.Bool(answered)
              )
            )
          case Event.DeliverProject(Some(n), k, length, marker, ids, applied, answered) =>
            s.emit(
              k,
              write(
                "DeliverProject",
                Seq("from" -> node(n)) ++ projection(length, marker, ids) ++
                  Seq("applied" -> Value.Bool(applied), "answered" -> Value.Bool(answered))*
              )
            )
          case Event.DeliverProject(None, k, length, marker, ids, applied, _) =>
            s.emit(k, write("DeliverSweep", projection(length, marker, ids) :+ ("applied" -> Value.Bool(applied))*))
          case Event.Throttle(who, k, commit, length, marker, ids) =>
            s.emit(
              k,
              write(
                "Throttle",
                Seq("from" -> from(who), "kind" -> Value.str(if commit then "commit" else "project")) ++
                  projection(length, marker, ids)*
              )
            )
      }
      .byKey(keys)

  /**
   * Checks one key's trace against the model with the writer's switches: the code's [[Writer]] flags are the model's
   * `FencedProject` and `EarlyAnswer`. The code always resumes, so `Resume` holds.
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
      "ProjectedWriterTrace",
      name,
      acts,
      commands,
      nodes,
      ListMap(
        "MaxCrashes"    -> WriterTraces.Unbounded.toString,
        "MaxLost"       -> WriterTraces.Unbounded.toString,
        "MaxThrottled"  -> WriterTraces.Unbounded.toString,
        "MaxGiveUps"    -> WriterTraces.Unbounded.toString,
        "FencedProject" -> WriterTraces.bool(model.fencedProject),
        "Resume"        -> "TRUE",
        "EarlyAnswer"   -> WriterTraces.bool(model.earlyAnswer)
      ),
      alter
    )

  /** Every key's trace of a run, checked: the verdicts by key. */
  def check(
    name: String,
    scenario: ProjectedWriterSimulation.Scenario,
    trace: ProjectedWriterSimulation.Trace,
    model: Writer = Writer()
  ): Map[Int, Tlc.Verdict] =
    val byKey = scenario.submissions.groupMap(_.command.key)(_.command.id).view.mapValues(_.toSet).toMap
    traces(trace.events, byKey.keySet).map { (k, acts) =>
      k -> validate(s"${name}_key$k", acts, byKey(k), ProjectedWriterSimulation.nodes, model)
    }
