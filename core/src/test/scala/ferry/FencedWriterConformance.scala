package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.FencedWriterSimulation.{Event, Writer}
import ferry.WriterTraces.{node, sequence, Scan}
import ferry.testkit.tla.{Alter, Tlc, Value}

/**
 * The mapping from a recorded run of [[FencedWriterSimulation]] to the steps of `formal/FencedWriter.tla`, and trace
 * validation of each key's steps against it (through `formal/FencedWriterTrace.tla`). The ferry's actions map as
 * [[WriterTraces]] says, with a failed call as `GiveUp`; the store's reads, deliveries and refusals are the model's own
 * `Read`, `Deliver` and `Throttle`.
 */
object FencedWriterConformance:

  /** Each key's steps, as the model's `act` records. */
  def traces(events: Chunk[Event], keys: Set[Int]): Map[Int, Chunk[Value]] =
    events
      .foldLeft(Scan()) { (s, event) =>
        event match
          case Event.Ferry(n, action)  => WriterTraces.ferry(s, n, action, keys, failed = "GiveUp")
          case Event.Read(n, k, fresh) =>
            s.emit(k, Value.rec("name" -> Value.str("Read"), "node" -> node(n), "ids" -> sequence(fresh)))
          case Event.Deliver(n, k, length, fresh, applied, answered) =>
            s.emit(
              k,
              Value.rec(
                "name"     -> Value.str("Deliver"),
                "from"     -> node(n),
                "length"   -> Value.int(length),
                "ids"      -> sequence(fresh),
                "applied"  -> Value.Bool(applied),
                "answered" -> Value.Bool(answered)
              )
            )
          case Event.Throttle(n, k, length, fresh) =>
            s.emit(
              k,
              Value.rec(
                "name"   -> Value.str("Throttle"),
                "from"   -> node(n),
                "length" -> Value.int(length),
                "ids"    -> sequence(fresh)
              )
            )
      }
      .byKey(keys)

  /**
   * Checks one key's trace against the model with the writer's switches: the code's [[Writer]] flags are the model's
   * `Fenced` and `Deduplicated`, so a weakened writer can be checked against the weakened model too.
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
      "FencedWriterTrace",
      name,
      acts,
      commands,
      nodes,
      ListMap(
        "MaxCrashes"   -> Conformance.Unbounded.toString,
        "MaxLost"      -> Conformance.Unbounded.toString,
        "MaxThrottled" -> Conformance.Unbounded.toString,
        "Fenced"       -> WriterTraces.bool(model.fenced),
        "Deduplicated" -> WriterTraces.bool(model.deduplicated)
      ),
      alter
    )

  /** Every key's trace of a run, checked: the verdicts by key. */
  def check(
    name: String,
    scenario: FencedWriterSimulation.Scenario,
    trace: FencedWriterSimulation.Trace,
    model: Writer = Writer()
  ): Map[Int, Tlc.Verdict] =
    val byKey = scenario.submissions.groupMap(_.command.key)(_.command.id).view.mapValues(_.toSet).toMap
    traces(trace.events, byKey.keySet).map { (k, acts) =>
      k -> validate(s"${name}_key$k", acts, byKey(k), FencedWriterSimulation.nodes, model)
    }
