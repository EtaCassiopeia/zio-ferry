package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.FencedWriterSimulation.{Event, Writer}
import ferry.Protocol.{Action, Outcome}
import ferry.testkit.tla.{Alter, Tlc, TraceValidation, Value}

/**
 * The mapping from a recorded run of [[FencedWriterSimulation]] to the steps of `formal/FencedWriter.tla`, and trace
 * validation of each key's steps against it (through `formal/FencedWriterTrace.tla`).
 *
 * The model is of one key, so a run is checked key by key: each key's trace holds the steps that name that key, and
 * every node's crash. The ferry's actions map onto the model's as follows; the store's steps are the model's own.
 *
 * | Ferry.perKey                      | the model                                                               |
 * |:----------------------------------|:------------------------------------------------------------------------|
 * | `Enqueue`                         | `Send`: the command is in the node's inbox                              |
 * | `Claim` (a direct call)           | `Send`, then `Take` of that one command                                 |
 * | `Take`                            | `Take`                                                                  |
 * | `Complete`, `CompleteDirect`      | `Reply`, `GiveUp` or `Abandon`, by outcome                              |
 * | `Close` (the node's scope closes) | `Crash`                                                                 |
 * | `Due`, `Refuse`                   | nothing: the model has no deadlines, and a refused send changes nothing |
 */
object FencedWriterConformance:

  /** What the scan of a run knows: each caller's key and command, and each running batch, by node. */
  final private case class Scan(
    commands: Map[(Int, Long), (Int, Int)] = Map.empty,
    running: Map[(Int, Int), (Int, Chunk[Int])] = Map.empty,
    acts: Chunk[(Int, Value)] = Chunk.empty
  ):
    def emit(key: Int, act: Value*): Scan = copy(acts = acts ++ act.map(key -> _))

  /** Each key's steps, as the model's `act` records. */
  def traces(events: Chunk[Event], keys: Set[Int]): Map[Int, Chunk[Value]] =
    val scan = events.foldLeft(Scan()) { (s, event) =>
      event match
        case Event.Ferry(n, action) =>
          action match
            case Action.Enqueue(c, k, _, id) =>
              s.copy(commands = s.commands + ((n, c) -> (k, id.toInt))).emit(k, send(id.toInt, n))
            case Action.Claim(c, k, _, id) =>
              s.copy(commands = s.commands + ((n, c) -> (k, id.toInt)))
                .emit(k, send(id.toInt, n), take(n, Chunk(id.toInt)))
            case Action.Take(w, k, batch) =>
              val commands = batch.map(c => s.commands((n, c))._2)
              s.copy(running = s.running + ((n, w) -> (k, commands))).emit(k, take(n, commands))
            case Action.Complete(w, outcomes) =>
              val (k, commands) = s.running((n, w))
              val outcome       = outcomes.headOption.getOrElse(throw IllegalStateException(s"empty $action"))._2
              s.copy(running = s.running - ((n, w))).emit(k, finish(n, commands, outcome))
            case Action.CompleteDirect(c, outcome) =>
              val (k, id) = s.commands((n, c))
              s.emit(k, finish(n, Chunk(id), outcome))
            case Action.Close(drained) =>
              val gone = drained.map(c => s.commands((n, c)))
              keys.foldLeft(s) { (s, k) =>
                s.emit(
                  k,
                  Value.rec(
                    "name"    -> Value.str("Crash"),
                    "node"    -> nodeName(n),
                    "drained" -> Value.Set(gone.collect { case (`k`, id) => Value.int(id) }.toSet)
                  )
                )
              }
            case Action.Due(_) | Action.Refuse(_, _, _, _) => s
            case other                                     =>
              throw IllegalStateException(s"the writer's clients never give up and are never rejected, yet: $other")
        case Event.Read(n, k, fresh) =>
          s.emit(k, Value.rec("name" -> Value.str("Read"), "node" -> nodeName(n), "ids" -> sequence(fresh)))
        case Event.Deliver(n, k, length, fresh, applied, answered) =>
          s.emit(
            k,
            Value.rec(
              "name"     -> Value.str("Deliver"),
              "from"     -> nodeName(n),
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
              "from"   -> nodeName(n),
              "length" -> Value.int(length),
              "ids"    -> sequence(fresh)
            )
          )
    }
    keys.map(k => k -> scan.acts.collect { case (`k`, act) => act }).toMap

  private def finish(node: Int, batch: Chunk[Int], outcome: Outcome): Value =
    val name = outcome match
      case Outcome.Succeeded   => "Reply"
      case Outcome.Failed      => "GiveUp"
      case Outcome.Interrupted => "Abandon"
    Value.rec("name" -> Value.str(name), "node" -> nodeName(node), "batch" -> ids(batch))

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
    Tlc.formal match
      case None         => Tlc.Verdict.Skipped("no formal/ directory above the working directory")
      case Some(formal) =>
        val trace = TraceValidation.Trace(
          name,
          definitions = ListMap(
            "TraceLog"      -> Value.Seq(acts.toVector),
            "TraceCommands" -> Value.Set(commands.map(Value.int(_))),
            "TraceNodes"    -> Value.Set((0 until nodes).map(nodeName).toSet)
          ),
          substitutions = ListMap("Commands" -> "TraceCommands", "Nodes" -> "TraceNodes"),
          assignments = ListMap(
            "MaxCrashes"   -> Conformance.Unbounded.toString,
            "MaxLost"      -> Conformance.Unbounded.toString,
            "MaxThrottled" -> Conformance.Unbounded.toString,
            "Fenced"       -> (if model.fenced then "TRUE" else "FALSE"),
            "Deduplicated" -> (if model.deduplicated then "TRUE" else "FALSE")
          )
        )
        TraceValidation.check(formal, "FencedWriterTrace", trace, alter)

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

  private def nodeName(n: Int): Value = Value.str(s"n$n")

  private def send(id: Int, n: Int) =
    Value.rec("name" -> Value.str("Send"), "command" -> Value.int(id), "node" -> nodeName(n))

  private def take(n: Int, batch: Chunk[Int]) =
    Value.rec("name" -> Value.str("Take"), "node" -> nodeName(n), "batch" -> ids(batch))

  private def ids(batch: Chunk[Int]): Value = Value.Set(batch.map(Value.int(_)).toSet)

  private def sequence(fresh: Chunk[Int]): Value = Value.Seq(fresh.map(Value.int(_)).toVector)
