package ferry

import scala.collection.immutable.ListMap

import zio.Chunk

import ferry.Protocol.{Action, Outcome}
import ferry.testkit.tla.{Alter, Tlc, TraceValidation, Value}

/**
 * What the writer models share with their simulations: how each node's `Ferry.perKey` actions are written as the
 * model's steps, key by key, and how a key's steps are checked with TLC.
 *
 * The models are of one key, so a run is checked key by key: each key's trace holds the steps that name that key, and
 * every node's crash. The ferry's actions map onto the models' steps as follows; the store's steps are each model's
 * own.
 *
 * | Ferry.perKey                      | the model                                                                 |
 * |:----------------------------------|:--------------------------------------------------------------------------|
 * | `Enqueue`                         | `Send`: the command is in the node's inbox                                |
 * | `Claim` (a direct call)           | `Send`, then `Take` of that one command                                   |
 * | `Take`                            | `Take`                                                                    |
 * | `Complete`, `CompleteDirect`      | by outcome: `Reply`; the model's failure step; `Abandon`                  |
 * | `Close` (the node's scope closes) | `Crash`                                                                   |
 * | `Due`, `Refuse`                   | nothing: the models have no deadlines, and a refused send changes nothing |
 *
 * The rank of each input is its command's id, which is how a caller is named by its command.
 */
object WriterTraces:

  /** TLC computes with 32-bit integers: unbounded limits are written as this. */
  val Unbounded: Long = 100_000_000L

  /** What a scan of a run knows: each caller's key and command, and each running batch, by node; and the steps. */
  final case class Scan(
    commands: Map[(Int, Long), (Int, Int)] = Map.empty,
    running: Map[(Int, Int), (Int, Chunk[Int])] = Map.empty,
    acts: Chunk[(Int, Value)] = Chunk.empty
  ):
    def emit(key: Int, act: Value*): Scan = copy(acts = acts ++ act.map(key -> _))

    /** Each key's steps. */
    def byKey(keys: Set[Int]): Map[Int, Chunk[Value]] = keys.map(k => k -> acts.collect { case (`k`, a) => a }).toMap

  /** Node `n`'s action, as the model's steps; `failed` names the model's step for a call that failed. */
  def ferry(s: Scan, n: Int, action: Action[Int], keys: Set[Int], failed: String): Scan =
    def finish(batch: Chunk[Int], outcome: Outcome) =
      val name = outcome match
        case Outcome.Succeeded   => "Reply"
        case Outcome.Failed      => failed
        case Outcome.Interrupted => "Abandon"
      Value.rec("name" -> Value.str(name), "node" -> node(n), "batch" -> ids(batch))
    action match
      case Action.Enqueue(c, k, _, id) =>
        s.copy(commands = s.commands + ((n, c) -> (k, id.toInt))).emit(k, send(id.toInt, n))
      case Action.Claim(c, k, _, id) =>
        s.copy(commands = s.commands + ((n, c) -> (k, id.toInt))).emit(k, send(id.toInt, n), take(n, Chunk(id.toInt)))
      case Action.Take(w, k, batch) =>
        val commands = batch.map(c => s.commands((n, c))._2)
        s.copy(running = s.running + ((n, w) -> (k, commands))).emit(k, take(n, commands))
      case Action.Complete(w, outcomes) =>
        val (k, commands) = s.running((n, w))
        val outcome       = outcomes.headOption.getOrElse(throw IllegalStateException(s"empty $action"))._2
        s.copy(running = s.running - ((n, w))).emit(k, finish(commands, outcome))
      case Action.CompleteDirect(c, outcome) =>
        val (k, id) = s.commands((n, c))
        s.emit(k, finish(Chunk(id), outcome))
      case Action.Close(drained) =>
        val gone = drained.map(c => s.commands((n, c)))
        keys.foldLeft(s) { (s, k) =>
          s.emit(
            k,
            Value.rec(
              "name"    -> Value.str("Crash"),
              "node"    -> node(n),
              "drained" -> Value.Set(gone.collect { case (`k`, id) => Value.int(id) }.toSet)
            )
          )
        }
      case Action.Due(_) | Action.Refuse(_, _, _, _) => s
      case other                                     =>
        throw IllegalStateException(s"the writers' clients never give up and are never rejected, yet: $other")

  /**
   * Checks one key's steps against `companion` (a trace module of a writer model): its commands, `nodes` nodes, and the
   * model's other constants as `assignments`.
   */
  def validate(
    companion: String,
    name: String,
    acts: Chunk[Value],
    commands: Set[Int],
    nodes: Int,
    assignments: ListMap[String, String],
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
            "TraceNodes"    -> Value.Set((0 until nodes).map(node).toSet)
          ),
          substitutions = ListMap("Commands" -> "TraceCommands", "Nodes" -> "TraceNodes"),
          assignments = assignments
        )
        TraceValidation.check(formal, companion, trace, alter)

  def node(n: Int): Value = Value.str(s"n$n")

  def ids(batch: Iterable[Int]): Value = Value.Set(batch.map(Value.int(_)).toSet)

  def sequence(ids: Iterable[Int]): Value = Value.Seq(ids.map(Value.int(_)).toVector)

  def bool(b: Boolean): String = if b then "TRUE" else "FALSE"

  private def send(id: Int, n: Int) =
    Value.rec("name" -> Value.str("Send"), "command" -> Value.int(id), "node" -> node(n))

  private def take(n: Int, batch: Chunk[Int]) =
    Value.rec("name" -> Value.str("Take"), "node" -> node(n), "batch" -> ids(batch))
