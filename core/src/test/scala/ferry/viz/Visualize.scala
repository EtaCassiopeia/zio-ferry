package ferry.viz

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import zio.Chunk

import ferry.{Engine, FerrySimulation}
import ferry.FerrySimulation.Family
import ferry.Protocol.Action
import ferry.check.ProtocolSpace

/**
 * Writes the branches of one simulated scenario for people to look at:
 *   - `multiverse.html`: a self-contained page, with the tree of branches, each branch's timeline (callers, workers,
 *     keys, and the simulator's decisions), and what changed from its parent;
 *   - `multiverse.json`: the same data;
 *   - `<branch>.perfetto.json`: one branch as Chrome trace events, to open in https://ui.perfetto.dev.
 *
 * {{{
 *   sbt "core/Test/runMain ferry.viz.Visualize PerKeyBatched 7 target/viz"
 *   sbt "core/Test/runMain ferry.viz.Visualize PerKeyBatched 7 target/viz-drift KeepCancelledInLane"
 * }}}
 * No database is involved: a run is a list of decisions and a list of events, and a branch is a run that shares a
 * prefix of decisions with its parent, so a tree of runs is plain data.
 */
object Visualize:

  def main(args: Array[String]): Unit =
    val family   = args.headOption.fold(Family.PerKeyBatched)(Family.valueOf)
    val seed     = args.lift(1).fold(7L)(_.toLong)
    val out      = Paths.get(args.lift(2).getOrElse("target/viz"))
    val scenario = FerrySimulation.scenario(family, seed)
    val rules    = args.lift(3).fold(Engine.Rules.standard)(drifted)
    val explored = Multiverse.explore(scenario, seed, rules = rules)
    val steered  =
      Multiverse.steer(scenario, Multiverse.failedThenServed, rules = rules).map { (found, script, result) =>
        val verdict = result.observation.map(o => ProtocolSpace.replay(scenario.limits, o.trace, rules))
        Multiverse.Branch(
          explored.size,
          None,
          s"steered: a call fails, then its key is served again (found at seed $found, pinned by ${script.size} decisions)",
          None,
          script.size,
          result,
          FerrySimulation.violations(result),
          verdict
        )
      }
    val branches = explored ++ steered
    Files.createDirectories(out)
    val data =
      json(s"$family, seed $seed${args.lift(3).fold("")(r => s", engine rules: $r")}", scenario.toString, branches)
    write(out.resolve("multiverse.json"), data)
    write(out.resolve("multiverse.html"), page(data))
    branches
      .filter(b => b.parent.isEmpty)
      .foreach(b => write(out.resolve(s"branch-${b.id}.perfetto.json"), perfetto(b)))
    println(s"${branches.size} branches written to ${out.toAbsolutePath}")

  /** The deliberately wrong rules of [[ferry.DriftSpec]], to see what rejected branches look like. */
  private def drifted(name: String): Engine.Rules = name match
    case "ServeLastReady"      => ferry.DriftSpec.ServeLastReady
    case "IgnoreWeightLimit"   => ferry.DriftSpec.IgnoreWeightLimit
    case "KeepCancelledInLane" => ferry.DriftSpec.KeepCancelledInLane
    case other                 => sys.error(s"no such rules: $other")

  private def write(path: Path, text: String): Unit = Files.writeString(path, text, StandardCharsets.UTF_8): Unit

  /** The viewer, with the data inlined so the page needs nothing else. */
  def page(data: String): String =
    val template = String(getClass.getResourceAsStream("/viz/multiverse.html").readAllBytes(), StandardCharsets.UTF_8)
    val body     = template.replace("/*__DATA__*/null", data.replace("</", "<\\/"))
    // The template is the page's content; the document around it lets the file open on its own.
    s"""<!doctype html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"></head><body>\n$body\n</body></html>\n"""

  def json(title: String, scenario: String, branches: Vector[Multiverse.Branch]): String =
    def branch(b: Multiverse.Branch) =
      obj(
        "id"         -> b.id.toString,
        "parent"     -> b.parent.fold("null")(_.toString),
        "label"      -> str(b.label),
        "forkAt"     -> b.forkAt.fold("null")(_.toString),
        "forced"     -> b.forced.toString,
        "verdict"    -> str(verdictText(b.verdict)),
        "accepted"   -> b.verdict.exists(_.conforms).toString,
        "violations" -> arr(b.violations.map(str)),
        "hung"       -> b.result.run.hung.toString,
        "decisions"  -> arr(b.decisions.zipWithIndex.map { (d, i) =>
          obj(
            "i"       -> i.toString,
            "step"    -> d.step.toString,
            "at"      -> d.at.toString,
            "point"   -> str(d.point),
            "options" -> d.options.toString,
            "took"    -> d.took.toString,
            "chose"   -> str(d.chose)
          )
        }),
        "events" -> arr(b.events.zipWithIndex.map { (e, i) =>
          obj(
            "i"    -> i.toString,
            "step" -> e.step.toString,
            "at"   -> e.at.toString,
            "act"  -> str(e.action.toString)
          )
        })
      )
    obj("title" -> str(title), "scenario" -> str(scenario), "branches" -> arr(Chunk.fromIterable(branches.map(branch))))

  /**
   * One branch as Chrome trace events. The time axis is logical (each event 1 ms apart, in the order they took effect)
   * because many events share one instant of virtual time; the virtual time is in each event's arguments.
   */
  def perfetto(b: Multiverse.Branch): String =
    val events                    = b.events.zipWithIndex
    def ts(i: Int)                = (i * 1000).toString
    def track(name: String)       = str(name)
    val spans                     = Vector.newBuilder[String]
    val open                      = collection.mutable.Map.empty[String, Int]
    def begin(id: String, i: Int) = open.update(id, i)
    def end(id: String, i: Int, name: String, tid: String, args: String): Unit =
      open.remove(id).foreach { start =>
        spans += obj(
          "name" -> str(name),
          "ph"   -> str("X"),
          "pid"  -> "1",
          "tid"  -> track(tid),
          "ts"   -> ts(start),
          "dur"  -> ((i - start).max(1) * 1000).toString,
          "args" -> args
        ): Unit
      }
    def instant(name: String, i: Int, tid: String, args: String): Unit =
      spans += obj(
        "name" -> str(name),
        "ph"   -> str("i"),
        "s"    -> str("t"),
        "pid"  -> "1",
        "tid"  -> track(tid),
        "ts"   -> ts(i),
        "args" -> args
      ): Unit
    events.foreach { (e, i) =>
      val args = obj("virtual time (ms)" -> (e.at / 1e6).toString, "act" -> str(e.action.toString))
      e.action match
        case Action.Enqueue(c, _, _, _) => begin(s"c$c", i)
        case Action.Claim(c, _, _, _)   => begin(s"c$c", i); begin(s"d$c", i)
        case Action.Reject(c, _, _, _)  => instant(s"c$c rejected", i, s"caller c$c", args)
        case Action.Refuse(c, _, _, _)  => instant(s"c$c refused", i, s"caller c$c", args)
        case Action.Due(k)              => instant(s"$k due", i, s"key $k", args)
        case Action.Take(w, k, batch)   => begin(s"w$w", i); instant(s"take ${batch.mkString(",")}", i, s"key $k", args)
        case Action.Complete(w, outcomes) =>
          end(s"w$w", i, s"batch ${outcomes.map(_._1).mkString(",")}", s"worker w$w", args)
          outcomes.foreach((c, o) => end(s"c$c", i, s"c$c $o", s"caller c$c", args))
        case Action.CompleteDirect(c, o) =>
          end(s"d$c", i, s"direct c$c", s"direct c$c", args); end(s"c$c", i, s"c$c $o", s"caller c$c", args)
        case Action.Cancel(c)      => end(s"c$c", i, s"c$c gave up", s"caller c$c", args)
        case Action.Close(drained) =>
          instant("close", i, "scope", args);
          drained.foreach(c => end(s"c$c", i, s"c$c interrupted", s"caller c$c", args))
    }
    obj("traceEvents" -> arr(Chunk.fromIterable(spans.result())), "displayTimeUnit" -> str("ms"))

  private def verdictText(v: Option[ProtocolSpace.Replay]): String = v match
    case Some(ProtocolSpace.Replay.Conforms(steps))             => s"every property held over $steps steps"
    case Some(ProtocolSpace.Replay.Violated(step, property, _)) => s"$property broken at step $step"
    case Some(ProtocolSpace.Replay.Diverged(step, recorded, _)) =>
      s"diverged from the protocol at step $step ($recorded)"
    case None => "the run did not finish"

  private def str(s: String): String =
    "\"" + s.flatMap {
      case '"'          => "\\\""
      case '\\'         => "\\\\"
      case '\n'         => "\\n"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c            => c.toString
    } + "\""
  private def arr(xs: Chunk[String]): String         = xs.mkString("[", ",", "]")
  private def obj(fields: (String, String)*): String = fields.map((k, v) => s"${str(k)}:$v").mkString("{", ",", "}")
