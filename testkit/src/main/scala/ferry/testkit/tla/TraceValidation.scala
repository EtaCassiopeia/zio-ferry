package ferry.testkit.tla

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*

/**
 * Checks a recorded run of the code against a TLA+ spec (trace validation, after Cirstea, Kuppe, Loillier and Merz,
 * SEFM 2024).
 *
 * The spec under test must have a trace companion, a module that `EXTENDS` it and declares
 * {{{
 *   CONSTANT Trace        \* the recorded events, a sequence of records
 *   VARIABLE i            \* how many have been matched
 *   TraceInit, TraceNext  \* the spec's Init, then one Next step per event, requiring the step's `act` to be the event
 *   Unfinished            \* i < Len(Trace)
 * }}}
 * The trace is written as a module that extends the companion and defines `Trace` and the instance (constants), and TLC
 * explores it with `Unfinished` as an invariant. TLC reporting that `Unfinished` is violated means it reached the end
 * of the trace: the run is a behaviour of the spec. TLC finishing with no error means it could not: the run is
 * rejected, after as many events as the depth of its search, less one.
 */
object TraceValidation:

  /**
   * One trace to check.
   *
   * @param definitions
   *   Operators the trace module defines: at least `TraceLog`, the events. They are substituted for the spec's
   *   constants through `substitutions`.
   * @param substitutions
   *   `constant -> definition` pairs, written as `constant <- definition` in the configuration.
   * @param assignments
   *   `constant -> value` pairs, written as `constant = value`: model values and switches.
   */
  final case class Trace(
    name: String,
    definitions: ListMap[String, Value],
    substitutions: ListMap[String, String],
    assignments: ListMap[String, String]
  )

  /** Writes `trace` into `dir`, as `<name>.tla` and `<name>.cfg`, and returns the two paths. */
  def write(dir: Path, companion: String, trace: Trace): (Path, Path) =
    val module = dir.resolve(s"${trace.name}.tla")
    val config = dir.resolve(s"${trace.name}.cfg")
    val defs   = trace.definitions.map((n, v) => s"$n ==\n  ${v.render}").mkString("\n\n")
    Files.writeString(
      module,
      s"""---- MODULE ${trace.name} ----
         |\\* A recorded run of the code, written by ferry.testkit.tla.TraceValidation.
         |EXTENDS $companion
         |
         |$defs
         |====
         |""".stripMargin,
      StandardCharsets.UTF_8
    )
    val constants =
      trace.assignments.map((c, v) => s"  $c = $v") ++ trace.substitutions.map((c, d) => s"  $c <- $d")
    Files.writeString(
      config,
      s"""CONSTANTS
         |${constants.mkString("\n")}
         |  Trace <- TraceLog
         |
         |INIT TraceInit
         |NEXT TraceNext
         |INVARIANT Unfinished
         |""".stripMargin,
      StandardCharsets.UTF_8
    )
    (module, config)

  /**
   * Checks `trace` against `companion`, a module in `formal`. The check runs in a scratch directory holding a copy of
   * every module in `formal`, so concurrent checks never share files and nothing is left in the repository.
   */
  def check(formal: Path, companion: String, trace: Trace): Tlc.Verdict =
    if Tlc.jar.isEmpty then Tlc.Verdict.Skipped("TLC not found: set TLA2TOOLS or put tla2tools.jar at the root")
    else
      val scratch = Files.createTempDirectory("trace-validation")
      try
        Files.list(formal).iterator().asScala.filter(_.toString.endsWith(".tla")).foreach { spec =>
          Files.copy(spec, scratch.resolve(spec.getFileName))
        }
        val (module, config) = write(scratch, companion, trace)
        verdict(Tlc.run(scratch, module.getFileName.toString, config.getFileName.toString, Seq("-deadlock")))
      finally Tlc.deleteRecursively(scratch)

  private val Depth = """The depth of the complete state graph search is (\d+)""".r.unanchored

  def verdict(output: Option[Tlc.Output]): Tlc.Verdict = output match
    case None                                                                 => Tlc.Verdict.Skipped("TLC not found")
    case Some(out) if out.text.contains("Invariant Unfinished is violated")   => Tlc.Verdict.Accepted
    case Some(out) if out.text.contains("Model checking completed. No error") =>
      out.text match
        case Depth(depth) =>
          Tlc.Verdict.Rejected(
            s"no behaviour of the spec matches event ${depth.toInt} (the first ${depth.toInt - 1} match)"
          )
        case _ => Tlc.Verdict.Rejected("no behaviour of the spec matches the trace")
    case Some(out) => Tlc.Verdict.ToolError(Tlc.summary(out.text))
