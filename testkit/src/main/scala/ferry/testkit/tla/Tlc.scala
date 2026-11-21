package ferry.testkit.tla

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters.*

/**
 * Runs TLC as a subprocess. Everything that asks TLC a question answers with a [[Tlc.Verdict]]: accepted, rejected
 * (with what TLC found), skipped (TLC is not available here: never a pass), or a tool error (TLC itself failed: never a
 * pass either).
 */
object Tlc:

  enum Verdict:
    case Accepted
    case Rejected(reason: String)
    case Skipped(reason: String)
    case ToolError(output: String)

    def accepted: Boolean = this == Accepted

  final case class Output(exitCode: Int, text: String)

  /**
   * Where `tla2tools.jar` is: `$TLA2TOOLS`, the `tla2tools` system property, `tla2tools.jar` at the root of the
   * repository (where CI downloads it), or the usual local installs.
   */
  def jar: Option[Path] =
    val candidates =
      sys.env.get("TLA2TOOLS").toList ++ sys.props.get("tla2tools").toList ++
        root.map(_.resolve("tla2tools.jar").toString).toList ++
        List(".local/share/tlaplus/tla2tools.jar", "tla2tools.jar").map(p => s"${sys.props("user.home")}/$p")
    candidates.map(Paths.get(_)).find(Files.isRegularFile(_))

  /** The repository root: the nearest directory, from the working directory up, that holds `formal/`. */
  def root: Option[Path] =
    Iterator
      .iterate(Option(Paths.get("").toAbsolutePath))(_.flatMap(p => Option(p.getParent)))
      .takeWhile(_.isDefined)
      .flatten
      .find(p => Files.isDirectory(p.resolve("formal")))

  /** The repository's `formal/` directory. */
  def formal: Option[Path] = root.map(_.resolve("formal"))

  /** Runs TLC on `spec` with `config` in `dir`, with a throwaway metadata directory. `None` if TLC is not available. */
  def run(dir: Path, spec: String, config: String, args: Seq[String] = Nil): Option[Output] =
    jar.map { tools =>
      val meta = Files.createTempDirectory("tlc-meta")
      try
        val java    = Paths.get(sys.props("java.home"), "bin", "java").toString
        val command = Seq(java, "-XX:+UseParallelGC", "-cp", tools.toString, "tlc2.TLC", "-workers", "1") ++
          Seq("-metadir", meta.toString, "-config", config, "-noGenerateSpecTE") ++ args :+ spec
        val process = ProcessBuilder(command.asJava).directory(dir.toFile).redirectErrorStream(true).start()
        val text    = String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
        Output(process.waitFor(), text)
      finally deleteRecursively(meta)
    }

  def deleteRecursively(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists(_))

  /** The lines that matter in TLC's output, for a failure message. */
  def summary(text: String, lines: Int = 40): String =
    text.linesIterator
      .filterNot(l => l.startsWith("Parsing file") || l.startsWith("Semantic processing") || l.startsWith("Linting"))
      .toVector
      .takeRight(lines)
      .mkString("\n")
