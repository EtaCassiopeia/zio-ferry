package ferry.testkit.tla

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

/**
 * A change to the text of a copy of the specs, keyed by module file name. Used to check a deliberately drifted model
 * without touching the real one: the drift fixtures show that a model that no longer describes the code is caught.
 */
final case class Alter(changes: Map[String, String => String]):

  /** Replaces `before` with `after` in `module`, and fails loudly if `before` is not there. */
  def replace(module: String, before: String, after: String): Alter =
    Alter(
      changes.updated(
        module,
        changes.getOrElse(module, identity[String]).andThen { text =>
          require(text.contains(before), s"$module does not contain the text to alter:\n$before")
          text.replace(before, after)
        }
      )
    )

object Alter:
  val none: Alter = Alter(Map.empty)

  /** Copies every module of `from` into `to`, applying `alter`. */
  def copy(from: Path, to: Path, alter: Alter): Unit =
    Files.list(from).iterator().asScala.filter(_.toString.endsWith(".tla")).foreach { spec =>
      val name = spec.getFileName.toString
      val text = alter.changes.get(name).fold(Files.readString(spec))(_(Files.readString(spec)))
      Files.writeString(to.resolve(name), text, StandardCharsets.UTF_8)
    }
