package ferry.testkit.sim

import java.util.SplittableRandom

/**
 * One choice the simulator made: at `point`, among `options` alternatives, it took the one at index `took`, at its
 * `step`-th scheduling step and virtual time `at` (nanoseconds). `chose` says what the option was, for people: which
 * fiber ran, which timer fired.
 *
 * A run of the simulator is a pure function of its program and its sequence of decisions. Recording the decisions is
 * therefore enough to replay a run exactly, and changing one of them is how a run is forked, shrunk or steered.
 */
final case class Decision(point: String, options: Int, took: Int, step: Long = 0L, at: Long = 0L, chose: String = ""):
  require(options >= 2 && took >= 0 && took < options, s"bad decision $this")

  override def toString: String = s"$point:$took/$options"

/**
 * Where the simulator's decisions come from. Only choices between two or more options are asked for: a step with one
 * possible continuation is not a decision and is never recorded.
 */
trait Decisions:
  def choose(point: String, options: Int): Int

object Decisions:

  /** Uniform choices from a seeded generator: a fresh, reproducible exploration. */
  def seeded(seed: Long): Decisions =
    val random = SplittableRandom(seed)
    (_, options) => random.nextInt(options)

  /**
   * Replays `script`, then continues with the first option of every further choice. A recorded decision that is out of
   * range for the run it is replayed into (because an earlier change altered the run) is clamped to the last option.
   * Taking the first option is the "default" continuation, so a script that has been shortened still describes a run.
   */
  def scripted(script: Seq[Int]): Decisions =
    val it = script.iterator
    (_, options) => if it.hasNext then it.next() min (options - 1) else 0

  /** The first option, always: the run with no decisions taken. */
  val first: Decisions = (_, _) => 0

  /**
   * Replays `prefix` exactly, then hands every further choice to `rest`: how a run is forked. A recorded run's first
   * decisions, with the last of them changed, followed by fresh choices, is a branch of that run.
   */
  def prefixed(prefix: Seq[Int], rest: Decisions): Decisions =
    val it = prefix.iterator
    (point, options) => if it.hasNext then it.next() min (options - 1) else rest.choose(point, options)
