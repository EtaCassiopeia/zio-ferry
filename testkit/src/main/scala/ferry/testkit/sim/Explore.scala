package ferry.testkit.sim

import java.util.SplittableRandom

import zio.Chunk

/**
 * Searching the space of decisions for new behaviour, the way coverage-guided fuzzers search the space of inputs.
 *
 * A run is a function of its decisions, so a run that reached something new can be revisited and varied: replayed up to
 * a point and continued afresh ([[Strategy.Fork]]), or replayed with a few of its decisions changed
 * ([[Strategy.Mutate]]). [[guided]] keeps every run that reached a fingerprint not seen before, and makes each new run
 * from one of them. [[random]] draws every run from a fresh seed. Both report how many distinct fingerprints have been
 * seen after each run, so they can be compared on the same budget: guidance is worth having only if it beats chance.
 */
object Explore:

  /** One run: its decisions, and the fingerprints of what it reached. */
  final case class Reached[F](decisions: Chunk[Decision], fingerprints: Set[F])

  enum Strategy:
    /** Replay a random prefix of a kept run, then choose at random. */
    case Fork

    /** Replay a kept run with up to three of its decisions chosen afresh, then choose at random past its end. */
    case Mutate

  /** Every run from a fresh seed. The distinct fingerprints seen after each run. */
  def random[F](runs: Int, seed: Long)(run: Decisions => Reached[F]): Vector[Int] =
    val r = SplittableRandom(seed)
    (1 to runs)
      .scanLeft(Set.empty[F])((seen, _) => seen ++ run(Decisions.seeded(r.nextLong())).fingerprints)
      .tail
      .map(_.size)
      .toVector

  final private case class Search[F](seen: Set[F], kept: Vector[Chunk[Decision]], curve: Vector[Int])

  /**
   * Each run varies a kept run by `strategy`; a share `fresh` of the runs, and every run until something is found,
   * start afresh instead. The distinct fingerprints seen after each run.
   */
  def guided[F](runs: Int, seed: Long, strategy: Strategy, fresh: Double = 0.1)(
    run: Decisions => Reached[F]
  ): Vector[Int] =
    val r                                        = SplittableRandom(seed)
    def vary(parent: Chunk[Decision]): Decisions = strategy match
      case Strategy.Fork =>
        Decisions.prefixed(parent.take(r.nextInt(parent.size + 1)).map(_.took), Decisions.seeded(r.nextLong()))
      case Strategy.Mutate =>
        val changed = (1 to 1 + r.nextInt(3)).foldLeft(parent) { (ds, _) =>
          if ds.isEmpty then ds
          else
            val i = r.nextInt(ds.size)
            ds.updated(i, ds(i).copy(took = r.nextInt(ds(i).options)))
        }
        Decisions.prefixed(changed.map(_.took), Decisions.seeded(r.nextLong()))
    (1 to runs)
      .foldLeft(Search[F](Set.empty, Vector.empty, Vector.empty)) { (s, _) =>
        val decisions =
          if s.kept.isEmpty || r.nextDouble() < fresh then Decisions.seeded(r.nextLong())
          else vary(s.kept(r.nextInt(s.kept.size)))
        val reached = run(decisions)
        val novel   = !reached.fingerprints.subsetOf(s.seen)
        val seen    = s.seen ++ reached.fingerprints
        Search(seen, if novel then s.kept :+ reached.decisions else s.kept, s.curve :+ seen.size)
      }
      .curve
