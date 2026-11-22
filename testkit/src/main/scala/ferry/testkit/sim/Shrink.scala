package ferry.testkit.sim

import zio.Chunk

/**
 * Shrinks a failing decision script to a smaller one that still fails.
 *
 * Every script describes a run (past its end, every choice takes the first option), so shrinking needs no knowledge of
 * the program: it tries shorter scripts and smaller choices, and keeps any that still fail. Greedy, deterministic and
 * bounded by `budget` runs. Candidates, in order:
 *   1. drop the tail, halving what is dropped until a single choice;
 *   2. set one choice to 0, the default continuation;
 *   3. lower one choice by one.
 */
object Shrink:

  def decisions(script: Chunk[Int], fails: Chunk[Int] => Boolean, budget: Int = 2000): Chunk[Int] =
    var best = script
    var left = budget

    def attempt(candidate: Chunk[Int]): Boolean =
      left > 0 && candidate != best && {
        left -= 1
        val failed = fails(candidate)
        if failed then best = candidate
        failed
      }

    var progress = true
    while progress && left > 0 do
      progress = false
      var drop = best.size
      while drop > 0 do if attempt(best.dropRight(drop)) then progress = true else drop /= 2
      var i = 0
      while i < best.size do
        if best(i) != 0 && attempt(best.updated(i, 0)) then progress = true
        else if best(i) > 1 && attempt(best.updated(i, best(i) - 1)) then progress = true
        i += 1
    best
