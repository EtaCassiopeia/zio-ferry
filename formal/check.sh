#!/usr/bin/env bash
# Model-checks the Ferry protocols with TLC. Each spec has one configuration that must pass, and weakened
# variants that must FAIL: each variant removes one mechanism, and its failure proves that mechanism is needed.
#
#   Ferry.tla      the protocol of Ferry.make and Ferry.perKey, action for action as ferry.Protocol implements it
#                  (safety and liveness): Ferry (perKey, idle keys called directly), FerryBatchOnly, FerryMake
#     FerryUnsorted                          -> inputs waiting together are sent out of rank order
#     FerryNoBusyGuard, FerryNoDirectGuard   -> a key has two calls in flight
#     FerryNonAtomicAdmission, FerryNoDrain  -> a caller is stranded after close
#   FencedWriter.tla  the single-writer-per-key deployment pattern, against a store with conditional appends
#     FencedWriterUnfenced, FencedWriterNoDedup                         -> a command is applied twice
#   ProjectedWriter.tla  the same pattern for a store that commits, then projects (ProjectedWrite); safety and liveness
#     ProjectedWriterUnfencedProject  -> an entry is projected twice
#     ProjectedWriterEarlyAnswer      -> a command is answered "projected" before it is in the view
#     ProjectedWriterNoResume         -> without retry and a sweeper, a projection cut short by a crash or a give-up is
#                                        never finished (liveness)
#   ProjectedWriterPartial.tla  the same, for a view of one row per entry (a per-entry ProjectedWrite); safety, and
#     liveness in ProjectedWriterPartialLiveness (one fault, no VIEW)
#     ProjectedWriterPartialUnconditionalRows -> a node's row and the sweeper's both land
#     ProjectedWriterPartialEarlyClear        -> the marker clears with a refused row missing
#     ProjectedWriterPartialUnfencedClear     -> a clear made for a shorter log clears the marker a later commit set
#     ProjectedWriterPartialNoResume          -> without reading again and a sweeper, a refused row is never written
#                                                (liveness)
#
# Usage: formal/check.sh [path/to/tla2tools.jar]   (defaults to $TLA2TOOLS or ./tla2tools.jar)
set -euo pipefail

cd "$(dirname "$0")"
JAR="${1:-${TLA2TOOLS:-tla2tools.jar}}"
META="$(mktemp -d)"
trap 'rm -rf "$META" ./*_TTrace_*.tla ./*_TTrace_*.bin' EXIT

tlc() { # spec config
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -deadlock -metadir "$META/$2" -config "$2.cfg" "$1.tla"
}

passes() { # spec config
  echo "== $2 (expected: no error)"
  tlc "$1" "$2" | tail -n 4
}

fails() { # spec config [expected TLC message]
  local expected="${3:-Invariant Safety is violated}"
  echo "== $2 (expected: $expected)"
  if out="$(tlc "$1" "$2")"; then
    echo "$out" | tail -n 4
    echo "FAIL: $2 was expected to report \"$expected\" but passed" >&2
    exit 1
  fi
  echo "$out" | grep -qE "$expected" || { echo "$out" | tail -n 20; exit 1; }
  echo "violated, as expected"
}

for config in Ferry FerryBatchOnly FerryMake; do passes Ferry "$config"; done
fails Ferry FerryUnsorted "Invariant InOrderPerKey is violated"
for variant in FerryNoBusyGuard FerryNoDirectGuard; do fails Ferry "$variant" "Invariant OneInFlightPerKey is violated"; done
for variant in FerryNonAtomicAdmission FerryNoDrain; do fails Ferry "$variant" "Invariant NoneStrandedAfterClose is violated"; done

passes FencedWriter FencedWriter
for variant in FencedWriterUnfenced FencedWriterNoDedup; do fails FencedWriter "$variant"; done

passes ProjectedWriter ProjectedWriter
for variant in ProjectedWriterUnfencedProject ProjectedWriterEarlyAnswer; do fails ProjectedWriter "$variant"; done
# Older TLC releases print "Temporal properties were violated"; newer releases name the property.
fails ProjectedWriter ProjectedWriterNoResume "Temporal propert(ies were|y EventuallyProjected was) violated"

passes ProjectedWriterPartial ProjectedWriterPartial
passes ProjectedWriterPartial ProjectedWriterPartialLiveness
# The variants list Safety's conjuncts one by one, so that each must break the one it is about.
fails ProjectedWriterPartial ProjectedWriterPartialUnconditionalRows "Invariant RowsAtMostOnce is violated"
for variant in ProjectedWriterPartialEarlyClear ProjectedWriterPartialUnfencedClear; do
  fails ProjectedWriterPartial "$variant" "Invariant MarkerHonest is violated"
done
fails ProjectedWriterPartial ProjectedWriterPartialNoResume "Temporal propert(ies were|y EventuallyProjected was) violated"
