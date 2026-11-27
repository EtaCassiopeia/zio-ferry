package ferry.bench

import zio.*

import ferry.{Ferry, FerryConfig, PerKeyConfig}

/**
 * Throughput and latency of a ferry under contention: 1 to 1024 concurrent callers, each making its share of the calls
 * back to back. The batch call does no work (or sleeps 1 ms), so the ferry's own cost is what is measured. The sharded
 * workloads split a per-key ferry into independent ferries by key hash ([[Sharded]]). Reports the median of the
 * measured rounds: calls per second, and the 50th and 99th percentile of one call's latency.
 *
 * Run: `core/Test/runMain ferry.bench.FerryBench`, forked with enough heap; `-Dbench.only=<text>` picks workloads.
 */
object FerryBench extends ZIOAppDefault:

  final case class Workload(name: String, build: URIO[Scope, Ferry[Int, Nothing, Int]], calls: Int = 200_000)

  private val total   = 200_000
  private val rounds  = 3
  private val callers = List(1, 16, 256, 1024)

  private def make(delay: Duration, call: NonEmptyChunk[Int] => UIO[Int]) =
    ZIO
      .fromEither(
        FerryConfig.make(maxBatchSize = 64, maxBatchDelay = delay, maxInFlightBatches = 4, maxQueueing = total)
      )
      .orDie
      .flatMap(cfg => Ferry.make(cfg)(call)((i, _) => i))

  private def perKey(keys: Int) =
    ZIO
      .fromEither(PerKeyConfig.make(maxBatchSize = 64, maxInFlightBatches = 8, maxQueueing = total))
      .orDie
      .flatMap(cfg =>
        Ferry.perKey(cfg, (i: Int) => i % keys)((_, batch: NonEmptyChunk[Int]) => ZIO.succeed(batch.size))((i, _) => i)
      )

  private def sharded(shards: Int, keys: Int, latency: Option[Duration]) =
    Sharded.perKey(shards, maxBatchSize = 64, maxInFlightBatches = 64, maxQueueing = total, (i: Int) => i % keys)(
      (_, batch: NonEmptyChunk[Int]) => ZIO.foreachDiscard(latency)(ZIO.sleep(_)).as(batch.size)
    )((i, _) => i)

  private val workloads = List(
    Workload("make, no-op call", make(Duration.Zero, b => ZIO.succeed(b.size))),
    Workload("perKey, 1000 keys", perKey(1000)),
    Workload("perKey, 4 hot keys", perKey(4)),
    Workload("make, 1 ms call", make(Duration.Zero, b => ZIO.sleep(1.milli).as(b.size)), 5_000),
    Workload("make, 1 ms call, 200 µs batch delay", make(200.micros, b => ZIO.sleep(1.milli).as(b.size)), 5_000),
    Workload("sharded perKey, 1000 keys, 1 shard", sharded(1, 1000, None)),
    Workload("sharded perKey, 1000 keys, 4 shards", sharded(4, 1000, None)),
    Workload("sharded perKey, 1000 keys, 8 shards", sharded(8, 1000, None)),
    Workload("sharded perKey, 1000 keys, 1 ms call, 1 shard", sharded(1, 1000, Some(1.milli)), 20_000),
    Workload("sharded perKey, 1000 keys, 1 ms call, 8 shards", sharded(8, 1000, Some(1.milli)), 20_000)
  )

  final case class Result(callsPerSecond: Double, p50: Long, p99: Long)

  private def once(w: Workload, n: Int, calls: Int): UIO[Result] =
    ZIO.scoped {
      w.build.flatMap { ferry =>
        val each      = (calls / n) max 1
        val latencies = new Array[Long](each * n)
        for
          start <- Clock.nanoTime
          _     <- ZIO.foreachParDiscard(0 until n) { c =>
            ZIO.foreachDiscard(0 until each) { j =>
              val i = c * each + j
              Clock.nanoTime.flatMap(t0 => ferry(i).ignore *> Clock.nanoTime.map(t1 => latencies(i) = t1 - t0))
            }
          }
          end <- Clock.nanoTime
        yield
          java.util.Arrays.sort(latencies)
          Result(
            latencies.length / ((end - start) / 1e9),
            latencies(latencies.length / 2),
            latencies(latencies.length * 99 / 100)
          )
      }
    }

  private def measure(w: Workload, n: Int): UIO[Result] =
    once(w, n, w.calls / 10) *> // warm-up
      ZIO.foreach(1 to rounds)(_ => once(w, n, w.calls)).map(rs => rs.sortBy(_.callsPerSecond).apply(rs.size / 2))

  private def us(nanos: Long) = f"${nanos / 1000.0}%.0f µs"

  def run =
    for
      _ <- Console.printLine(s"calls per run: up to $total, median of $rounds rounds").orDie
      only = Option(java.lang.System.getProperty("bench.only"))
      _ <- ZIO.foreachDiscard(workloads.filter(w => only.forall(w.name.contains))) { w =>
        Console.printLine(s"\n## ${w.name}\n| callers | calls/s | p50 | p99 |\n|---|---|---|---|").orDie *>
          ZIO.foreachDiscard(callers) { n =>
            measure(w, n).flatMap { r =>
              Console.printLine(f"| $n | ${r.callsPerSecond}%.0f | ${us(r.p50)} | ${us(r.p99)} |").orDie
            }
          }
      }
    yield ()
