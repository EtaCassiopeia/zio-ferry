package ferry.testkit.sim

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import zio.*

/**
 * A deterministic runtime for ZIO programs: every source of nondeterminism a run can meet is turned into an explicit,
 * recorded [[Decision]].
 *
 *   - '''Scheduling.''' Every fiber runs on one executor, driven from the calling thread. Whenever more than one fiber
 *     could run next, which one does is a decision.
 *   - '''Time.''' The clock is virtual. It advances only when nothing can run, straight to the earliest pending sleep.
 *     A sleep that is due competes with the runnable fibers as one more option, so "the timeout fires first" and "the
 *     answer arrives first" are both explored.
 *   - '''Anything else''' the program wants to leave open (a call's outcome, a latency) it asks for through
 *     [[Simulator.choose]], which is a decision too.
 *
 * A run is therefore a pure function of the program and the decision sequence: [[Simulator.run]] with
 * `Decisions.scripted(run.decisions.map(_.took))` reproduces the run exactly. Code that escapes the simulator (a
 * submission from another thread, for example from `ZIO.blocking` or a foreign thread pool) breaks that property; such
 * escapes are reported in [[Simulator.Run.escapes]] instead of being silently tolerated.
 */
object Simulator:

  /** The outcome of one run. `exit` is `None` when the program could no longer make progress: it is hung. */
  final case class Run[E, A](
    exit: Option[Exit[E, A]],
    decisions: Chunk[Decision],
    escapes: Chunk[String],
    steps: Long,
    elapsed: Duration
  ):
    def hung: Boolean = exit.isEmpty

    /** The script that replays this run. */
    def script: Chunk[Int] = decisions.map(_.took)

  /**
   * Asks the simulator to decide between `options` alternatives at `point`; outside a simulation, takes the first.
   * Programs use it for the nondeterminism they own, such as the outcome of a call to a simulated dependency.
   */
  def choose(point: String, options: Int): UIO[Int] =
    if options <= 1 then ZIO.succeed(0)
    else current.get.map(_.fold(0)(_.choose(point, options)))

  /** Chooses one of `values` (by [[choose]]). */
  def pick[A](point: String, values: NonEmptyChunk[A]): UIO[A] =
    choose(point, values.size).map(values(_))

  private val current: FiberRef[Option[Driver]] = Unsafe.unsafe(FiberRef.unsafe.make(Option.empty[Driver]))

  /**
   * Runs `program` to completion, or until it hangs or takes `maxSteps` steps (reported as hung), taking every decision
   * from `decisions`.
   */
  def run[E, A](program: ZIO[Any, E, A], decisions: Decisions, maxSteps: Long = 5_000_000L): Run[E, A] =
    val driver = Driver(decisions, Thread.currentThread())
    Unsafe.unsafe { implicit unsafe =>
      val runtime = Runtime.unsafe.fromLayer(Runtime.setExecutor(driver) ++ Runtime.setBlockingExecutor(driver))
      val fiber   = runtime.unsafe.fork(current.locally(Some(driver))(ZIO.withClock(driver.clock)(program)))
      val exit    = driver.drive(() => fiber.unsafe.poll, maxSteps)
      Run(exit, driver.log.result(), driver.escapes.result(), driver.steps, Duration.fromNanos(driver.now))
    }

  /** The executor, the clock and the decision log of one run. Not thread-safe by design: one thread drives it. */
  final private class Driver(decisions: Decisions, thread: Thread) extends Executor:

    private val runnable  = mutable.ArrayBuffer.empty[(Long, Runnable)]
    private val timers    = mutable.TreeMap.empty[(Long, Long), () => Unit]
    private var submitted = 0L
    private var timerSeq  = 0L

    var now: Long   = 0L
    var steps: Long = 0L
    val log         = Chunk.newBuilder[Decision]
    val escapes     = Chunk.newBuilder[String]

    def choose(point: String, options: Int): Int =
      val took = decisions.choose(point, options)
      log += Decision(point, options, took)
      took

    override def metrics(implicit unsafe: Unsafe): Option[zio.internal.ExecutionMetrics] = None

    override def submit(task: Runnable)(implicit unsafe: Unsafe): Boolean =
      if Thread.currentThread() ne thread then
        escapes.synchronized(escapes += s"submitted from ${Thread.currentThread().getName}"): Unit
      runnable.synchronized {
        submitted += 1
        runnable += (order(task, submitted) -> task)
      }: Unit
      true

    // Options are ordered by when the simulator first saw each fiber, which is the same in every run of the same program
    // and decisions. (Fiber ids are random, so they cannot be used.) Anything that is not a fiber is ordered by
    // submission.
    private val ordinals = java.util.IdentityHashMap[AnyRef, java.lang.Long]()

    private def order(task: Runnable, seq: Long): Long = task match
      case fiber: zio.internal.FiberRuntime[?, ?] =>
        ordinals.computeIfAbsent(fiber, _ => java.lang.Long.valueOf(ordinals.size.toLong)).longValue
      case _ => Long.MaxValue / 2 + seq

    private def addTimer(at: Long, fire: () => Unit): (Long, Long) =
      timerSeq += 1
      val key = (at, timerSeq)
      timers.update(key, fire)
      key

    def drive[A](done: () => Option[A], maxSteps: Long): Option[A] =
      @annotation.tailrec
      def loop(): Option[A] =
        done() match
          case finished @ Some(_)       => finished
          case None if steps > maxSteps => None
          case None                     =>
            val fibers = runnable.synchronized(runnable.sortInPlaceBy(_._1).toVector)
            val due    = timers.keysIterator.takeWhile(_._1 <= now).toVector
            val count  = fibers.size + due.size
            if count == 0 then
              timers.headOption match
                case Some(((at, _), _)) => now = at; loop()
                case None               => None // nothing can run and nothing will wake up: hung
            else
              steps += 1
              val i = if count == 1 then 0 else choose("schedule", count)
              if i < fibers.size then
                val (key, task) = fibers(i)
                runnable.synchronized(runnable -= (key -> task)): Unit
                task.run()
              else
                val key = due(i - fibers.size)
                timers.remove(key).foreach(_())
              loop()
      loop()

    val clock: Clock = new Clock:
      private def instantNow                                               = Instant.EPOCH.plusNanos(now)
      def currentTime(unit: => TimeUnit)(implicit trace: Trace): UIO[Long] =
        ZIO.succeed(unit.convert(now, TimeUnit.NANOSECONDS))
      def currentTime(unit: => ChronoUnit)(implicit trace: Trace, d: DummyImplicit): UIO[Long] =
        ZIO.succeed(unit.between(Instant.EPOCH, instantNow))
      def currentDateTime(implicit trace: Trace): UIO[OffsetDateTime] =
        ZIO.succeed(OffsetDateTime.ofInstant(instantNow, ZoneOffset.UTC))
      def instant(implicit trace: Trace): UIO[Instant]           = ZIO.succeed(instantNow)
      def javaClock(implicit trace: Trace): UIO[java.time.Clock] =
        ZIO.succeed(java.time.Clock.fixed(instantNow, ZoneOffset.UTC))
      def localDateTime(implicit trace: Trace): UIO[LocalDateTime] =
        ZIO.succeed(LocalDateTime.ofInstant(instantNow, ZoneOffset.UTC))
      def nanoTime(implicit trace: Trace): UIO[Long]       = ZIO.succeed(now)
      def scheduler(implicit trace: Trace): UIO[Scheduler] =
        ZIO.dieMessage("the simulator's clock has no Scheduler; use sleep")
      def sleep(duration: => Duration)(implicit trace: Trace): UIO[Unit] =
        ZIO.asyncInterrupt[Any, Nothing, Unit] { resume =>
          val key = addTimer(now + (duration.toNanos max 0L), () => resume(ZIO.unit))
          Left(ZIO.succeed(timers.remove(key)).unit)
        }
