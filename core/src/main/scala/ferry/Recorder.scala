package ferry

import zio.*
import zio.stm.*

/**
 * The seam between the protocol and its trace. [[Engine]] hands every step's actions to its recorder inside the STM
 * transaction that makes the step, so a recorded trace is in the order the steps took effect: no sequence numbers, no
 * clock, nothing to reorder.
 *
 * In production the recorder is [[Recorder.off]] and costs nothing. Tests record into a `TRef` and check the trace
 * against the TLA+ model.
 */
private[ferry] trait Recorder[-K]:
  def record(actions: Chunk[Protocol.Action[K]]): USTM[Unit]

private[ferry] object Recorder:

  val off: Recorder[Any] = _ => STM.unit

  /** Records into a `TRef`; `trace` reads what has been recorded so far. */
  final class InMemory[K](log: TRef[Chunk[Protocol.Action[K]]]) extends Recorder[K]:
    def record(actions: Chunk[Protocol.Action[K]]): USTM[Unit] = log.update(_ ++ actions)
    def trace: UIO[Chunk[Protocol.Action[K]]]                  = log.get.commit

  def inMemory[K]: UIO[InMemory[K]] = TRef.make(Chunk.empty[Protocol.Action[K]]).commit.map(InMemory(_))
