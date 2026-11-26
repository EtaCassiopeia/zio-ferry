package ferry.testkit.check

import scala.collection.mutable

/**
 * An explicit-state checker for a pure state machine: every state reachable from `init` through `next` is visited once,
 * breadth first, so a counterexample is a shortest path.
 *
 * Two kinds of property are checked:
 *   - '''invariants''': a predicate that must hold in every reachable state;
 *   - '''settling''': in every reachable state where no ''progress'' step is enabled (only the environment could still
 *     act, and it need not), `settled` must hold. With an acyclic state graph, every run of progress steps ends, so
 *     this is the liveness property "once the environment stops, the system reaches a settled state". The graph is
 *     checked for cycles; a cycle is reported, because it would make the settling check unsound.
 *
 * States must have structural equality; labels name the steps in a counterexample.
 */
object StateSpace:

  /** One step of a counterexample: the step's label and the state it led to. */
  final case class Step[S, A](label: A, state: S)

  enum Verdict[S, A]:
    case Holds(states: Int, steps: Int)
    case Violated(property: String, init: S, path: List[Step[S, A]])
    case Unsettled(init: S, path: List[Step[S, A]])
    case Cycle(init: S, path: List[Step[S, A]])
    case TooLarge(limit: Int)

    def holds: Boolean = this match
      case Holds(_, _) => true
      case _           => false

    def describe(show: S => String = (s: S) => s.toString): String = this match
      case Holds(states, steps)           => s"holds: $states states, $steps steps"
      case Violated(property, init, path) => s"$property violated:\n${render(init, path, show)}"
      case Unsettled(init, path)          => s"stops unsettled:\n${render(init, path, show)}"
      case Cycle(init, path)              => s"cycle reached:\n${render(init, path, show)}"
      case TooLarge(limit)                => s"more than $limit states"

  private def render[S, A](init: S, path: List[Step[S, A]], show: S => String): String =
    (s"  init  ${show(init)}" :: path.map(st => s"  ${st.label}\n        ${show(st.state)}")).mkString("\n")

  def check[S, A](
    init: S,
    next: S => Iterable[(A, S)],
    invariants: Seq[(String, S => Boolean)],
    progress: A => Boolean,
    settled: S => Boolean,
    limit: Int = 5_000_000
  ): Verdict[S, A] =
    val index  = mutable.HashMap[S, Int](init -> 0)
    val states = mutable.ArrayBuffer[S](init)
    val parent = mutable.ArrayBuffer[Int](-1)
    val via    = mutable.ArrayBuffer[Option[A]](None)
    val edges  = mutable.ArrayBuffer[Array[Int]]()
    var steps  = 0

    def pathTo(i: Int): List[Step[S, A]] =
      Iterator.iterate(i)(parent(_)).takeWhile(_ > 0).map(j => Step(via(j).get, states(j))).toList.reverse

    def violated(s: S): Option[String] = invariants.collectFirst { case (name, p) if !p(s) => name }

    var verdict: Option[Verdict[S, A]] = violated(init).map(name => Verdict.Violated(name, init, Nil))
    var i                              = 0
    while verdict.isEmpty && i < states.size do
      val s       = states(i)
      val out     = next(s).toVector
      val targets = Array.newBuilder[Int]
      steps += out.size
      val it = out.iterator
      while verdict.isEmpty && it.hasNext do
        val (label, t) = it.next()
        index.get(t) match
          case Some(j) => targets += j
          case None    =>
            if states.size >= limit then verdict = Some(Verdict.TooLarge(limit))
            else
              states += t; parent += i; via += Some(label)
              val j = states.size - 1
              index(t) = j
              targets += j
              verdict = violated(t).map(name => Verdict.Violated(name, init, pathTo(j)))
      if verdict.isEmpty && !out.exists((label, _) => progress(label)) && !settled(s) then
        verdict = Some(Verdict.Unsettled(init, pathTo(i)))
      edges += targets.result()
      i += 1

    verdict.getOrElse {
      findCycle(edges) match
        case Some(j) => Verdict.Cycle(init, pathTo(j))
        case None    => Verdict.Holds(states.size, steps)
    }

  /**
   * A state on a cycle, if the graph has one (Kahn's algorithm: whatever cannot be peeled off lies on or after one).
   */
  private def findCycle(edges: collection.IndexedSeq[Array[Int]]): Option[Int] =
    val n        = edges.size
    val incoming = Array.fill(n)(0)
    edges.foreach(_.foreach(j => incoming(j) += 1))
    val queue  = mutable.Queue.from((0 until n).filter(incoming(_) == 0))
    var peeled = 0
    while queue.nonEmpty do
      val i = queue.dequeue()
      peeled += 1
      edges(i).foreach { j =>
        incoming(j) -= 1
        if incoming(j) == 0 then queue.enqueue(j)
      }
    Option.when(peeled < n)((0 until n).find(incoming(_) > 0).get)
