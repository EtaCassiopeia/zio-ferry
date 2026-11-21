package ferry.testkit.tla

import scala.collection.immutable.ListMap

/**
 * A TLA+ value, as TLC prints it and as a trace module writes it: the common ground between a recorded run of the code
 * and a state of the model.
 */
enum Value:
  case Bool(value: Boolean)
  case Int(value: Long)
  case Str(value: String)

  /** A model value, or any other bare identifier TLC prints, such as `c1`. */
  case Model(name: String)
  case Seq(items: Vector[Value])
  case Set(items: scala.collection.immutable.Set[Value])
  case Rec(fields: ListMap[String, Value])

  /** A function that is not a sequence, as `(a :> 1 @@ b :> 2)`; an empty one is printed as `<<>>` by TLC. */
  case Fn(pairs: ListMap[Value, Value])

  /** The value in TLA+ syntax, ready to paste into a module. Sets and records are written in a canonical order. */
  def render: String = this match
    case Bool(b)    => if b then "TRUE" else "FALSE"
    case Int(n)     => n.toString
    case Str(s)     => "\"" + s.flatMap(Value.escape) + "\""
    case Model(n)   => n
    case Seq(items) => items.map(_.render).mkString("<<", ", ", ">>")
    case Set(items) => items.toVector.map(_.render).sorted.mkString("{", ", ", "}")
    case Rec(fs)    => fs.toVector.sortBy(_._1).map((k, v) => s"$k |-> ${v.render}").mkString("[", ", ", "]")
    case Fn(pairs)  =>
      if pairs.isEmpty then "<<>>"
      else pairs.toVector.map((k, v) => s"${k.render} :> ${v.render}").sorted.mkString("(", " @@ ", ")")

  /** Two values TLC would consider equal: an empty function is the empty sequence, records ignore field order. */
  def sameAs(other: Value): Boolean = Value.normal(this) == Value.normal(other)

object Value:

  def rec(fields: (String, Value)*): Value = Rec(ListMap(fields*))
  def seq(items: Value*): Value            = Seq(items.toVector)
  def set(items: Value*): Value            = Set(items.toSet)
  def str(s: String): Value                = Str(s)
  def int(n: Long): Value                  = Int(n)

  private def escape(c: Char): String = c match
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case c    => c.toString

  private def normal(v: Value): Value = v match
    case Seq(items) if items.isEmpty => Fn(ListMap.empty)
    case Seq(items)                  => Seq(items.map(normal))
    case Set(items)                  => Set(items.map(normal))
    case Rec(fs)                     => Rec(ListMap.from(fs.toVector.sortBy(_._1).map((k, x) => k -> normal(x))))
    case Fn(pairs)                   =>
      val ps = pairs.map((k, x) => normal(k) -> normal(x))
      // A function on 1..n is a sequence.
      val keys = ps.keySet
      if ps.nonEmpty && keys == (1L to ps.size.toLong).map(Int(_)).toSet then
        Seq((1L to ps.size.toLong).map(i => ps(Int(i))).toVector)
      else Fn(ListMap.from(ps.toVector.sortBy(_._1.render)))
    case other => other

  /** Parses a value as TLC prints it in a state. */
  def parse(text: String): Either[String, Value] =
    val p = Parser(text)
    try
      val v = p.value()
      p.skip()
      if p.atEnd then Right(v) else Left(s"unexpected text at ${p.pos}: ${text.drop(p.pos).take(40)}")
    catch case e: IllegalArgumentException => Left(e.getMessage)

  final private class Parser(s: String):
    var pos: scala.Int = 0

    def atEnd: Boolean = pos >= s.length

    def skip(): Unit =
      while pos < s.length && s(pos).isWhitespace do pos += 1

    private def fail(what: String) =
      throw IllegalArgumentException(s"expected $what at $pos: ${s.slice(pos, pos + 40)}")

    private def peek(t: String): Boolean =
      skip()
      s.startsWith(t, pos)

    private def expect(t: String): Unit =
      if peek(t) then pos += t.length else fail(s"'$t'")

    private def separated[A](close: String)(item: () => A): Vector[A] =
      if peek(close) then
        pos += close.length
        Vector.empty
      else
        val b = Vector.newBuilder[A]
        b += item()
        while peek(",") do
          pos += 1
          b += item()
        expect(close)
        b.result()

    def value(): Value =
      skip()
      if atEnd then fail("a value")
      else if peek("<<") then
        pos += 2
        Seq(separated(">>")(() => value()))
      else if peek("{") then
        pos += 1
        Set(separated("}")(() => value()).toSet)
      else if peek("[") then
        pos += 1
        Rec(ListMap.from(separated("]") { () =>
          val k = identifier()
          expect("|->")
          k -> value()
        }))
      else if peek("(") then
        pos += 1
        val b    = ListMap.newBuilder[Value, Value]
        var more = true
        while more do
          val k = value()
          expect(":>")
          b += k -> value()
          if peek("@@") then pos += 2 else more = false
        expect(")")
        Fn(b.result())
      else if peek("\"") then
        pos += 1
        val out = StringBuilder()
        while pos < s.length && s(pos) != '"' do
          if s(pos) == '\\' && pos + 1 < s.length then
            out += (s(pos + 1) match
              case 'n' => '\n'
              case c   => c)
            pos += 2
          else
            out += s(pos)
            pos += 1
        expect("\"")
        Str(out.result())
      else if s(pos) == '-' || s(pos).isDigit then
        val start = pos
        pos += 1
        while pos < s.length && s(pos).isDigit do pos += 1
        Int(s.slice(start, pos).toLong)
      else
        identifier() match
          case "TRUE"  => Bool(true)
          case "FALSE" => Bool(false)
          case name    => Model(name)

    private def identifier(): String =
      skip()
      val start = pos
      while pos < s.length && (s(pos).isLetterOrDigit || s(pos) == '_') do pos += 1
      if start == pos then fail("an identifier") else s.slice(start, pos)
