/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ing.wbaa.druid.dql.expressions

import ca.mrvisser.sealerate
import ing.wbaa.druid._
import ing.wbaa.druid.dql.Dim
import ing.wbaa.druid.dql.expressions.functions.{
  ApplyFunctions,
  ArrayFunctions,
  GeneralFunctions,
  IPAddressFunctions,
  MathFunctions,
  ReductionFunctions,
  StringFunctions,
  TimeFunctions
}
import scala.annotation.implicitNotFound

// scalastyle:off todo.comment
// scalastyle:off number.of.methods

// scalastyle:off method.name

object BinaryExpressionOps extends Enumeration {
  val Power    = Value("^")
  val Multiply = Value(" * ")
  val Divide   = Value(" / ")
  val Modulo   = Value(" % ")
  val Add      = Value(" + ")
  val Subtract = Value(" - ")

  val LessThan      = Value(" < ")
  val LessThanEq    = Value(" <= ")
  val GreaterThan   = Value(" > ")
  val GreaterThanEq = Value(" >= ")

  val Equals    = Value(" == ")
  val NotEquals = Value(" != ")

  val And = Value(" && ")
  val Or  = Value(" || ")
}

sealed trait CastType extends Enum with UpperCaseEnumStringEncoder
object CastType extends EnumCodec[CastType] {
  case object Long        extends CastType
  case object Double      extends CastType
  case object String      extends CastType
  case object LongArray   extends CastType
  case object DoubleArray extends CastType
  case object StringArray extends CastType

  val values: Set[CastType] = sealerate.values[CastType]
}

sealed trait Expression {

  def build(): String

  // Binary Comparison
  def <(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.LessThan)
  def <=(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.LessThanEq)
  def >(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.GreaterThan)
  def >=(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.GreaterThanEq)

  def ===(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.Equals)
  def =!=(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.NotEquals)

  // Binary Logical AND, OR
  def and(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.And)
  def or(other: Expression): Expression =
    Expression.binaryOperator(this, other, BinaryExpressionOps.Or)
}
// scalastyle:on method.name

object Expression {

  @inline
  private def binaryOperator(left: Expression,
                             right: Expression,
                             op: BinaryExpressionOps.Value): Expression =
    BaseExpression(s"${left.build()}${op}${right.build()}")

  @deprecated(message = "will be removed", since = "now")
  def function(name: String, arg: String): LeftExpression =
    new LeftExpression(s"${name}(${arg})")

  @deprecated(message = "will be removed", since = "now")
  def function(name: String, args: Any*): LeftExpression =
    new LeftExpression(s"${name}(${args.map(_.toString).mkString(",")})")

  @deprecated(message = "will be removed", since = "now")
  def function(name: String, args: Iterable[String]): LeftExpression =
    new LeftExpression(s"${name}(${args.mkString(",")})")

  def fun(name: String): LeftExpression =
    new LeftExpression(s"${name}()")

  def fun[T: ExpressionLiteral](name: String, arg: T): LeftExpression = {
    val value = implicitly[ExpressionLiteral[T]].literal(arg)
    new LeftExpression(s"${name}(${value})")
  }

  def fun[T1: ExpressionLiteral, T2: ExpressionLiteral](name: String,
                                                        arg1: T1,
                                                        arg2: T2): LeftExpression = {
    val value1 = implicitly[ExpressionLiteral[T1]].literal(arg1)
    val value2 = implicitly[ExpressionLiteral[T2]].literal(arg2)
    new LeftExpression(s"${name}(${value1},${value2})")
  }

  def fun[T1: ExpressionLiteral, T2: ExpressionLiteral, T3: ExpressionLiteral](
      name: String,
      arg1: T1,
      arg2: T2,
      arg3: T3
  ): LeftExpression = {
    val value1 = implicitly[ExpressionLiteral[T1]].literal(arg1)
    val value2 = implicitly[ExpressionLiteral[T2]].literal(arg2)
    val value3 = implicitly[ExpressionLiteral[T3]].literal(arg3)
    new LeftExpression(s"${name}(${value1},${value2},${value3})")
  }

  def fun[T1: ExpressionLiteral,
          T2: ExpressionLiteral,
          T3: ExpressionLiteral,
          T4: ExpressionLiteral](
      name: String,
      arg1: T1,
      arg2: T2,
      arg3: T3,
      arg4: T4
  ): LeftExpression = {
    val value1 = implicitly[ExpressionLiteral[T1]].literal(arg1)
    val value2 = implicitly[ExpressionLiteral[T2]].literal(arg2)
    val value3 = implicitly[ExpressionLiteral[T3]].literal(arg3)
    val value4 = implicitly[ExpressionLiteral[T4]].literal(arg4)
    new LeftExpression(s"${name}(${value1},${value2},${value3},${value4})")
  }

  def fun[T1: ExpressionLiteral,
          T2: ExpressionLiteral,
          T3: ExpressionLiteral,
          T4: ExpressionLiteral,
          T5: ExpressionLiteral](
      name: String,
      arg1: T1,
      arg2: T2,
      arg3: T3,
      arg4: T4,
      arg5: T5
  ): LeftExpression = {
    val value1 = implicitly[ExpressionLiteral[T1]].literal(arg1)
    val value2 = implicitly[ExpressionLiteral[T2]].literal(arg2)
    val value3 = implicitly[ExpressionLiteral[T3]].literal(arg3)
    val value4 = implicitly[ExpressionLiteral[T4]].literal(arg4)
    val value5 = implicitly[ExpressionLiteral[T5]].literal(arg5)
    new LeftExpression(s"${name}(${value1},${value2},${value3},${value4},${value5})")
  }

  def funs[T: ExpressionLiteral](name: String, expressions: Iterable[T]): LeftExpression = {
    val value = expressions
      .map(implicitly[ExpressionLiteral[T]].literal)
      .mkString(s"${name}(", ",", ")")
    new LeftExpression(value)
  }

  def funs[T: ExpressionLiteral](name: String,
                                 arg: String,
                                 expressions: Iterable[T]): LeftExpression = {
    val value = expressions
      .map(implicitly[ExpressionLiteral[T]].literal)
      .mkString(s"${name}(${arg},", ",", ")")
    new LeftExpression(value)
  }

}

case class BaseExpression(value: String) extends Expression {
  override def build(): String = this.value
}

// scalastyle:off method.name
class LeftExpression(value: String) extends Expression {
  override def build(): String = this.value

  def cast(to: CastType): LeftExpression = GeneralFunctions.cast(this, to)

  def cast(to: String): LeftExpression =
    CastType.decode(to) match {
      case Left(error)          => throw new IllegalArgumentException(s"Failed to cast to type '$to'", error)
      case Right(castTypeValue) => cast(castTypeValue)
    }

  // Unary NOT
  def unary_!(): LeftExpression = new LeftExpression(s"!$value")

  // Unary Minus
  def unary_-(): LeftExpression = new LeftExpression(s"-$value")

  // Binary power op
  def ^(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Power)

  // Binary multiplicative
  def *(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Multiply)

  def /(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Divide)

  def %(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Modulo)

  // Binary additive
  def +(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Add)

  def -(other: LeftExpression): LeftExpression =
    binaryOperator(this, other, BinaryExpressionOps.Subtract)

  @inline
  private def binaryOperator(
      left: LeftExpression,
      right: LeftExpression,
      op: BinaryExpressionOps.Value
  ): LeftExpression = new LeftExpression(s"${left.build()}${op}${right.build()}")

}
// scalastyle:on method.name

class RightExpression(value: String) extends Expression {
  override def build(): String = this.value
}

@implicitNotFound(
  msg =
    "Only implementations of Expression, Dim and primitives (String, Int, Double etc.) are supported"
)
sealed trait ExpressionLiteral[T] {
  def literal(v: T): String
}

object ExpressionLiteral {

  def valueOf[T](v: T)(implicit exprLit: ExpressionLiteral[T]): String = exprLit.literal(v)

  sealed trait ExpressionLiteralQuoted[T] extends ExpressionLiteral[T] {
    override def literal(v: T): String = s"'${v.toString}'"
  }

  sealed trait ExpressionLiteralUnquoted[T] extends ExpressionLiteral[T] {
    override def literal(v: T): String = s"$v"
  }

  sealed trait ExpressionLiteralImpl[T <: Expression] extends ExpressionLiteral[T] {
    override def literal(v: T): String = s"${v.build()}"
  }

  implicit object LeftExpressionLiteral  extends ExpressionLiteralImpl[LeftExpression]
  implicit object RightExpressionLiteral extends ExpressionLiteralImpl[RightExpression]
  implicit object BaseExpressionLiteral  extends ExpressionLiteralImpl[BaseExpression]
  implicit object TExpressionLiteral     extends ExpressionLiteralImpl[Expression]

  implicit object DimExpressionLiteral extends ExpressionLiteral[Dim] {
    override def literal(v: Dim): String = s"${v.getName}"
  }

  implicit object StringExpressionLiteral extends ExpressionLiteralQuoted[String]

  implicit object ByteExpressionLiteral   extends ExpressionLiteralUnquoted[Byte]
  implicit object ShortExpressionLiteral  extends ExpressionLiteralUnquoted[Short]
  implicit object IntExpressionLiteral    extends ExpressionLiteralUnquoted[Int]
  implicit object LongExpressionLiteral   extends ExpressionLiteralUnquoted[Long]
  implicit object FloatExpressionLiteral  extends ExpressionLiteralUnquoted[Float]
  implicit object DoubleExpressionLiteral extends ExpressionLiteralUnquoted[Double]

  implicit object BooleanExpressionLiteral extends ExpressionLiteral[Boolean] {
    override def literal(v: Boolean): String = s"${v.toString.toLowerCase}"
  }

  object AnyExpressionLiteral {
    def literal(value: Any): String = value match {
      case v: LeftExpression  => LeftExpressionLiteral.literal(v)
      case v: RightExpression => RightExpressionLiteral.literal(v)
      case v: BaseExpression  => BaseExpressionLiteral.literal(v)
      case v: Expression      => TExpressionLiteral.literal(v)
      case v: Dim             => DimExpressionLiteral.literal(v)
      case v: String          => StringExpressionLiteral.literal(v)
      case v: Byte            => ByteExpressionLiteral.literal(v)
      case v: Short           => ShortExpressionLiteral.literal(v)
      case v: Int             => IntExpressionLiteral.literal(v)
      case v: Long            => LongExpressionLiteral.literal(v)
      case v: Float           => FloatExpressionLiteral.literal(v)
      case v: Double          => DoubleExpressionLiteral.literal(v)
      case v: Boolean         => BooleanExpressionLiteral.literal(v)
      case _ =>
        throw new UnsupportedOperationException(s"Not implemented for type ${value.getClass}")
    }
  }

  implicit def seqExpressionLiteral[T: ExpressionLiteral]: ExpressionLiteral[Iterable[T]] =
    new ExpressionLiteral[Iterable[T]] {
      override def literal(v: Iterable[T]): String =
        v.map(implicitly[ExpressionLiteral[T]].literal).mkString("[", ",", "]")
    }
}

trait ExpressionFunctions
    extends GeneralFunctions
    with StringFunctions
    with MathFunctions
    with TimeFunctions
    with ApplyFunctions
    with ArrayFunctions
    with IPAddressFunctions
    with ReductionFunctions

object ExpressionFunctions extends ExpressionFunctions

// scalastyle:on todo.comment
// scalastyle:on number.of.methods
