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

  def function(name: String, arg: String): LeftExpression =
    new LeftExpression(s"${name}(${arg})")

  def function(name: String, args: Any*): LeftExpression =
    new LeftExpression(s"${name}(${args.map(_.toString).mkString(",")})")

  def function(name: String, args: Iterable[String]): LeftExpression =
    new LeftExpression(s"${name}(${args.mkString(",")})")

}

case class BaseExpression(value: String) extends Expression {
  override def build(): String = this.value
}

// scalastyle:off method.name
class LeftExpression(value: String) extends Expression {
  override def build(): String = this.value

  def cast(to: CastType): LeftExpression = GeneralFunctions.cast(this, to)

  def cast(to: String): LeftExpression =
    cast(CastType.decode(to).getOrElse(throw new IllegalArgumentException))

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
