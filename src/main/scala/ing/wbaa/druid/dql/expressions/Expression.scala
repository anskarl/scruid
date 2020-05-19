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

// scalastyle:off

import ca.mrvisser.sealerate
import ing.wbaa.druid._

sealed trait Expression {

  def build(): String

  // Unary NOT and Minus
  def unary_!(): Expression = ExpressionOps.not(this)
  def unary_-(): Expression = ExpressionOps.minus(this)

  // Binary power op
  def ^(other: Expression): Expression = ExpressionOps.power(this, other)

  // Binary multiplicative
  def *(other: Expression): Expression = ExpressionOps.multiply(this, other)
  def /(other: Expression): Expression = ExpressionOps.divide(this, other)
  def %(other: Expression): Expression = ExpressionOps.modulo(this, other)

  // Binary additive
  def +(other: Expression): Expression = ExpressionOps.multiply(this, other)
  def -(other: Expression): Expression = ExpressionOps.multiply(this, other)

  // Binary Comparison
  def <(other: Expression): Expression  = ExpressionOps.lessThan(this, other)
  def <=(other: Expression): Expression = ExpressionOps.lessThanEq(this, other)
  def >(other: Expression): Expression  = ExpressionOps.greaterThan(this, other)
  def >=(other: Expression): Expression = ExpressionOps.greaterThanEq(this, other)

  def ===(other: Expression): Expression = ExpressionOps.equals(this, other)
  def =!=(other: Expression): Expression = ExpressionOps.notEquals(this, other)

  // Binary Logical AND, OR
  def and(other: Expression): Expression = ExpressionOps.and(this, other)
  def or(other: Expression): Expression  = ExpressionOps.or(this, other)
}

object ExpressionOps {

  private object BinaryOps extends Enumeration {
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

  @inline
  private def binaryExpression(left: Expression,
                               right: Expression,
                               op: BinaryOps.Value): Expression =
    Expr(s"${left.build()}${op}${right.build()}")

  // Unary NOT and Minus
  def not(e: Expression): Expression   = Expr(s"!${e.build()}")
  def minus(e: Expression): Expression = Expr(s"-${e.build()}")

  def not(v: String): Expression   = Expr(s"!$v")
  def minus(v: String): Expression = Expr(s"-$v")

  def not[T: Numeric](v: T): Expression   = Expr(s"!${v.toString}")
  def minus[T: Numeric](v: T): Expression = Expr(s"-${v.toString}")

  // Binary power op
  def power(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Power)

  // Binary multiplicative
  def multiply(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Multiply)
  def divide(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Divide)
  def modulo(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Modulo)

  // Binary additive
  def add(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Add)
  def subtract(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Subtract)

  // Binary Comparison
  def lessThan(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.LessThan)
  def lessThanEq(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.LessThanEq)
  def greaterThan(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.GreaterThan)
  def greaterThanEq(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.GreaterThanEq)
  def equals(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Equals)
  def notEquals(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.NotEquals)

  // Binary Logical AND, OR
  def and(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.And)
  def or(left: Expression, right: Expression): Expression =
    binaryExpression(left, right, BinaryOps.Or)

}

class Expr private (value: String) extends Expression {
  override def build(): String = this.value
}

object Expr {

  def apply(v: String): Expr        = new Expr(v)
  def apply[T: Numeric](v: T): Expr = new Expr(v.toString)

  def apply[T <: CharSequence](values: Iterable[T]): Expr = new Expr(values.mkString("[", ",", "]"))
  def apply[T: Numeric](values: Iterable[T]): Expr        = this.apply(values.map(_.toString))

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

object CaseSearched {

  def builder: CaseBuilder = new CaseBuilder()

  sealed trait CaseSearchedBuilder {
    private[expressions] def build(): Expression
  }

  class BuilderOtherwise private[expressions] (caseStatements: Seq[(Expression, Expression)],
                                               elseResult: Expression)
      extends CaseSearchedBuilder {
    override private[expressions] def build(): Expression = Expr {
      caseStatements
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_searched(", ",", s"${elseResult.build()})")
    }
  }

  class CaseBuilder private[expressions] () extends CaseSearchedBuilder { self =>
    private var caseStatements = Seq.empty[(Expression, Expression)]

    def when(condition: Expression, result: Expression): self.type = {
      caseStatements :+= condition -> result
      self
    }

    def otherwise(elseResult: Expression): CaseSearchedBuilder =
      new BuilderOtherwise(caseStatements, elseResult)

    override private[expressions] def build(): Expression = Expr {
      caseStatements
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_searched(", ",", ")")
    }
  }
}

object CaseSimple {

  def builder(expression: Expression): CaseBuilder = new CaseBuilder(expression)

  sealed trait CaseSimpleBuilder {
    private[expressions] def build(): Expression
  }

  class BuilderOtherwise private[expressions] (expression: Expression,
                                               valueResults: Seq[(Expression, Expression)],
                                               elseResult: Expression)
      extends CaseSimpleBuilder {
    override private[expressions] def build(): Expression = Expr {
      valueResults
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_simple(${expression.build()}", ",", s"${elseResult.build()})")
    }
  }

  class CaseBuilder private[expressions] (expression: Expression) extends CaseSimpleBuilder {
    self =>

    private var valueResults = Seq.empty[(Expression, Expression)]

    def when(condition: Expression, result: Expression): self.type = {
      valueResults :+= condition -> result
      self
    }

    def otherwise(elseResult: Expression): CaseSimpleBuilder =
      new BuilderOtherwise(expression, valueResults, elseResult)

    override private[expressions] def build(): Expression = Expr {
      valueResults
        .map { case (condition, result) => s"${condition.build()}, ${result.build()}" }
        .mkString(s"case_simple(${expression.build()}", ",", ")")
    }
  }
}

trait GeneralFunctions {

  def cast(expr: Expression, castType: CastType): Expression =
    Expr(s"cast(${expr.build()}, ${castType})")

  def when(predicateExpr: Expression, thenExpr: Expression, elseExpr: Expression): Expression =
    Expr(s"if(${predicateExpr.build()}, ${thenExpr.build()}, ${elseExpr.build()})")

  def nvl(expr: Expression, exprWhenNull: Expression): Expression =
    Expr(s"nvl(${expr.build()}, ${exprWhenNull.build()})")

  def like(expr: Expression, pattern: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern}")

  def like(expr: Expression, pattern: String, escape: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern} ${escape}")

  /**
    * Builder to create case_searched function expressions:
    *
    * For example to create the following:
    * {{{
    * case_searched(expr1, result1, [[expr2, result2, ...], else-result])
    * }}}
    *
    * {{{
    *   caseSearched{ _
    *     .when(Expr("expr1"), Expr("result1"))
    *     .when(Expr("expr2"), Expr("result2"))
    *     .otherwise(Expr("else-result"))
    *   }
    * }}}
    * @param builder CaseBuilder to create the expression
    * @return the resulting case_searched expression
    */
  def caseSearched(
      builder: CaseSearched.CaseBuilder => CaseSearched.CaseSearchedBuilder
  ): Expression =
    builder(CaseSearched.builder).build()

  /**
    * Builder to create case_simple function expressions:
    *
    * For example to create the following:
    * {{{
    * case_simple(expr, value1, result1, [[value2, result2, ...], else-result])
    * }}}
    *
    * {{{
    *   caseSimple(Expr("expr"), { _
    *     .when(Expr("value1"), Expr("result1"))
    *     .when(Expr("value2"), Expr("result2"))
    *     .otherwise(Expr("else-result"))
    *   })
    * }}}
    *
    * @param builder CaseBuilder to create the expression
    * @return the resulting case_simple expression
    */
  def caseSimple(expression: Expression,
                 builder: CaseSimple.CaseBuilder => CaseSimple.CaseSimpleBuilder): Expression =
    builder(CaseSimple.builder(expression)).build()

}

object Expression extends GeneralFunctions
// scalastyle:on
