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

  def function(name: String, arg: String): Expression =
    Expr(s"${name}(${arg})")

  def function(name: String, args: Any*): Expression =
    Expr(s"${name}(${args.map(_.toString).mkString(",")})")

  def function(name: String, args: Iterable[String]): Expression =
    Expr(s"${name}(${args.mkString(",")})")

  def apply(v: String): Expr        = new Expr(v)
  def apply[T: Numeric](v: T): Expr = new Expr(v.toString)

  def apply[T <: CharSequence](values: Iterable[T]): Expr = new Expr(values.mkString("[", ",", "]"))
  def apply[T: Numeric](values: Iterable[T]): Expr        = this.apply(values.map(_.toString))

}

class LeftExpresssion(value: String) extends Expression {
  override def build(): String = this.value
}

class RightExpresssion(value: String) extends Expression {
  override def build(): String = this.value
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

  def nvl(expr: LeftExpresssion, exprWhenNull: Expression): Expression =
    Expr(s"nvl(${expr.build()}, ${exprWhenNull.build()})")

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

  // todo bloom_filter_test

}

trait StringFunctions {

  def concat(args: LeftExpresssion*): Expression =
    Expr.function("concat", args.map(_.build()))

  def format(pattern: String, args: LeftExpresssion*): Expression =
    Expr.function("format", args.scanLeft(pattern)((_, e) => e.build()))

  def like(expr: LeftExpresssion, pattern: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern}")

  def like(expr: LeftExpresssion, pattern: String, escape: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern} ${escape}")

  def lookup(expr: LeftExpresssion, lookupName: String): Expression =
    Expr.function("lookup", expr.build(), lookupName)

  def parseLong(value: String): Expression =
    Expr.function("parse_long", value)

  def parseLong(value: String, radix: Int): Expression =
    Expr.function("parse_long", value, radix)

  def regexpExtract(expr: LeftExpresssion, pattern: String): Expression =
    Expr.function("timestamp_ceil", expr.build(), pattern)

  def regexpExtract(expr: LeftExpresssion, pattern: String, index: Int): Expression =
    Expr.function("regexp_extract", expr.build(), pattern, index)

  def regexpLike(expr: LeftExpresssion, pattern: String): Expression =
    Expr.function("regexp_like", expr.build(), pattern)

  def containsString(expr: LeftExpresssion, pattern: String): Expression =
    Expr.function("contains_string", expr.build(), pattern)

  def icontainsString(expr: LeftExpresssion, pattern: String): Expression =
    Expr.function("icontains_string", expr.build(), pattern)

  def replace(expr: LeftExpresssion, pattern: String, replacement: String): Expression =
    Expr.function("replace", expr.build(), pattern, replacement)

  def substring(expr: LeftExpresssion, index: Int, length: Int): Expression =
    Expr.function("substring", expr.build(), index, length)

  // right(expr, length) returns the rightmost length characters from a string
  def right(expr: LeftExpresssion, length: Int): Expression =
    Expr.function("right", expr.build(), length)

  def left(expr: LeftExpresssion, length: Int): Expression =
    Expr.function("left", expr.build(), length)

  def strlen(expr: LeftExpresssion): Expression =
    Expr.function("left", expr.build())

  def strpos(haystack: LeftExpresssion, needle: String): Expression =
    Expr.function("strpos", haystack.build(), needle)

  def strpos(haystack: LeftExpresssion, needle: String, fromIndex: Int): Expression =
    Expr.function("strpos", haystack.build(), needle, fromIndex)

  def trim(expr: LeftExpresssion): Expression =
    Expr.function("trim", expr.build())

  def trim(expr: LeftExpresssion, chars: String = " "): Expression =
    Expr.function("trim", expr.build(), chars)

  def ltrim(expr: LeftExpresssion): Expression =
    Expr.function("ltrim", expr.build())

  def ltrim(expr: LeftExpresssion, chars: String = " "): Expression =
    Expr.function("ltrim", expr.build(), chars)

  def rtrim(expr: LeftExpresssion): Expression =
    Expr.function("rtrim", expr.build())

  def rtrim(expr: LeftExpresssion, chars: String = " "): Expression =
    Expr.function("rtrim", expr.build(), chars)

  def lower(expr: LeftExpresssion): Expression =
    Expr.function("lower", expr.build())

  def upper(expr: LeftExpresssion): Expression =
    Expr.function("upper", expr.build())

  def reverse(expr: LeftExpresssion): Expression =
    Expr.function("reverse", expr.build())

  def repeat(expr: LeftExpresssion, n: Int): Expression =
    Expr.function("repeat", expr.build(), n)

  def lpad(expr: LeftExpresssion, length: Int, chars: String): Expression =
    Expr.function("lpad", expr.build(), length, chars)

  def rpad(expr: LeftExpresssion, length: Int, chars: String): Expression =
    Expr.function("rpad", expr.build(), length, chars)
}

trait TimeFunctions {

  def timestamp(expr: LeftExpresssion): Expression =
    Expr.function("timestamp", expr.build())

  def timestamp(expr: LeftExpresssion, formatString: String): Expression =
    Expr.function("timestamp", expr.build(), formatString)

  def unixTimestamp(expr: LeftExpresssion): Expression =
    Expr.function("unix_timestamp", expr.build())

  def unixTimestamp(expr: LeftExpresssion, formatString: String): Expression =
    Expr.function("unix_timestamp", expr.build(), formatString)

  def timestampCeil(expr: LeftExpresssion, period: String): Expression =
    Expr.function("timestamp_ceil", expr.build(), period)

  def timestampCeil(expr: LeftExpresssion,
                    period: String,
                    timezone: String,
                    origin: Option[String] = None): Expression =
    Expr.function("timestamp_ceil", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampFloor(expr: LeftExpresssion, period: String): Expression =
    Expr.function("timestamp_floor", expr.build(), period)

  def timestampFloor(expr: LeftExpresssion,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): Expression =
    Expr.function("timestamp_floor", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampShift(expr: LeftExpresssion, period: String): Expression =
    Expr.function("timestamp_shift", expr.build(), period)

  def timestampShift(expr: LeftExpresssion,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): Expression =
    Expr.function("timestamp_shift", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampExtract(expr: LeftExpresssion, unit: String): Expression =
    Expr.function("timestamp_extract", expr.build(), unit)

  def timestampExtract(expr: LeftExpresssion, unit: String, timezone: String): Expression =
    Expr.function("timestamp_extract", expr.build(), unit, timezone)

  def timestampParse(expr: String): Expression =
    Expr.function("timestamp_parse", expr)

  def timestampParse(expr: String, pattern: String): Expression =
    Expr.function("timestamp_parse", expr, pattern)

  def timestampParse(expr: String, pattern: String, timezone: String): Expression =
    Expr.function("timestamp_parse", expr, pattern, timezone)

  def timestampFormat(expr: LeftExpresssion): Expression =
    Expr.function("timestamp_format", expr.build())

  def timestampFormat(expr: LeftExpresssion, pattern: String): Expression =
    Expr.function("timestamp_format", expr.build(), pattern)

  def timestampFormat(expr: LeftExpresssion, pattern: String, timezone: String): Expression =
    Expr.function("timestamp_format", expr.build(), pattern, timezone)

}

trait MathFunctions {
  def abs[@specialized(Long, Double) T](x: T): Expression = Expr.function("abs", x)
  def abs(x: LeftExpresssion): Expression                 = Expr.function("abs", x.build())

  def acos[@specialized(Long, Double) T](x: T): Expression = Expr.function("acos", x)
  def acos(x: LeftExpresssion): Expression                 = Expr.function("acos", x.build())

  def asin[@specialized(Long, Double) T](x: T): Expression = Expr.function("asin", x)
  def asin(x: LeftExpresssion): Expression                 = Expr.function("asin", x.build())

  def atan(x: LeftExpresssion): Expression = Expr.function("atan", x.build())

  def atan2[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("atan2", x, y)
  def atan2[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("atan2", x.build(), y)
  def atan2[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("atan2", x, y.build())
  def atan2(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("atan2", x.build(), y.build())

  def cbrt[@specialized(Long, Double) T](x: T): Expression = Expr.function("cbrt", x)
  def cbrt(x: LeftExpresssion): Expression                 = Expr.function("cbrt", x.build())

  def ceil[@specialized(Long, Double) T](x: T): Expression = Expr.function("ceil", x)
  def ceil(x: LeftExpresssion): Expression                 = Expr.function("ceil", x.build())

  def copysign[@specialized(Long, Double) T](x: T): Expression = Expr.function("copysign", x)
  def copysign(x: LeftExpresssion): Expression                 = Expr.function("copysign", x.build())

  def cos[@specialized(Long, Double) T](x: T): Expression = Expr.function("cos", x)
  def cos(x: LeftExpresssion): Expression                 = Expr.function("cos", x.build())

  def cosh[@specialized(Long, Double) T](x: T): Expression = Expr.function("cosh", x)
  def cosh(x: LeftExpresssion): Expression                 = Expr.function("cosh", x.build())

  def cot[@specialized(Long, Double) T](x: T): Expression = Expr.function("cot", x)
  def cot(x: LeftExpresssion): Expression                 = Expr.function("cot", x.build())

  def div[@specialized(Long, Double) T](x: T): Expression = Expr.function("div", x)
  def div(x: LeftExpresssion): Expression                 = Expr.function("div", x.build())

  def exp[@specialized(Long, Double) T](x: T): Expression = Expr.function("exp", x)
  def exp(x: LeftExpresssion): Expression                 = Expr.function("exp", x.build())

  def floor[@specialized(Long, Double) T](x: T): Expression = Expr.function("floor", x)
  def floor(x: LeftExpresssion): Expression                 = Expr.function("floor", x.build())

  def getExponent[@specialized(Long, Double) T](x: T): Expression = Expr.function("getExponent", x)
  def getExponent(x: LeftExpresssion): Expression                 = Expr.function("getExponent", x.build())

  def hypot[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("hypot", x, y)
  def hypot[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("hypot", x.build(), y)
  def hypot[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("hypot", x, y.build())
  def hypot(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("hypot", x.build(), y.build())

  def log[@specialized(Long, Double) T](x: T): Expression = Expr.function("hypot", x)
  def log(x: LeftExpresssion): Expression                 = Expr.function("hypot", x.build())

  def log10[@specialized(Long, Double) T](x: T): Expression = Expr.function("log10", x)
  def log10(x: LeftExpresssion): Expression                 = Expr.function("log10", x.build())

  def log1p[@specialized(Long, Double) T](x: T): Expression = Expr.function("log1p", x)
  def log1p(x: LeftExpresssion): Expression                 = Expr.function("log1p", x.build())

  def max[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("max", x, y)
  def max[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("max", x.build(), y)
  def max[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("max", x, y.build())
  def max(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("max", x.build(), y.build())

  def min[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("min", x, y)
  def min[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("min", x.build(), y)
  def min[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("min", x, y.build())
  def min(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("min", x.build(), y.build())

  def nextafter[@specialized(Long, Double) T](x: T, y: T): Expression =
    Expr.function("nextafter", x, y)
  def nextafter[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("nextafter", x.build(), y)
  def nextafter[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("nextafter", x, y.build())
  def nextafter(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("nextafter", x.build(), y.build())

  def nextUp(x: LeftExpresssion): Expression = Expr.function("nextUp", x.build())
  def pi: Expression                         = Expr.function("pi")

  def pow(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("pow", x.build(), y.build())
  def remainder(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("remainder", x.build(), y.build())

  def rint[@specialized(Long, Double) T](x: T): Expression = Expr.function("rint", x)
  def rint(x: LeftExpresssion): Expression                 = Expr.function("rint", x.build())

  def round[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("round", x, y)
  def round[@specialized(Long, Double) T](x: LeftExpresssion, y: T): Expression =
    Expr.function("round", x.build(), y)
  def round[@specialized(Long, Double) T](x: T, y: LeftExpresssion): Expression =
    Expr.function("round", x, y.build())
  def round(x: LeftExpresssion, y: LeftExpresssion): Expression =
    Expr.function("round", x.build(), y.build())

  def scalb[@specialized(Long, Double) T](d: T, sf: T): Expression = Expr.function("scalb", d, sf)
  def scalb[@specialized(Long, Double) T](d: LeftExpresssion, sf: T): Expression =
    Expr.function("scalb", d.build(), sf)
  def scalb[@specialized(Long, Double) T](d: T, sf: LeftExpresssion): Expression =
    Expr.function("scalb", d, sf.build())
  def scalb(d: LeftExpresssion, sf: LeftExpresssion): Expression =
    Expr.function("scalb", d.build(), sf.build())

  def signum[@specialized(Long, Double) T](x: T): Expression = Expr.function("signum", x)
  def signum(x: LeftExpresssion): Expression                 = Expr.function("signum", x.build())

  def sin[@specialized(Long, Double) T](x: T): Expression = Expr.function("sin", x)
  def sin(x: LeftExpresssion): Expression                 = Expr.function("sin", x.build())

  def sinh[@specialized(Long, Double) T](x: T): Expression = Expr.function("sinh", x)
  def sinh(x: LeftExpresssion): Expression                 = Expr.function("sinh", x.build())

  def sqrt[@specialized(Long, Double) T](x: T): Expression = Expr.function("sqrt", x)
  def sqrt(x: LeftExpresssion): Expression                 = Expr.function("sqrt", x.build())

  def tan[@specialized(Long, Double) T](x: T): Expression = Expr.function("tan", x)
  def tan(x: LeftExpresssion): Expression                 = Expr.function("tan", x.build())

  def tanh[@specialized(Long, Double) T](x: T): Expression = Expr.function("tanh", x)
  def tanh(x: LeftExpresssion): Expression                 = Expr.function("tanh", x.build())

  def todegrees[@specialized(Long, Double) T](x: T): Expression = Expr.function("todegrees", x)
  def todegrees(x: LeftExpresssion): Expression                 = Expr.function("todegrees", x.build())

  def toradians[@specialized(Long, Double) T](x: T): Expression = Expr.function("toradians", x)
  def toradians(x: LeftExpresssion): Expression                 = Expr.function("toradians", x.build())

  def ulp[@specialized(Long, Double) T](x: T): Expression = Expr.function("ulp", x)
  def ulp(x: LeftExpresssion): Expression                 = Expr.function("ulp", x.build())

}

trait ArrayFunctions {}

trait ApplyFunctions {}

trait ReductionFunctions {}

trait IPAddressFunctions

object ExpressionFunctions extends GeneralFunctions with StringFunctions
// scalastyle:on
