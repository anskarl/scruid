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

class LeftExpression(value: String) extends Expression {
  override def build(): String = this.value
}

class RightExpression(value: String) extends Expression {
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

  def cast(expr: LeftExpression, castType: CastType): LeftExpression =
    new LeftExpression(s"cast(${expr.build()}, '${castType.encode()}')")

  def when(predicateExpr: LeftExpression,
           thenExpr: LeftExpression,
           elseExpr: Expression): LeftExpression =
    new LeftExpression(s"if(${predicateExpr.build()}, ${thenExpr.build()}, ${elseExpr.build()})")

  def nvl(expr: LeftExpression, exprWhenNull: LeftExpression): LeftExpression =
    new LeftExpression(s"nvl(${expr.build()}, ${exprWhenNull.build()})")

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

  def concat(args: LeftExpression*): Expression =
    Expr.function("concat", args.map(_.build()))

  def format(pattern: String, args: LeftExpression*): Expression =
    Expr.function("format", args.scanLeft(pattern)((_, e) => e.build()))

  def like(expr: LeftExpression, pattern: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern}")

  def like(expr: LeftExpression, pattern: String, escape: String): Expression =
    Expr(s"${expr.build()} LIKE ${pattern} ${escape}")

  def lookup(expr: LeftExpression, lookupName: String): Expression =
    Expr.function("lookup", expr.build(), lookupName)

  def parseLong(value: String): Expression =
    Expr.function("parse_long", value)

  def parseLong(value: String, radix: Int): Expression =
    Expr.function("parse_long", value, radix)

  def regexpExtract(expr: LeftExpression, pattern: String): Expression =
    Expr.function("timestamp_ceil", expr.build(), pattern)

  def regexpExtract(expr: LeftExpression, pattern: String, index: Int): Expression =
    Expr.function("regexp_extract", expr.build(), pattern, index)

  def regexpLike(expr: LeftExpression, pattern: String): Expression =
    Expr.function("regexp_like", expr.build(), pattern)

  def containsString(expr: LeftExpression, pattern: String): Expression =
    Expr.function("contains_string", expr.build(), pattern)

  def icontainsString(expr: LeftExpression, pattern: String): Expression =
    Expr.function("icontains_string", expr.build(), pattern)

  def replace(expr: LeftExpression, pattern: String, replacement: String): Expression =
    Expr.function("replace", expr.build(), pattern, replacement)

  def substring(expr: LeftExpression, index: Int, length: Int): Expression =
    Expr.function("substring", expr.build(), index, length)

  // right(expr, length) returns the rightmost length characters from a string
  def right(expr: LeftExpression, length: Int): Expression =
    Expr.function("right", expr.build(), length)

  def left(expr: LeftExpression, length: Int): Expression =
    Expr.function("left", expr.build(), length)

  def strlen(expr: LeftExpression): Expression =
    Expr.function("left", expr.build())

  def strpos(haystack: LeftExpression, needle: String): Expression =
    Expr.function("strpos", haystack.build(), needle)

  def strpos(haystack: LeftExpression, needle: String, fromIndex: Int): Expression =
    Expr.function("strpos", haystack.build(), needle, fromIndex)

  def trim(expr: LeftExpression): Expression =
    Expr.function("trim", expr.build())

  def trim(expr: LeftExpression, chars: String = " "): Expression =
    Expr.function("trim", expr.build(), chars)

  def ltrim(expr: LeftExpression): Expression =
    Expr.function("ltrim", expr.build())

  def ltrim(expr: LeftExpression, chars: String = " "): Expression =
    Expr.function("ltrim", expr.build(), chars)

  def rtrim(expr: LeftExpression): Expression =
    Expr.function("rtrim", expr.build())

  def rtrim(expr: LeftExpression, chars: String = " "): Expression =
    Expr.function("rtrim", expr.build(), chars)

  def lower(expr: LeftExpression): Expression =
    Expr.function("lower", expr.build())

  def upper(expr: LeftExpression): Expression =
    Expr.function("upper", expr.build())

  def reverse(expr: LeftExpression): Expression =
    Expr.function("reverse", expr.build())

  def repeat(expr: LeftExpression, n: Int): Expression =
    Expr.function("repeat", expr.build(), n)

  def lpad(expr: LeftExpression, length: Int, chars: String): Expression =
    Expr.function("lpad", expr.build(), length, chars)

  def rpad(expr: LeftExpression, length: Int, chars: String): Expression =
    Expr.function("rpad", expr.build(), length, chars)
}

trait TimeFunctions {

  def timestamp(expr: LeftExpression): Expression =
    Expr.function("timestamp", expr.build())

  def timestamp(expr: LeftExpression, formatString: String): Expression =
    Expr.function("timestamp", expr.build(), formatString)

  def unixTimestamp(expr: LeftExpression): Expression =
    Expr.function("unix_timestamp", expr.build())

  def unixTimestamp(expr: LeftExpression, formatString: String): Expression =
    Expr.function("unix_timestamp", expr.build(), formatString)

  def timestampCeil(expr: LeftExpression, period: String): Expression =
    Expr.function("timestamp_ceil", expr.build(), period)

  def timestampCeil(expr: LeftExpression,
                    period: String,
                    timezone: String,
                    origin: Option[String] = None): Expression =
    Expr.function("timestamp_ceil", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampFloor(expr: LeftExpression, period: String): Expression =
    Expr.function("timestamp_floor", expr.build(), period)

  def timestampFloor(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): Expression =
    Expr.function("timestamp_floor", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampShift(expr: LeftExpression, period: String): Expression =
    Expr.function("timestamp_shift", expr.build(), period)

  def timestampShift(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): Expression =
    Expr.function("timestamp_shift", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampExtract(expr: LeftExpression, unit: String): Expression =
    Expr.function("timestamp_extract", expr.build(), unit)

  def timestampExtract(expr: LeftExpression, unit: String, timezone: String): Expression =
    Expr.function("timestamp_extract", expr.build(), unit, timezone)

  def timestampParse(expr: String): Expression =
    Expr.function("timestamp_parse", expr)

  def timestampParse(expr: String, pattern: String): Expression =
    Expr.function("timestamp_parse", expr, pattern)

  def timestampParse(expr: String, pattern: String, timezone: String): Expression =
    Expr.function("timestamp_parse", expr, pattern, timezone)

  def timestampFormat(expr: LeftExpression): Expression =
    Expr.function("timestamp_format", expr.build())

  def timestampFormat(expr: LeftExpression, pattern: String): Expression =
    Expr.function("timestamp_format", expr.build(), pattern)

  def timestampFormat(expr: LeftExpression, pattern: String, timezone: String): Expression =
    Expr.function("timestamp_format", expr.build(), pattern, timezone)

}

trait MathFunctions {
  import ExpressionFunctions.cast

  def abs(x: LeftExpression): Expression                     = Expr.function("abs", x.build())
  def abs(x: LeftExpression, castType: CastType): Expression = abs(cast(x, castType))

  def acos(x: LeftExpression): Expression                     = Expr.function("acos", x.build())
  def acos(x: LeftExpression, castType: CastType): Expression = acos(cast(x, castType))

  def asin(x: LeftExpression): Expression                     = Expr.function("asin", x.build())
  def asin(x: LeftExpression, castType: CastType): Expression = asin(cast(x, castType))

  def atan(x: LeftExpression): Expression                     = Expr.function("atan", x.build())
  def atan(x: LeftExpression, castType: CastType): Expression = atan(cast(x, castType))

//  def atan2[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("atan2", x, y)
//  def atan2[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
//    Expr.function("atan2", x.build(), y)
//  def atan2[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
//    Expr.function("atan2", x, y.build())
  def atan2(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("atan2", x.build(), y.build())

  def atan2(x: LeftExpression,
            y: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): Expression =
    atan2(cast(x, xCastType), cast(y, yCastType))

  def cbrt(x: LeftExpression): Expression                     = Expr.function("cbrt", x.build())
  def cbrt(x: LeftExpression, castType: CastType): Expression = cbrt(cast(x, castType))

  def ceil(x: LeftExpression): Expression                     = Expr.function("ceil", x.build())
  def ceil(x: LeftExpression, castType: CastType): Expression = ceil(cast(x, castType))

//  def copysign[@specialized(Long, Double) T](x: T, y: T): Expression =
//    Expr.function("copysign", x, y)
//  def copysign[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
//    Expr.function("copysign", x.build(), y)
//  def copysign[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
//    Expr.function("copysign", x, y.build())
  def copysign(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("copysign", x.build(), y.build())
  def copysign(x: LeftExpression,
               y: LeftExpression,
               xCastType: CastType,
               yCastType: CastType): Expression =
    copysign(cast(x, xCastType), cast(y, yCastType))

  def cos(x: LeftExpression): Expression                     = Expr.function("cos", x.build())
  def cos(x: LeftExpression, castType: CastType): Expression = cos(cast(x, castType))

  def cosh(x: LeftExpression): Expression                     = Expr.function("cosh", x.build())
  def cosh(x: LeftExpression, castType: CastType): Expression = cosh(cast(x, castType))

  def cot(x: LeftExpression): Expression                     = Expr.function("cot", x.build())
  def cot(x: LeftExpression, castType: CastType): Expression = cot(cast(x, castType))

  def div(x: LeftExpression): Expression                     = Expr.function("div", x.build())
  def div(x: LeftExpression, castType: CastType): Expression = div(cast(x, castType))

  def exp(x: LeftExpression): Expression                     = Expr.function("exp", x.build())
  def exp(x: LeftExpression, castType: CastType): Expression = exp(cast(x, castType))

  def expm1(x: LeftExpression): Expression                     = Expr.function("expm1", x.build())
  def expm1(x: LeftExpression, castType: CastType): Expression = expm1(cast(x, castType))

  def floor[@specialized(Long, Double) T](x: T): Expression = Expr.function("floor", x)
  def floor(x: LeftExpression): Expression                  = Expr.function("floor", x.build())

  def getExponent[@specialized(Long, Double) T](x: T): Expression = Expr.function("getExponent", x)
  def getExponent(x: LeftExpression): Expression                  = Expr.function("getExponent", x.build())

  def hypot[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("hypot", x, y)
  def hypot[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
    Expr.function("hypot", x.build(), y)
  def hypot[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
    Expr.function("hypot", x, y.build())
  def hypot(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("hypot", x.build(), y.build())

  def log[@specialized(Long, Double) T](x: T): Expression = Expr.function("hypot", x)
  def log(x: LeftExpression): Expression                  = Expr.function("hypot", x.build())

  def log10[@specialized(Long, Double) T](x: T): Expression = Expr.function("log10", x)
  def log10(x: LeftExpression): Expression                  = Expr.function("log10", x.build())

  def log1p[@specialized(Long, Double) T](x: T): Expression = Expr.function("log1p", x)
  def log1p(x: LeftExpression): Expression                  = Expr.function("log1p", x.build())

  def max[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("max", x, y)
  def max[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
    Expr.function("max", x.build(), y)
  def max[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
    Expr.function("max", x, y.build())
  def max(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("max", x.build(), y.build())

  def min[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("min", x, y)
  def min[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
    Expr.function("min", x.build(), y)
  def min[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
    Expr.function("min", x, y.build())
  def min(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("min", x.build(), y.build())

  def nextafter[@specialized(Long, Double) T](x: T, y: T): Expression =
    Expr.function("nextafter", x, y)
  def nextafter[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
    Expr.function("nextafter", x.build(), y)
  def nextafter[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
    Expr.function("nextafter", x, y.build())
  def nextafter(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("nextafter", x.build(), y.build())

  def nextUp(x: LeftExpression): Expression = Expr.function("nextUp", x.build())
  def pi: Expression                        = Expr.function("pi")

  def pow(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("pow", x.build(), y.build())
  def remainder(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("remainder", x.build(), y.build())

  def rint[@specialized(Long, Double) T](x: T): Expression = Expr.function("rint", x)
  def rint(x: LeftExpression): Expression                  = Expr.function("rint", x.build())

  def round[@specialized(Long, Double) T](x: T, y: T): Expression = Expr.function("round", x, y)
  def round[@specialized(Long, Double) T](x: LeftExpression, y: T): Expression =
    Expr.function("round", x.build(), y)
  def round[@specialized(Long, Double) T](x: T, y: LeftExpression): Expression =
    Expr.function("round", x, y.build())
  def round(x: LeftExpression, y: LeftExpression): Expression =
    Expr.function("round", x.build(), y.build())

  def scalb[@specialized(Long, Double) T](d: T, sf: T): Expression = Expr.function("scalb", d, sf)
  def scalb[@specialized(Long, Double) T](d: LeftExpression, sf: T): Expression =
    Expr.function("scalb", d.build(), sf)
  def scalb[@specialized(Long, Double) T](d: T, sf: LeftExpression): Expression =
    Expr.function("scalb", d, sf.build())
  def scalb(d: LeftExpression, sf: LeftExpression): Expression =
    Expr.function("scalb", d.build(), sf.build())

  def signum[@specialized(Long, Double) T](x: T): Expression = Expr.function("signum", x)
  def signum(x: LeftExpression): Expression                  = Expr.function("signum", x.build())

  def sin[@specialized(Long, Double) T](x: T): Expression = Expr.function("sin", x)
  def sin(x: LeftExpression): Expression                  = Expr.function("sin", x.build())

  def sinh[@specialized(Long, Double) T](x: T): Expression = Expr.function("sinh", x)
  def sinh(x: LeftExpression): Expression                  = Expr.function("sinh", x.build())

  def sqrt[@specialized(Long, Double) T](x: T): Expression = Expr.function("sqrt", x)
  def sqrt(x: LeftExpression): Expression                  = Expr.function("sqrt", x.build())

  def tan[@specialized(Long, Double) T](x: T): Expression = Expr.function("tan", x)
  def tan(x: LeftExpression): Expression                  = Expr.function("tan", x.build())

  def tanh[@specialized(Long, Double) T](x: T): Expression = Expr.function("tanh", x)
  def tanh(x: LeftExpression): Expression                  = Expr.function("tanh", x.build())

  def todegrees[@specialized(Long, Double) T](x: T): Expression = Expr.function("todegrees", x)
  def todegrees(x: LeftExpression): Expression                  = Expr.function("todegrees", x.build())

  def toradians[@specialized(Long, Double) T](x: T): Expression = Expr.function("toradians", x)
  def toradians(x: LeftExpression): Expression                  = Expr.function("toradians", x.build())

  def ulp[@specialized(Long, Double) T](x: T): Expression = Expr.function("ulp", x)
  def ulp(x: LeftExpression): Expression                  = Expr.function("ulp", x.build())

}

object MathFunctions extends MathFunctions

trait ArrayFunctions {}

trait ApplyFunctions {}

trait ReductionFunctions {}

trait IPAddressFunctions

object ExpressionFunctions extends GeneralFunctions with StringFunctions with MathFunctions
// scalastyle:on
