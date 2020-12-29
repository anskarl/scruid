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

// scalastyle:off todo.comment
// scalastyle:off number.of.methods

// scalastyle:off method.name
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
  def +(other: Expression): Expression = ExpressionOps.add(this, other)
  def -(other: Expression): Expression = ExpressionOps.subtract(this, other)

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
// scalastyle:on method.name

object ExpressionOps {

  object BinaryOps extends Enumeration {
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

  def lit[T](n: T): LeftExpression = new LeftExpression(n.toString)

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

object Expr {

  def function(name: String, arg: String): LeftExpression =
    new LeftExpression(s"${name}(${arg})")

  def function(name: String, args: Any*): LeftExpression =
    new LeftExpression(s"${name}(${args.map(_.toString).mkString(",")})")

  def function(name: String, args: Iterable[String]): LeftExpression =
    new LeftExpression(s"${name}(${args.mkString(",")})")

  def apply(v: String): LeftExpression        = new LeftExpression(v)
  def apply[T: Numeric](v: T): LeftExpression = new LeftExpression(v.toString)

  def apply[T <: CharSequence](values: Iterable[T]): LeftExpression =
    new LeftExpression(values.mkString("[", ",", "]"))
  def apply[T: Numeric](values: Iterable[T]): LeftExpression = this.apply(values.map(_.toString))

}

class LeftExpression(value: String) extends Expression {
  override def build(): String = this.value

  def cast(to: CastType): LeftExpression = GeneralFunctions.cast(this, to)

  def cast(to: String): LeftExpression =
    cast(CastType.decode(to).getOrElse(throw new IllegalArgumentException))

  // Binary additive
  def +(other: LeftExpression): LeftExpression =
    new LeftExpression(s"${this.build()}+${other.build()}")

  def -(other: LeftExpression): LeftExpression =
    new LeftExpression(s"${this.build()}-${other.build()}")
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
    // scalastyle:off var.field
    private var caseStatements = Seq.empty[(Expression, Expression)]
    // scalastyle:on var.field

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
    // scalastyle:off var.field
    private var valueResults = Seq.empty[(Expression, Expression)]
    // scalastyle:on var.field

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

object GeneralFunctions extends GeneralFunctions

trait StringFunctions {

  def concat(args: LeftExpression*): LeftExpression =
    Expr.function("concat", args.map(_.build()))

  def format(pattern: String, args: LeftExpression*): LeftExpression =
    Expr.function("format", args.scanLeft(pattern)((_, e) => e.build()))

  def like(expr: LeftExpression, pattern: String): LeftExpression =
    Expr(s"${expr.build()} LIKE ${pattern}")

  def like(expr: LeftExpression, pattern: String, escape: String): LeftExpression =
    Expr(s"${expr.build()} LIKE ${pattern} ${escape}")

  def lookup(expr: LeftExpression, lookupName: String): LeftExpression =
    Expr.function("lookup", expr.build(), lookupName)

  def parseLong(value: String): LeftExpression =
    Expr.function("parse_long", value)

  def parseLong(value: String, radix: Int): LeftExpression =
    Expr.function("parse_long", value, radix)

  def regexpExtract(expr: LeftExpression, pattern: String): LeftExpression =
    Expr.function("timestamp_ceil", expr.build(), pattern)

  def regexpExtract(expr: LeftExpression, pattern: String, index: Int): LeftExpression =
    Expr.function("regexp_extract", expr.build(), pattern, index)

  def regexpLike(expr: LeftExpression, pattern: String): LeftExpression =
    Expr.function("regexp_like", expr.build(), pattern)

  def containsString(expr: LeftExpression, pattern: String): LeftExpression =
    Expr.function("contains_string", expr.build(), pattern)

  def icontainsString(expr: LeftExpression, pattern: String): LeftExpression =
    Expr.function("icontains_string", expr.build(), pattern)

  def replace(expr: LeftExpression, pattern: String, replacement: String): LeftExpression =
    Expr.function("replace", expr.build(), pattern, replacement)

  def substring(expr: LeftExpression, index: Int, length: Int): LeftExpression =
    Expr.function("substring", expr.build(), index, length)

  // right(expr, length) returns the rightmost length characters from a string
  def right(expr: LeftExpression, length: Int): LeftExpression =
    Expr.function("right", expr.build(), length)

  def left(expr: LeftExpression, length: Int): LeftExpression =
    Expr.function("left", expr.build(), length)

  def strlen(expr: LeftExpression): LeftExpression =
    Expr.function("left", expr.build())

  def strpos(haystack: LeftExpression, needle: String): LeftExpression =
    Expr.function("strpos", haystack.build(), needle)

  def strpos(haystack: LeftExpression, needle: String, fromIndex: Int): LeftExpression =
    Expr.function("strpos", haystack.build(), needle, fromIndex)

  def trim(expr: LeftExpression): LeftExpression =
    Expr.function("trim", expr.build())

  def trim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expr.function("trim", expr.build(), chars)

  def ltrim(expr: LeftExpression): LeftExpression =
    Expr.function("ltrim", expr.build())

  def ltrim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expr.function("ltrim", expr.build(), chars)

  def rtrim(expr: LeftExpression): LeftExpression =
    Expr.function("rtrim", expr.build())

  def rtrim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expr.function("rtrim", expr.build(), chars)

  def lower(expr: LeftExpression): LeftExpression =
    Expr.function("lower", expr.build())

  def upper(expr: LeftExpression): LeftExpression =
    Expr.function("upper", expr.build())

  def reverse(expr: LeftExpression): LeftExpression =
    Expr.function("reverse", expr.build())

  def repeat(expr: LeftExpression, n: Int): LeftExpression =
    Expr.function("repeat", expr.build(), n)

  def lpad(expr: LeftExpression, length: Int, chars: String): LeftExpression =
    Expr.function("lpad", expr.build(), length, chars)

  def rpad(expr: LeftExpression, length: Int, chars: String): LeftExpression =
    Expr.function("rpad", expr.build(), length, chars)
}

trait TimeFunctions {

  def timestamp(expr: LeftExpression): LeftExpression =
    Expr.function("timestamp", expr.build())

  def timestamp(expr: LeftExpression, formatString: String): LeftExpression =
    Expr.function("timestamp", expr.build(), formatString)

  def unixTimestamp(expr: LeftExpression): LeftExpression =
    Expr.function("unix_timestamp", expr.build())

  def unixTimestamp(expr: LeftExpression, formatString: String): LeftExpression =
    Expr.function("unix_timestamp", expr.build(), formatString)

  def timestampCeil(expr: LeftExpression, period: String): LeftExpression =
    Expr.function("timestamp_ceil", expr.build(), period)

  def timestampCeil(expr: LeftExpression,
                    period: String,
                    timezone: String,
                    origin: Option[String] = None): LeftExpression =
    Expr.function("timestamp_ceil", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampFloor(expr: LeftExpression, period: String): LeftExpression =
    Expr.function("timestamp_floor", expr.build(), period)

  def timestampFloor(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): LeftExpression =
    Expr.function("timestamp_floor", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampShift(expr: LeftExpression, period: String): LeftExpression =
    Expr.function("timestamp_shift", expr.build(), period)

  def timestampShift(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): LeftExpression =
    Expr.function("timestamp_shift", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampExtract(expr: LeftExpression, unit: String): LeftExpression =
    Expr.function("timestamp_extract", expr.build(), unit)

  def timestampExtract(expr: LeftExpression, unit: String, timezone: String): LeftExpression =
    Expr.function("timestamp_extract", expr.build(), unit, timezone)

  def timestampParse(expr: String): LeftExpression =
    Expr.function("timestamp_parse", expr)

  def timestampParse(expr: String, pattern: String): LeftExpression =
    Expr.function("timestamp_parse", expr, pattern)

  def timestampParse(expr: String, pattern: String, timezone: String): LeftExpression =
    Expr.function("timestamp_parse", expr, pattern, timezone)

  def timestampFormat(expr: LeftExpression): LeftExpression =
    Expr.function("timestamp_format", expr.build())

  def timestampFormat(expr: LeftExpression, pattern: String): LeftExpression =
    Expr.function("timestamp_format", expr.build(), pattern)

  def timestampFormat(expr: LeftExpression, pattern: String, timezone: String): LeftExpression =
    Expr.function("timestamp_format", expr.build(), pattern, timezone)

}

trait MathFunctions {
  import ExpressionFunctions.cast

  def abs(x: LeftExpression): LeftExpression                     = new LeftExpression(s"abs(${x.build()})")
  def abs(x: LeftExpression, castType: CastType): LeftExpression = abs(cast(x, castType))

  def acos(x: LeftExpression): LeftExpression                     = Expr.function("acos", x.build())
  def acos(x: LeftExpression, castType: CastType): LeftExpression = acos(cast(x, castType))

  def asin(x: LeftExpression): LeftExpression                     = Expr.function("asin", x.build())
  def asin(x: LeftExpression, castType: CastType): LeftExpression = asin(cast(x, castType))

  def atan(x: LeftExpression): LeftExpression                     = Expr.function("atan", x.build())
  def atan(x: LeftExpression, castType: CastType): LeftExpression = atan(cast(x, castType))

  def atan2(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("atan2", x.build(), y.build())

  def atan2(x: LeftExpression,
            y: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    atan2(cast(x, xCastType), cast(y, yCastType))

  def cbrt(x: LeftExpression): LeftExpression                     = Expr.function("cbrt", x.build())
  def cbrt(x: LeftExpression, castType: CastType): LeftExpression = cbrt(cast(x, castType))

  def ceil(x: LeftExpression): LeftExpression                     = Expr.function("ceil", x.build())
  def ceil(x: LeftExpression, castType: CastType): LeftExpression = ceil(cast(x, castType))

  def copysign(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("copysign", x.build(), y.build())

  def copysign(x: LeftExpression,
               y: LeftExpression,
               xCastType: CastType,
               yCastType: CastType): LeftExpression =
    copysign(cast(x, xCastType), cast(y, yCastType))

  def cos(x: LeftExpression): LeftExpression                     = Expr.function("cos", x.build())
  def cos(x: LeftExpression, castType: CastType): LeftExpression = cos(cast(x, castType))

  def cosh(x: LeftExpression): LeftExpression                     = Expr.function("cosh", x.build())
  def cosh(x: LeftExpression, castType: CastType): LeftExpression = cosh(cast(x, castType))

  def cot(x: LeftExpression): LeftExpression                     = Expr.function("cot", x.build())
  def cot(x: LeftExpression, castType: CastType): LeftExpression = cot(cast(x, castType))

  def div(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("div", x.build(), y.build())
  def div(x: LeftExpression, y: LeftExpression, castType: CastType): LeftExpression =
    div(cast(x, castType), cast(y, castType))

  def exp(x: LeftExpression): LeftExpression                     = Expr.function("exp", x.build())
  def exp(x: LeftExpression, castType: CastType): LeftExpression = exp(cast(x, castType))

  def expm1(x: LeftExpression): LeftExpression                     = Expr.function("expm1", x.build())
  def expm1(x: LeftExpression, castType: CastType): LeftExpression = expm1(cast(x, castType))

  def floor(x: LeftExpression): LeftExpression                     = Expr.function("floor", x.build())
  def floor(x: LeftExpression, castType: CastType): LeftExpression = floor(cast(x, castType))

  def getExponent(x: LeftExpression): LeftExpression = Expr.function("getExponent", x.build())
  def getExponent(x: LeftExpression, castType: CastType): LeftExpression =
    getExponent(cast(x, castType))

  def hypot(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("hypot", x.build(), y.build())

  def hypot(x: LeftExpression,
            y: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    hypot(cast(x, xCastType), cast(y, yCastType))

  def log(x: LeftExpression): LeftExpression                     = Expr.function("log", x.build())
  def log(x: LeftExpression, castType: CastType): LeftExpression = log(cast(x, castType))

  def log10(x: LeftExpression): LeftExpression                     = Expr.function("log10", x.build())
  def log10(x: LeftExpression, castType: CastType): LeftExpression = log10(cast(x, castType))

  def log1p(x: LeftExpression): LeftExpression                     = Expr.function("log1p", x.build())
  def log1p(x: LeftExpression, castType: CastType): LeftExpression = log1p(cast(x, castType))

  def max(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("max", x.build(), y.build())

  def max(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    max(cast(x, xCastType), cast(y, yCastType))

  def min(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("min", x.build(), y.build())

  def min(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    min(cast(x, xCastType), cast(y, yCastType))

  def nextAfter(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("nextAfter", x.build(), y.build())

  def nextAfter(x: LeftExpression,
                y: LeftExpression,
                xCastType: CastType,
                yCastType: CastType): LeftExpression =
    nextAfter(cast(x, xCastType), cast(y, yCastType))

  def nextUp(x: LeftExpression): LeftExpression                     = Expr.function("nextUp", x.build())
  def nextUp(x: LeftExpression, castType: CastType): LeftExpression = nextUp(cast(x, castType))

  def pi: LeftExpression = Expr.function("pi")

  def pow(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("pow", x.build(), y.build())
  def pow(x: LeftExpression,
          y: LeftExpression,
          xCastType: CastType,
          yCastType: CastType): LeftExpression =
    pow(cast(x, xCastType), cast(y, yCastType))

  def remainder(x: LeftExpression, y: LeftExpression): LeftExpression =
    Expr.function("remainder", x.build(), y.build())
  def remainder(x: LeftExpression,
                y: LeftExpression,
                xCastType: CastType,
                yCastType: CastType): LeftExpression =
    remainder(cast(x, xCastType), cast(y, yCastType))

  def rint(x: LeftExpression): LeftExpression                     = Expr.function("rint", x.build())
  def rint(x: LeftExpression, castType: CastType): LeftExpression = rint(cast(x, castType))

  def round(x: LeftExpression, scale: LeftExpression): LeftExpression =
    Expr.function("round", x.build(), scale.cast(CastType.Long).build())

  def round(x: LeftExpression, scale: Int): LeftExpression =
    Expr.function("round", x.build(), scale.toString)

  def round(x: LeftExpression): LeftExpression = round(x, scale = 0)

  def round(x: LeftExpression, castType: CastType, scale: Int): LeftExpression =
    round(cast(x, castType), scale)
  def round(x: LeftExpression, castType: CastType): LeftExpression = round(x, castType, scale = 0)

  def round(x: LeftExpression, scale: LeftExpression, castType: CastType): LeftExpression =
    round(x.cast(castType), scale)

  def scalb(d: LeftExpression, sf: Int): LeftExpression =
    Expr.function("scalb", d.build(), sf.toString)

  def scalb(d: LeftExpression, sf: LeftExpression): LeftExpression =
    Expr.function("scalb", d.build(), sf.build())

  def scalb(d: LeftExpression,
            sf: LeftExpression,
            xCastType: CastType,
            yCastType: CastType): LeftExpression =
    scalb(cast(d, xCastType), cast(sf, yCastType))

  def signum(x: LeftExpression): LeftExpression                     = Expr.function("signum", x.build())
  def signum(x: LeftExpression, castType: CastType): LeftExpression = signum(cast(x, castType))

  def sin(x: LeftExpression): LeftExpression                     = Expr.function("sin", x.build())
  def sin(x: LeftExpression, castType: CastType): LeftExpression = sin(cast(x, castType))

  def sinh(x: LeftExpression): LeftExpression                     = Expr.function("sinh", x.build())
  def sinh(x: LeftExpression, castType: CastType): LeftExpression = sinh(cast(x, castType))

  def sqrt(x: LeftExpression): LeftExpression                     = Expr.function("sqrt", x.build())
  def sqrt(x: LeftExpression, castType: CastType): LeftExpression = sqrt(cast(x, castType))

  def tan(x: LeftExpression): LeftExpression                     = Expr.function("tan", x.build())
  def tan(x: LeftExpression, castType: CastType): LeftExpression = tan(cast(x, castType))

  def tanh(x: LeftExpression): LeftExpression                     = Expr.function("tanh", x.build())
  def tanh(x: LeftExpression, castType: CastType): LeftExpression = tanh(cast(x, castType))

  def todegrees(x: LeftExpression): LeftExpression = Expr.function("todegrees", x.build())
  def todegrees(x: LeftExpression, castType: CastType): LeftExpression =
    todegrees(cast(x, castType))

  def toradians(x: LeftExpression): LeftExpression = Expr.function("toradians", x.build())
  def toradians(x: LeftExpression, castType: CastType): LeftExpression =
    toradians(cast(x, castType))

  def ulp(x: LeftExpression): LeftExpression                     = Expr.function("ulp", x.build())
  def ulp(x: LeftExpression, castType: CastType): LeftExpression = ulp(cast(x, castType))

}

object MathFunctions extends MathFunctions

trait ArrayFunctions

trait ApplyFunctions

trait ReductionFunctions

trait IPAddressFunctions

object ExpressionFunctions extends GeneralFunctions with StringFunctions with MathFunctions
// scalastyle:on todo.comment
// scalastyle:on number.of.methods
