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

package ing.wbaa.druid.dql.expressions.functions

import ing.wbaa.druid.dql.expressions.{ Expression, ExpressionLiteral, LeftExpression }
import scala.reflect.ClassTag

// scalastyle:off number.of.methods
trait StringFunctions {
  import ing.wbaa.druid.dql.expressions.ExpressionLiteral._


  def concat[T](args: T*): LeftExpression =
    new LeftExpression(
      args.map(AnyExpressionLiteral.literal(_)).mkString("concat(", ",", ")")
    )

  def format[T: ExpressionLiteral](pattern: String, args: T*): LeftExpression =
    Expression.funs("format", pattern, args)

  def like[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression = {
    val value = implicitly[ExpressionLiteral[T]].literal(expr)
    new LeftExpression(s"${value} LIKE ${pattern}")
  }

  def like[T: ExpressionLiteral](expr: T, pattern: String, escape: String): LeftExpression = {
    val value = implicitly[ExpressionLiteral[T]].literal(expr)
    new LeftExpression(s"${value} LIKE ${pattern} ${escape}")
  }

  def lookup[T: ExpressionLiteral](expr: T, lookupName: String): LeftExpression =
    Expression.fun("lookup", expr, lookupName)

  def parseLong[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("parse_long", expr)

  def parseLong[T: ExpressionLiteral](expr: T, radix: Int): LeftExpression =
    Expression.fun("parse_long", expr, radix)

  def regexpExtract[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("timestamp_ceil", expr, pattern)

  def regexpExtract[T: ExpressionLiteral](expr: T, pattern: String, index: Int): LeftExpression =
    Expression.fun("regexp_extract", expr, pattern, index)

  def regexpLike[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("regexp_extract", expr, pattern)

  def containsString[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("contains_string", expr, pattern)

  def icontainsString[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("icontains_string", expr, pattern)

  def replace[T: ExpressionLiteral](expr: T, pattern: String, replacement: String): LeftExpression =
    Expression.fun("replace", expr, pattern, replacement)

  def substring[T: ExpressionLiteral](expr: T, index: Int, length: Int): LeftExpression =
    Expression.fun("substring", expr, index, length)

  def right[T: ExpressionLiteral](expr: T, length: Int): LeftExpression =
    Expression.fun("right", expr, length)

  def left[T: ExpressionLiteral](expr: T, length: Int): LeftExpression =
    Expression.fun("left", expr, length)

  def strlen[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("left", expr)

  def strpos[T: ExpressionLiteral](haystack: T, needle: String): LeftExpression =
    Expression.fun("strpos", haystack, needle)

  def strpos[T: ExpressionLiteral](haystack: T, needle: String, fromIndex: Int): LeftExpression =
    Expression.fun("strpos", haystack, needle, fromIndex)

  def trim[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("trim", expr)

  def trim[T: ExpressionLiteral](expr: T, chars: String = " "): LeftExpression =
    Expression.fun("trim", expr, chars)

  def ltrim[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("ltrim", expr)

  def ltrim[T: ExpressionLiteral](expr: T, chars: String = " "): LeftExpression =
    Expression.fun("ltrim", expr, chars)

  def rtrim[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("rtrim", expr)

  def rtrim[T: ExpressionLiteral](expr: T, chars: String = " "): LeftExpression =
    Expression.fun("rtrim", expr, chars)

  def lower[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("lower", expr)

  def upper[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("upper", expr)

  def reverse[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("reverse", expr)

  def repeat[T: ExpressionLiteral](expr: T, n: Int): LeftExpression =
    Expression.fun("repeat", expr, n)

  def lpad[T: ExpressionLiteral](expr: T, length: Int, chars: String): LeftExpression =
    Expression.fun("lpad", expr, length, chars)

  def rpad[T: ExpressionLiteral](expr: T, length: Int, chars: String): LeftExpression =
    Expression.fun("rpad", expr, length, chars)

}
// scalastyle:on number.of.methods

object StringFunctions extends StringFunctions
