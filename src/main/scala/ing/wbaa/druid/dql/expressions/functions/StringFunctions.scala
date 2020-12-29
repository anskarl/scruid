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

import ing.wbaa.druid.dql.expressions.{ Expression, LeftExpression }

// scalastyle:off number.of.methods
trait StringFunctions {

  def concat(args: LeftExpression*): LeftExpression =
    Expression.function("concat", args.map(_.build()))

  def format(pattern: String, args: LeftExpression*): LeftExpression =
    Expression.function("format", args.scanLeft(pattern)((_, e) => e.build()))

  def like(expr: LeftExpression, pattern: String): LeftExpression =
    new LeftExpression(s"${expr.build()} LIKE ${pattern}")

  def like(expr: LeftExpression, pattern: String, escape: String): LeftExpression =
    new LeftExpression(s"${expr.build()} LIKE ${pattern} ${escape}")

  def lookup(expr: LeftExpression, lookupName: String): LeftExpression =
    Expression.function("lookup", expr.build(), lookupName)

  def parseLong(value: String): LeftExpression =
    Expression.function("parse_long", value)

  def parseLong(value: String, radix: Int): LeftExpression =
    Expression.function("parse_long", value, radix)

  def regexpExtract(expr: LeftExpression, pattern: String): LeftExpression =
    Expression.function("timestamp_ceil", expr.build(), pattern)

  def regexpExtract(expr: LeftExpression, pattern: String, index: Int): LeftExpression =
    Expression.function("regexp_extract", expr.build(), pattern, index)

  def regexpLike(expr: LeftExpression, pattern: String): LeftExpression =
    Expression.function("regexp_like", expr.build(), pattern)

  def containsString(expr: LeftExpression, pattern: String): LeftExpression =
    Expression.function("contains_string", expr.build(), pattern)

  def icontainsString(expr: LeftExpression, pattern: String): LeftExpression =
    Expression.function("icontains_string", expr.build(), pattern)

  def replace(expr: LeftExpression, pattern: String, replacement: String): LeftExpression =
    Expression.function("replace", expr.build(), pattern, replacement)

  def substring(expr: LeftExpression, index: Int, length: Int): LeftExpression =
    Expression.function("substring", expr.build(), index, length)

  def right(expr: LeftExpression, length: Int): LeftExpression =
    Expression.function("right", expr.build(), length)

  def left(expr: LeftExpression, length: Int): LeftExpression =
    Expression.function("left", expr.build(), length)

  def strlen(expr: LeftExpression): LeftExpression =
    Expression.function("left", expr.build())

  def strpos(haystack: LeftExpression, needle: String): LeftExpression =
    Expression.function("strpos", haystack.build(), needle)

  def strpos(haystack: LeftExpression, needle: String, fromIndex: Int): LeftExpression =
    Expression.function("strpos", haystack.build(), needle, fromIndex)

  def trim(expr: LeftExpression): LeftExpression =
    Expression.function("trim", expr.build())

  def trim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expression.function("trim", expr.build(), chars)

  def ltrim(expr: LeftExpression): LeftExpression =
    Expression.function("ltrim", expr.build())

  def ltrim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expression.function("ltrim", expr.build(), chars)

  def rtrim(expr: LeftExpression): LeftExpression =
    Expression.function("rtrim", expr.build())

  def rtrim(expr: LeftExpression, chars: String = " "): LeftExpression =
    Expression.function("rtrim", expr.build(), chars)

  def lower(expr: LeftExpression): LeftExpression =
    Expression.function("lower", expr.build())

  def upper(expr: LeftExpression): LeftExpression =
    Expression.function("upper", expr.build())

  def reverse(expr: LeftExpression): LeftExpression =
    Expression.function("reverse", expr.build())

  def repeat(expr: LeftExpression, n: Int): LeftExpression =
    Expression.function("repeat", expr.build(), n)

  def lpad(expr: LeftExpression, length: Int, chars: String): LeftExpression =
    Expression.function("lpad", expr.build(), length, chars)

  def rpad(expr: LeftExpression, length: Int, chars: String): LeftExpression =
    Expression.function("rpad", expr.build(), length, chars)

}
// scalastyle:on number.of.methods

object StringFunctions extends StringFunctions
