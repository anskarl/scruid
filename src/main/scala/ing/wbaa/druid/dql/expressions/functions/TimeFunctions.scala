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

trait TimeFunctions {

  def timestamp[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("timestamp", expr)

  def timestamp[T: ExpressionLiteral](expr: T, formatString: String): LeftExpression =
    Expression.fun("timestamp", expr, formatString)

  def unixTimestamp[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("unix_timestamp", expr)

  def unixTimestamp[T: ExpressionLiteral](expr: T, formatString: String): LeftExpression =
    Expression.fun("unix_timestamp", expr, formatString)

  def timestampCeil[T: ExpressionLiteral](expr: T, period: String): LeftExpression =
    Expression.fun("timestamp_ceil", expr, period)

  def timestampCeil[T: ExpressionLiteral](expr: T,
                                          period: String,
                                          timezone: String,
                                          origin: Option[String] = None): LeftExpression =
    Expression.fun("timestamp_ceil", expr, period, origin.getOrElse("null"), timezone)

  def timestampFloor[T: ExpressionLiteral](expr: T, period: String): LeftExpression =
    Expression.fun("timestamp_floor", expr, period)

  def timestampFloor[T: ExpressionLiteral](expr: T,
                                           period: String,
                                           timezone: String,
                                           origin: Option[String] = None): LeftExpression =
    Expression.fun("timestamp_floor", expr, period, origin.getOrElse("null"), timezone)

  def timestampShift[T: ExpressionLiteral](expr: T, period: String): LeftExpression =
    Expression.fun("timestamp_shift", expr, period)

  def timestampShift[T: ExpressionLiteral](expr: T,
                                           period: String,
                                           timezone: String,
                                           origin: Option[String] = None): LeftExpression =
    Expression.fun("timestamp_shift", expr, period, origin.getOrElse("null"), timezone)

  def timestampExtract[T: ExpressionLiteral](expr: T, unit: String): LeftExpression =
    Expression.fun("timestamp_extract", expr, unit)

  def timestampExtract[T: ExpressionLiteral](expr: T,
                                             unit: String,
                                             timezone: String): LeftExpression =
    Expression.fun("timestamp_extract", expr, unit, timezone)

  def timestampParse[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("timestamp_parse", expr)

  def timestampParse[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("timestamp_parse", expr, pattern)

  def timestampParse[T: ExpressionLiteral](expr: T,
                                           pattern: String,
                                           timezone: String): LeftExpression =
    Expression.fun("timestamp_parse", expr, pattern, timezone)

  def timestampFormat[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("timestamp_format", expr)

  def timestampFormat[T: ExpressionLiteral](expr: T, pattern: String): LeftExpression =
    Expression.fun("timestamp_format", expr, pattern)

  def timestampFormat[T: ExpressionLiteral](expr: T,
                                            pattern: String,
                                            timezone: String): LeftExpression =
    Expression.fun("timestamp_format", expr, pattern, timezone)

}

object TimeFunctions extends TimeFunctions
