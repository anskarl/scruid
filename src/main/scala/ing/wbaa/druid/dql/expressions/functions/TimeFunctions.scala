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

trait TimeFunctions {

  def timestamp(expr: LeftExpression): LeftExpression =
    Expression.function("timestamp", expr.build())

  def timestamp(expr: LeftExpression, formatString: String): LeftExpression =
    Expression.function("timestamp", expr.build(), formatString)

  def unixTimestamp(expr: LeftExpression): LeftExpression =
    Expression.function("unix_timestamp", expr.build())

  def unixTimestamp(expr: LeftExpression, formatString: String): LeftExpression =
    Expression.function("unix_timestamp", expr.build(), formatString)

  def timestampCeil(expr: LeftExpression, period: String): LeftExpression =
    Expression.function("timestamp_ceil", expr.build(), period)

  def timestampCeil(expr: LeftExpression,
                    period: String,
                    timezone: String,
                    origin: Option[String] = None): LeftExpression =
    Expression.function("timestamp_ceil", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampFloor(expr: LeftExpression, period: String): LeftExpression =
    Expression.function("timestamp_floor", expr.build(), period)

  def timestampFloor(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): LeftExpression =
    Expression.function("timestamp_floor", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampShift(expr: LeftExpression, period: String): LeftExpression =
    Expression.function("timestamp_shift", expr.build(), period)

  def timestampShift(expr: LeftExpression,
                     period: String,
                     timezone: String,
                     origin: Option[String] = None): LeftExpression =
    Expression.function("timestamp_shift", expr.build(), period, origin.getOrElse("null"), timezone)

  def timestampExtract(expr: LeftExpression, unit: String): LeftExpression =
    Expression.function("timestamp_extract", expr.build(), unit)

  def timestampExtract(expr: LeftExpression, unit: String, timezone: String): LeftExpression =
    Expression.function("timestamp_extract", expr.build(), unit, timezone)

  def timestampParse(expr: String): LeftExpression =
    Expression.function("timestamp_parse", expr)

  def timestampParse(expr: String, pattern: String): LeftExpression =
    Expression.function("timestamp_parse", expr, pattern)

  def timestampParse(expr: String, pattern: String, timezone: String): LeftExpression =
    Expression.function("timestamp_parse", expr, pattern, timezone)

  def timestampFormat(expr: LeftExpression): LeftExpression =
    Expression.function("timestamp_format", expr.build())

  def timestampFormat(expr: LeftExpression, pattern: String): LeftExpression =
    Expression.function("timestamp_format", expr.build(), pattern)

  def timestampFormat(expr: LeftExpression, pattern: String, timezone: String): LeftExpression =
    Expression.function("timestamp_format", expr.build(), pattern, timezone)

}

object TimeFunctions extends TimeFunctions
