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

import ing.wbaa.druid.dql.expressions.{ Expression, LeftExpression, RightExpression }

trait ArrayFunctions {

  def array(expr: LeftExpression*): LeftExpression =
    Expression.function("array", expr.map(_.build()))

  def arrayLength(arr: LeftExpression): LeftExpression =
    Expression.function("array_length", arr.build())

  def arrayOffset(arr: LeftExpression, index: Long): LeftExpression =
    Expression.function("array_offset", arr.build(), index)

  def arrayOrdinal(arr: LeftExpression, index: Long): LeftExpression =
    Expression.function("array_ordinal", arr.build(), index)

  def arrayContains(arr: LeftExpression, expr: LeftExpression): LeftExpression =
    Expression.function("array_contains", arr.build(), expr.build())

  def arrayContains(arr: LeftExpression, expr: RightExpression): LeftExpression =
    Expression.function("array_contains", arr.build(), expr.build())

  def arrayOverlap(arr1: LeftExpression, arr2: LeftExpression): LeftExpression =
    Expression.function("array_overlap", arr1.build(), arr2.build())

  def arrayOffsetOf(arr: LeftExpression, expr: LeftExpression): LeftExpression =
    Expression.function("array_offset_of", arr.build(), expr.build())

  def arrayOrdinalOf(arr: LeftExpression, expr: LeftExpression): LeftExpression =
    Expression.function("array_ordinal_of", arr.build(), expr.build())

  def arrayPrepend(expr: LeftExpression, arr: LeftExpression): LeftExpression =
    Expression.function("array_prepend", expr.build(), arr.build())

  def arrayAppend(arr: LeftExpression, expr: LeftExpression): LeftExpression =
    Expression.function("array_append", arr.build(), expr.build())

  def arrayConcat(arr1: LeftExpression, arr2: LeftExpression): LeftExpression =
    Expression.function("array_concat", arr1.build(), arr2.build())

  def arraySlice(arr1: LeftExpression, start: Long, end: Long): LeftExpression =
    Expression.function("array_slice", arr1.build(), start, end)

  def arrayToString(arr: LeftExpression, str: String): LeftExpression =
    Expression.function("array_to_string", arr.build(), s"'$str'")

  def stringToArray(str1: LeftExpression, str2: String): LeftExpression =
    Expression.function("string_to_array", str1.build(), s"'$str2'")

}

object ArrayFunctions extends ArrayFunctions
