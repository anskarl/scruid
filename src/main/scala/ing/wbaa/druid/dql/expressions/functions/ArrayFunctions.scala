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

trait ArrayFunctions {

  def array[T: ExpressionLiteral](expr: T): LeftExpression =
    Expression.fun("array", expr)

  def arrayLength[T: ExpressionLiteral](arr: T): LeftExpression =
    Expression.fun("array_length", arr)

  def arrayOffset[T: ExpressionLiteral](arr: T, index: Long): LeftExpression =
    Expression.fun("array_offset", arr, index)

  def arrayOrdinal[T: ExpressionLiteral](arr: T, index: Long): LeftExpression =
    Expression.fun("array_ordinal", arr, index)

  def arrayContains[T1: ExpressionLiteral, T2: ExpressionLiteral](arr: T1,
                                                                  expr: T2): LeftExpression =
    Expression.fun("array_contains", arr, expr)

  def arrayOverlap[T1: ExpressionLiteral, T2: ExpressionLiteral](arr1: T1,
                                                                 arr2: T2): LeftExpression =
    Expression.fun("array_overlap", arr1, arr2)

  def arrayOffsetOf[T1: ExpressionLiteral, T2: ExpressionLiteral](arr: T1,
                                                                  expr: T2): LeftExpression =
    Expression.fun("array_offset_of", arr, expr)

  def arrayOrdinalOf[T1: ExpressionLiteral, T2: ExpressionLiteral](arr: T1,
                                                                   expr: T2): LeftExpression =
    Expression.fun("array_ordinal_of", arr, expr)

  def arrayPrepend[T1: ExpressionLiteral, T2: ExpressionLiteral](expr: T1,
                                                                 arr: T2): LeftExpression =
    Expression.fun("array_prepend", expr, arr)

  def arrayAppend[T1: ExpressionLiteral, T2: ExpressionLiteral](arr: T1, expr: T2): LeftExpression =
    Expression.fun("array_append", arr, expr)

  def arrayConcat[T1: ExpressionLiteral, T2: ExpressionLiteral](arr1: T1,
                                                                arr2: T2): LeftExpression =
    Expression.fun("array_concat", arr1, arr2)

  def arraySlice[T: ExpressionLiteral](arr1: T, start: Long, end: Long): LeftExpression =
    Expression.fun("array_slice", arr1, start, end)

  def arrayToString[T: ExpressionLiteral](arr: T, str: String): LeftExpression =
    Expression.fun("array_to_string", arr, str)

  def stringToArray[T: ExpressionLiteral](str1: T, str2: String): LeftExpression =
    Expression.fun("string_to_array", str1, str2)

}

object ArrayFunctions extends ArrayFunctions
