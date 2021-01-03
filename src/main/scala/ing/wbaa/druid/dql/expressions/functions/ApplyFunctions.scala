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

import ing.wbaa.druid.dql.expressions._

trait ApplyFunctions {

  def map[T: ExpressionLiteral](lambda: String, arr: T): LeftExpression =
    Expression.fun("map", lambda, arr)

  def cartesianMap[T: ExpressionLiteral](lambda: String, arr: T*): LeftExpression =
    Expression.funs("cartesian_map", lambda, arr)

  def filter[T: ExpressionLiteral](lambda: String, arr: T): LeftExpression =
    Expression.fun("filter", lambda, arr)

  def cartesianFold[T: ExpressionLiteral](lambda: String, arr: T*): LeftExpression =
    Expression.funs("cartesian_fold", lambda, arr)

  def any[T: ExpressionLiteral](lambda: String, arr: T): LeftExpression =
    Expression.fun("any", lambda, arr)

  def all[T: ExpressionLiteral](lambda: String, arr: T): LeftExpression =
    Expression.fun("all", lambda, arr)

}

object ApplyFunctions extends ApplyFunctions
