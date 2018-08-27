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

package ing.wbaa.druid.dql

import ing.wbaa.druid.definitions._

import scala.language.implicitConversions

object FilterDSL {

  implicit class FilterDim(val dimension: Symbol) extends AnyVal {

    @inline
    private def valueFilter(name: String, valueOpt: Option[String] = None): Filter =
      SelectFilter(name, valueOpt)

    def ===(v: String): Filter = valueFilter(dimension.name, Option(v))

    def =!=(v: String): Filter = NotFilter(valueFilter(dimension.name, Option(v)))

    def isNotNull: Filter = NotFilter(valueFilter(dimension.name, None))

    def isNull: Filter = valueFilter(dimension.name, None)

    def in(values: String*): Filter = InFilter(dimension.name, values)

    def notIn(values: String*): Filter = NotFilter(InFilter(dimension.name, values))

    def like(pattern: String): Filter = LikeFilter(dimension.name, pattern)

    def regex(pattern: String): Filter = RegexFilter(dimension.name, pattern)

  }

  implicit class FilterOps(val filter: Filter) extends AnyVal {

    def and(others: Filter*): Filter = AndFilter(filter :: others.toList)

    def or(others: Filter*): Filter = OrFilter(filter :: others.toList)

  }

  def not(filter: Filter): Filter = NotFilter(filter)

  def conjunction(others: Filter*): Filter = AndFilter(others.toList)

  def disjunction(others: Filter*): Filter = OrFilter(others.toList)

}
