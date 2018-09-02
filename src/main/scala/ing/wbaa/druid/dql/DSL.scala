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

import ing.wbaa.druid.{ DimensionOrder, Direction, OrderByColumnSpec }
import ing.wbaa.druid.definitions._

import scala.language.implicitConversions

object DSL {

  final val TS = new TimestampDim()

  def not(op: Expression): Expression = op match {
    case neg: Not => neg.op
    case _        => new Not(op)
  }

  def disjunction(others: Expression*): Expression = new Or(others.toList)

  def conjunction(others: Expression*): Expression = new And(others.toList)

  def filter(value: Expression): Expression = new FilterOnlyOperator {
    override protected[dql] def createFilter: Filter = value.createFilter
  }

  implicit class DimOps(val s: Symbol) extends AnyVal {

    @inline
    private def eqVal(value: String): Expression = new EqString(s.name, value)

    @inline
    private def eqNum(value: Double): Expression = new EqDouble(s.name, value)

    @inline
    private def compareWith(other: Symbol): Expression = new Comparison(s.name :: other.name :: Nil)

    def ===(other: Symbol): Expression = compareWith(other)

    def ===(value: String): Expression = eqVal(value)

    def ===(value: Double): Expression = eqNum(value)

    def =!=(value: String): Expression = not(eqVal(value))

    def =!=(value: Double): Expression = not(eqNum(value))

    def =!=(other: Symbol): Expression = not(compareWith(other))

    def in(values: String*): Expression = new In(s.name, values.toList)

    def notIn(values: String*): Expression = not(new In(s.name, values.toList))

    def like(pattern: String): Expression = new Like(s.name, pattern)

    def regex(pattern: String): Expression = new Regex(s.name, pattern)

    def isNull: Expression = new NullDim(s.name)

    def isNotNull: Expression = not(this.isNull)

    def >(value: Double): Expression = new Gt(s.name, value)

    def >=(value: Double): Expression = new GtEq(s.name, value)

    def <(value: Double): Expression = new Lt(s.name, value)

    def =<(value: Double): Expression = new LtEq(s.name, value)

    def between(lower: String, upper: String): Bound =
      Bound(dimension = s.name,
            lower = Option(lower),
            upper = Option(upper),
            ordering = Option(DimensionOrder.lexicographic))

    def between(lower: Double, upper: Double): Bound =
      Bound(dimension = s.name,
            lower = Option(lower.toString),
            upper = Option(upper.toString),
            ordering = Option(DimensionOrder.numeric))

  }

  implicit class StringOps(val value: String) extends AnyVal {
    def ===(s: Symbol): Expression = s === value
    def =!=(s: Symbol): Expression = s =!= value
  }

  implicit class NumOps(val value: Double) extends AnyVal {
    def ===(s: Symbol): Expression = s === value
    def =!=(s: Symbol): Expression = s =!= value
    def >(s: Symbol): Expression   = s < value
    def >=(s: Symbol): Expression  = s =< value
    def <(s: Symbol): Expression   = s > value
    def =<(s: Symbol): Expression  = s >= value
  }

  implicit def symbolToOrderByColumnSpec(s: Symbol): OrderByColumnSpec =
    OrderByColumnSpec(dimension = s.name)

  implicit class OrderByColumnSpecWrapper(val s: Symbol) extends AnyVal {

    def asc: OrderByColumnSpec =
      OrderByColumnSpec(dimension = s.name, direction = Direction.ascending)

    def desc: OrderByColumnSpec =
      OrderByColumnSpec(dimension = s.name, direction = Direction.descending)

    def asc(order: DimensionOrder): OrderByColumnSpec =
      OrderByColumnSpec(
        dimension = s.name,
        direction = Direction.ascending,
        dimensionOrder = order
      )

    def desc(order: DimensionOrder): OrderByColumnSpec =
      OrderByColumnSpec(
        dimension = s.name,
        direction = Direction.descending,
        dimensionOrder = order
      )
  }

}
