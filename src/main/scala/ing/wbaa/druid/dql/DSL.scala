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

import ing.wbaa.druid.{ DimensionOrder, DimensionOrderType, Direction, OrderByColumnSpec }
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.expressions._

import scala.language.implicitConversions

object DSL {

  def DQL: QueryBuilder = new QueryBuilder

  implicit def symbolToDim(s: Symbol): Dim = new Dim(s.name)

  implicit class StringToColumn(val sc: StringContext) extends AnyVal {
    def d(args: Any*): Dim = Dim(sc.s(args: _*))
  }

  def dim(name: String): Dim = Dim(name)

  def not(op: FilteringExpression): FilteringExpression = op match {
    case neg: Not => neg.op
    case _        => new Not(op)
  }

  def disjunction(others: FilteringExpression*): FilteringExpression = new Or(others.toList)

  def conjunction(others: FilteringExpression*): FilteringExpression = new And(others.toList)

  def filter(value: FilteringExpression): FilteringExpression = new FilterOnlyOperator {
    override protected[dql] def createFilter: Filter = value.createFilter
  }

  implicit def symbolToOrderByColumnSpec(s: Symbol): OrderByColumnSpec =
    OrderByColumnSpec(dimension = s.name)

  implicit class OrderByColumnSpecValueClass(val s: Dim) extends AnyVal {

    def asc: OrderByColumnSpec =
      OrderByColumnSpec(dimension = s.name, direction = Direction.ascending)

    def desc: OrderByColumnSpec =
      OrderByColumnSpec(dimension = s.name, direction = Direction.descending)

    def asc(orderType: DimensionOrderType): OrderByColumnSpec =
      OrderByColumnSpec(
        dimension = s.name,
        direction = Direction.ascending,
        dimensionOrder = DimensionOrder(orderType)
      )

    def desc(orderType: DimensionOrderType): OrderByColumnSpec =
      OrderByColumnSpec(
        dimension = s.name,
        direction = Direction.descending,
        dimensionOrder = DimensionOrder(orderType)
      )
  }

  def longSum(fieldName: Symbol): LongSumAgg = new LongSumAgg(fieldName.name)

  def longMax(fieldName: Symbol): LongMaxAgg = new LongMaxAgg(fieldName.name)

  def longFirst(fieldName: Symbol): LongFirstAgg = new LongFirstAgg(fieldName.name)

  def longLast(fieldName: Symbol): LongLastAgg = new LongLastAgg(fieldName.name)

  def doubleSum(fieldName: Symbol): DoubleSumAgg = new DoubleSumAgg(fieldName.name)

  def doubleMax(fieldName: Symbol): DoubleMaxAgg = new DoubleMaxAgg(fieldName.name)

  def doubleFirst(fieldName: Symbol): DoubleFirstAgg = new DoubleFirstAgg(fieldName.name)

  def doubleLast(fieldName: Symbol): DoubleLastAgg = new DoubleLastAgg(fieldName.name)

  def thetaSketch(fieldName: Symbol): ThetaSketchAgg = ThetaSketchAgg(fieldName.name)

  def hyperUnique(fieldName: Symbol): HyperUniqueAgg = HyperUniqueAgg(fieldName.name)

  def inFiltered(dimension: Symbol,
                 aggregator: AggregationExpression,
                 values: String*): InFilteredAgg =
    InFilteredAgg(dimension.name, values, aggregator.build())

  def selectorFiltered(dimension: Symbol,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    SelectorFilteredAgg(dimension.name, Option(value), aggregator.build())

  def selectorFiltered(dimension: Symbol, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dimension.name, None, aggregator.build())

  implicit class SymbolAgg(val s: Symbol) extends AnyVal {
    def longSum: AggregationExpression   = DSL.longSum(s)
    def longMax: AggregationExpression   = DSL.longMax(s)
    def longFirst: AggregationExpression = DSL.longFirst(s)
    def longLast: AggregationExpression  = DSL.longLast(s)

    def doubleSum: AggregationExpression   = DSL.doubleSum(s)
    def doubleMax: AggregationExpression   = DSL.doubleMax(s)
    def doubleFirst: AggregationExpression = DSL.doubleFirst(s)
    def doubleLast: AggregationExpression  = DSL.doubleLast(s)

    def thetaSketch: AggregationExpression = DSL.thetaSketch(s)

    def hyperUnique: AggregationExpression = DSL.hyperUnique(s)

    def inFiltered(aggregator: AggregationExpression, values: String*): AggregationExpression =
      DSL.inFiltered(s, aggregator, values: _*)

    def selectorFiltered(aggregator: AggregationExpression): SelectorFilteredAgg =
      DSL.selectorFiltered(s, aggregator)

    def selectorFiltered(aggregator: AggregationExpression, value: String): SelectorFilteredAgg =
      DSL.selectorFiltered(s, aggregator, value)

  }

  def count: CountAgg = new CountAgg()

  implicit class SymbolPostAgg(val s: Symbol) extends AnyVal {

    @inline
    private def arithmeticAgg(value: Double, fn: String): ArithmeticPostAgg =
      ArithmeticPostAgg(leftField = new FieldAccessPostAgg(s.name),
                        rightField = new ConstantPostAgg(value),
                        fn = fn)
    @inline
    private def arithmeticFieldAgg(right: Dim, fn: String): ArithmeticPostAgg =
      ArithmeticPostAgg(leftField = new FieldAccessPostAgg(s.name),
                        rightField = new FieldAccessPostAgg(right.name),
                        fn = fn)

    def +(v: Double): ArithmeticPostAgg        = arithmeticAgg(v, "+")
    def -(v: Double): ArithmeticPostAgg        = arithmeticAgg(v, "-")
    def *(v: Double): ArithmeticPostAgg        = arithmeticAgg(v, "*")
    def /(v: Double): ArithmeticPostAgg        = arithmeticAgg(v, "/")
    def quotient(v: Double): ArithmeticPostAgg = arithmeticAgg(v, "quotient")

    def +(v: Symbol): ArithmeticPostAgg        = arithmeticFieldAgg(v, "+")
    def -(v: Symbol): ArithmeticPostAgg        = arithmeticFieldAgg(v, "-")
    def *(v: Symbol): ArithmeticPostAgg        = arithmeticFieldAgg(v, "*")
    def /(v: Symbol): ArithmeticPostAgg        = arithmeticFieldAgg(v, "/")
    def quotient(v: Symbol): ArithmeticPostAgg = arithmeticFieldAgg(v, "quotient")

    def hyperUniqueCardinality: HyperUniqueCardinalityPostAgg =
      HyperUniqueCardinalityPostAgg(s.name)

  }

  implicit class StringOps(val value: String) extends AnyVal {
    def ===(s: Dim): FilteringExpression = s === value
    def =!=(s: Dim): FilteringExpression = s =!= value
  }

  implicit class NumOps(val value: Double) extends AnyVal {

    @inline
    private def arithmeticAgg(s: Dim, fn: String): PostAggregationExpression =
      ArithmeticPostAgg(
        new ConstantPostAgg(value),
        new FieldAccessPostAgg(s.name),
        fn = fn
      )

    def ===(s: Dim): FilteringExpression = s === value
    def =!=(s: Dim): FilteringExpression = s =!= value
    def >(s: Dim): FilteringExpression   = s < value
    def >=(s: Dim): FilteringExpression  = s =< value
    def <(s: Dim): FilteringExpression   = s > value
    def =<(s: Dim): FilteringExpression  = s >= value

    def +(s: Symbol): PostAggregationExpression        = arithmeticAgg(s, "+")
    def -(s: Symbol): PostAggregationExpression        = arithmeticAgg(s, "-")
    def *(s: Symbol): PostAggregationExpression        = arithmeticAgg(s, "*")
    def /(s: Symbol): PostAggregationExpression        = arithmeticAgg(s, "/")
    def quotient(s: Symbol): PostAggregationExpression = arithmeticAgg(s, "quotient")

  }
}
