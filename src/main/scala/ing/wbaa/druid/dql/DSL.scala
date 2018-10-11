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
import ing.wbaa.druid.dql.expressions._

import scala.language.implicitConversions

object DSL {

  def DQL: QueryBuilder = new QueryBuilder

  implicit def symbolToDim(s: Symbol): Dim = new Dim(s.name)

  implicit class StringToDim(val sc: StringContext) extends AnyVal {
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

  def extract(dim: Dim, fn: ExtractionFn): Dim        = dim.extract(fn)
  def extract(dimName: String, fn: ExtractionFn): Dim = Dim(dimName, extractionFnOpt = Option(fn))
  def extract(dim: Symbol, fn: ExtractionFn): Dim     = Dim(dim.name, extractionFnOpt = Option(fn))

  def longSum(dimName: String): LongSumAgg = new LongSumAgg(dimName)
  def longSum(dim: Symbol): LongSumAgg     = longSum(dim.name)
  def longSum(dim: Dim): LongSumAgg        = longSum(dim.name)

  def longMax(dimName: String): LongMaxAgg = new LongMaxAgg(dimName)
  def longMax(dim: Symbol): LongMaxAgg     = longMax(dim.name)
  def longMax(dim: Dim): LongMaxAgg        = longMax(dim.name)

  def longFirst(dimName: String): LongFirstAgg = new LongFirstAgg(dimName)
  def longFirst(dim: Symbol): LongFirstAgg     = longFirst(dim.name)
  def longFirst(dim: Dim): LongFirstAgg        = longFirst(dim.name)

  def longLast(dimName: String): LongLastAgg = new LongLastAgg(dimName)
  def longLast(dim: Symbol): LongLastAgg     = longLast(dim.name)
  def longLast(dim: Dim): LongLastAgg        = longLast(dim.name)

  def doubleSum(dimName: String): DoubleSumAgg = new DoubleSumAgg(dimName)
  def doubleSum(dim: Symbol): DoubleSumAgg     = doubleSum(dim.name)
  def doubleSum(dim: Dim): DoubleSumAgg        = doubleSum(dim.name)

  def doubleMax(dimName: String): DoubleMaxAgg = new DoubleMaxAgg(dimName)
  def doubleMax(dim: Symbol): DoubleMaxAgg     = doubleMax(dim.name)
  def doubleMax(dim: Dim): DoubleMaxAgg        = doubleMax(dim.name)

  def doubleFirst(dimName: String): DoubleFirstAgg = new DoubleFirstAgg(dimName)
  def doubleFirst(dim: Symbol): DoubleFirstAgg     = doubleFirst(dim.name)
  def doubleFirst(dim: Dim): DoubleFirstAgg        = doubleFirst(dim.name)

  def doubleLast(dimName: String): DoubleLastAgg = new DoubleLastAgg(dimName)
  def doubleLast(dim: Symbol): DoubleLastAgg     = doubleLast(dim.name)
  def doubleLast(dim: Dim): DoubleLastAgg        = doubleLast(dim.name)

  def thetaSketch(dimName: String): ThetaSketchAgg = ThetaSketchAgg(dimName)
  def thetaSketch(dim: Symbol): ThetaSketchAgg     = thetaSketch(dim.name)
  def thetaSketch(dim: Dim): ThetaSketchAgg        = thetaSketch(dim.name)

  def hyperUnique(dimName: String): HyperUniqueAgg = HyperUniqueAgg(dimName)
  def hyperUnique(dim: Symbol): HyperUniqueAgg     = hyperUnique(dim.name)
  def hyperUnique(dim: Dim): HyperUniqueAgg        = hyperUnique(dim.name)

  def inFiltered(dimName: String,
                 aggregator: AggregationExpression,
                 values: String*): InFilteredAgg =
    InFilteredAgg(dimName, values, aggregator.build())

  def inFiltered(dim: Symbol, aggregator: AggregationExpression, values: String*): InFilteredAgg =
    inFiltered(dim.name, aggregator, values: _*)

  def inFiltered(dim: Dim, aggregator: AggregationExpression, values: String*): InFilteredAgg =
    inFiltered(dim.name, aggregator, values: _*)

  def selectorFiltered(dimName: String,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    SelectorFilteredAgg(dimName, Option(value), aggregator.build())

  def selectorFiltered(dim: Symbol,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    selectorFiltered(dim.name, aggregator, value)

  def selectorFiltered(dim: Dim,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    selectorFiltered(dim.name, aggregator, value)

  def selectorFiltered(dimName: String, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dimName, None, aggregator.build())

  def selectorFiltered(dim: Symbol, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dim.name, None, aggregator.build())

  def selectorFiltered(dim: Dim, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dim.name, None, aggregator.build())

  def count: CountAgg = new CountAgg()

  // Post-aggs

  def hyperUniqueCardinality(fieldName: String): PostAggregationExpression =
    HyperUniqueCardinalityPostAgg(fieldName)

  def hyperUniqueCardinality(dim: Dim): PostAggregationExpression =
    HyperUniqueCardinalityPostAgg(dim.name, dim.outputNameOpt)

  implicit class StringOps(val value: String) extends AnyVal {
    def ===(s: Dim): FilteringExpression = s === value
    def =!=(s: Dim): FilteringExpression = s =!= value
  }

  implicit class NumOps(val value: Double) extends AnyVal {

    @inline
    private def arithmeticPostAgg(s: Dim, fn: String): PostAggregationExpression =
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

    def +(s: Symbol): PostAggregationExpression        = arithmeticPostAgg(s, "+")
    def -(s: Symbol): PostAggregationExpression        = arithmeticPostAgg(s, "-")
    def *(s: Symbol): PostAggregationExpression        = arithmeticPostAgg(s, "*")
    def /(s: Symbol): PostAggregationExpression        = arithmeticPostAgg(s, "/")
    def quotient(s: Symbol): PostAggregationExpression = arithmeticPostAgg(s, "quotient")

  }
}
