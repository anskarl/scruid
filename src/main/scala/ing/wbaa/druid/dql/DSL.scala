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

import ing.wbaa.druid.definitions.{ ArithmeticFunction, ThetaSketchField, ThetaSketchOperationType }
import ing.wbaa.druid.dql.expressions._

import scala.language.implicitConversions

object DSL
    extends FilteringExpressionOps
    with ExtractionFnOps
    with AggregationOps
    with PostAggregationOps {

  /**
    * @return a new instance of the QueryBuilder
    */
  def DQL: QueryBuilder = new QueryBuilder

  /**
    * Implicitly convert a Scala Symbol to an instance of Dim
    */
  implicit def symbolToDim(s: Symbol): Dim = new Dim(s.name)

  implicit def symbolsToDims[T <: Iterable[Symbol]](iterable: T): Iterable[Dim] =
    iterable.map(s => new Dim(s.name))

  implicit class StringToDim(val sc: StringContext) extends AnyVal {

    /**
      * Create a dim using simple string interpolator.
      *
      * {{{
      *   val prefix = "foo"
      *   val suffix = "bar"
      *
      *   d"${prefix}_${suffix}"
      * }}}
      *
      * @param args The arguments to be inserted into the resulting string.
      *
      * @see StringContext
      */
    def d(args: Any*): Dim = Dim(sc.s(args: _*))
  }

  /**
    * Create an instance of Dim
    * @param name the name of the dimension
    *
    * @return the resulting Dim
    */
  def dim(name: String): Dim = Dim(name)

  implicit class StringOps(val value: String) extends AnyVal {
    def ===(s: Dim): FilteringExpression = s === value
    def =!=(s: Dim): FilteringExpression = s =!= value
  }

  implicit class NumOps(val value: Double) extends AnyVal {

    @inline
    private def arithmeticPostAgg(s: Dim, fn: ArithmeticFunction): PostAggregationExpression =
      ArithmeticPostAgg(
        new ConstantPostAgg(value),
        new FieldAccessPostAgg(s.name),
        fn = fn
      )

    def ===(s: Dim): FilteringExpression = s === value
    def =!=(s: Dim): FilteringExpression = s =!= value
    def >(s: Dim): FilteringExpression   = s < value
    def >=(s: Dim): FilteringExpression  = s <= value
    def <(s: Dim): FilteringExpression   = s > value
    def <=(s: Dim): FilteringExpression  = s >= value

    def +(s: Symbol): PostAggregationExpression = arithmeticPostAgg(s, ArithmeticFunction.PLUS)
    def -(s: Symbol): PostAggregationExpression = arithmeticPostAgg(s, ArithmeticFunction.MINUS)
    def *(s: Symbol): PostAggregationExpression = arithmeticPostAgg(s, ArithmeticFunction.MULT)
    def /(s: Symbol): PostAggregationExpression = arithmeticPostAgg(s, ArithmeticFunction.DIV)
    def quotient(s: Symbol): PostAggregationExpression =
      arithmeticPostAgg(s, ArithmeticFunction.QUOT)

  }

  implicit class AnyPostAggregatorExpressionOps[T <: AnyPostAggregatorExpression[T]](
      val postAggExpr: AnyPostAggregatorExpression[T]
  ) extends AnyVal {

    def quantile(fraction: Double): PostAggregationExpression =
      PostAggregationOps.quantile(postAggExpr, fraction)

    def quantiles(fractions: Double*): PostAggregationExpression =
      PostAggregationOps.quantiles(postAggExpr, fractions)
    def quantiles(fractions: Iterable[Double]): PostAggregationExpression =
      PostAggregationOps.quantiles(postAggExpr, fractions)

    def histogram(splitPoints: Double*): PostAggregationExpression =
      PostAggregationOps.histogram(postAggExpr, splitPoints)
    def histogram(splitPoints: Iterable[Double]): PostAggregationExpression =
      PostAggregationOps.histogram(postAggExpr, splitPoints)

    def quantilesSketchRank(value: Double): PostAggregationExpression =
      PostAggregationOps.quantilesSketchRank(postAggExpr, value)

    def quantilesSketchCDF(splitPoints: Double*): PostAggregationExpression =
      PostAggregationOps.quantilesSketchCDF(postAggExpr, splitPoints)

    def quantilesSketchCDF(splitPoints: Iterable[Double]): PostAggregationExpression =
      PostAggregationOps.quantilesSketchCDF(postAggExpr, splitPoints)

    def sketchSummary: PostAggregationExpression =
      PostAggregationOps.quantilesSketchSummary(postAggExpr)
  }

  implicit class ThetaSketchFieldOps[T <: ThetaSketchField](val sketchField: T) extends AnyVal {

    def thetaSketchEstimate: ThetaSketchEstimatePostAgg =
      PostAggregationOps.thetaSketchEstimate(sketchField)

    def thetaSketchEstimate(name: String): ThetaSketchEstimatePostAgg =
      PostAggregationOps.thetaSketchEstimate(name, sketchField)

    def thetaSketchSummary: ThetaSketchSummaryPostAgg =
      ThetaSketchSummaryPostAgg(sketchField)

    def thetaSketchSummary(name: String): ThetaSketchSummaryPostAgg =
      ThetaSketchSummaryPostAgg(sketchField, Option(name))
  }

  object ThetaSketch {

    def union[T <: ThetaSketchField](fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Union, fileds)

    def intersect[T <: ThetaSketchField](fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Intersect, fileds)

    def not[T <: ThetaSketchField](fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Not, fileds)

    def union[T <: ThetaSketchField](name: String, fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Union, fileds, Option(name))

    def intersect[T <: ThetaSketchField](name: String,
                                         fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Intersect, fileds, Option(name))

    def not[T <: ThetaSketchField](name: String, fileds: Iterable[T]): ThetaSketchSetOpPostAgg =
      ThetaSketchSetOpPostAgg(ThetaSketchOperationType.Not, fileds, Option(name))
  }
}
