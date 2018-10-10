/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreement  See the NOTICE file distributed with
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

package ing.wbaa.druid.dql.expressions

import ing.wbaa.druid.DimensionOrderType
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL.not

case class Dim(name: String,
               outputNameOpt: Option[String] = None,
               outputTypeOpt: Option[String] = None,
               extractionFnOpt: Option[ExtractionFn] = None)
    extends Named[Dim] {

  override def alias(name: String): Dim = copy(outputNameOpt = Option(name))

  override def getName: String = name

  def cast(outputType: String): Dim = {
    val uppercase = outputType.toUpperCase
    require(Dim.ValidTypes.contains(uppercase))
    copy(outputTypeOpt = Option(uppercase))
  }

  def toFloat: Dim = cast("FLOAT")
  def toLong: Dim  = cast("LONG")

  def extract(fn: ExtractionFn): Dim =
    copy(extractionFnOpt = Option(fn))

  protected[dql] def build(): Dimension =
    extractionFnOpt match {
      case Some(extractionFn) =>
        ExtractionDimension(name, outputNameOpt.orElse(Option(name)), outputTypeOpt, extractionFn)

      case None =>
        DefaultDimension(name, outputNameOpt.orElse(Option(name)), outputTypeOpt)
    }

  @inline
  private def eqVal(value: String): FilteringExpression = new EqString(this, value)

  @inline
  private def eqNum(value: Double): FilteringExpression = new EqDouble(this, value)

  @inline
  private def compareWith(other: Dim): FilteringExpression =
    new ColumnComparison(this :: other :: Nil)

  def ===(other: Dim): FilteringExpression = compareWith(other)

  def ===(value: String): FilteringExpression = eqVal(value)

  def ===(value: Double): FilteringExpression = eqNum(value)

  def =!=(value: String): FilteringExpression = not(eqVal(value))

  def =!=(value: Double): FilteringExpression = not(eqNum(value))

  def =!=(other: Dim): FilteringExpression = not(compareWith(other))

  def in(values: String*): FilteringExpression = new In(this, values.toList)

  def notIn(values: String*): FilteringExpression = not(new In(this, values.toList))

  def like(pattern: String): FilteringExpression = new Like(this, pattern)

  def regex(pattern: String): FilteringExpression = new Regex(this, pattern)

  def isNull: FilteringExpression = new NullDim(this)

  def isNotNull: FilteringExpression = not(this.isNull)

  def >(value: Double): FilteringExpression = new Gt(this, value)

  def >=(value: Double): FilteringExpression = new GtEq(this, value)

  def <(value: Double): FilteringExpression = new Lt(this, value)

  def =<(value: Double): FilteringExpression = new LtEq(this, value)

  def between(lower: Double, upper: Double, lowerStrict: Boolean, upperStrict: Boolean): Bound =
    Bound(
      dimension = name,
      lower = Option(lower.toString),
      lowerStrict = Option(lowerStrict),
      upper = Option(upper.toString),
      upperStrict = Option(upperStrict),
      ordering = Option(DimensionOrderType.numeric)
    )

  def between(lower: Double, upper: Double): Bound =
    between(lower, upper, lowerStrict = false, upperStrict = false)

  def >(value: String): Bound =
    Bound(dimension = name,
          lower = Option(value),
          upper = None,
          ordering = Option(DimensionOrderType.lexicographic))

  def >=(value: String): Bound =
    Bound(dimension = name,
          lower = Option(value),
          lowerStrict = Some(true),
          upper = None,
          ordering = Option(DimensionOrderType.lexicographic))

  def <(value: String): Bound =
    Bound(dimension = name,
          lower = None,
          upper = Option(value),
          ordering = Option(DimensionOrderType.lexicographic))

  def =<(value: String): Bound =
    Bound(dimension = name,
          lower = None,
          upper = Option(value),
          upperStrict = Some(true),
          ordering = Option(DimensionOrderType.lexicographic))

  def between(lower: String, upper: String, lowerStrict: Boolean, upperStrict: Boolean): Bound =
    Bound(
      dimension = name,
      lower = Option(lower),
      lowerStrict = Option(lowerStrict),
      upper = Option(upper),
      upperStrict = Option(upperStrict),
      ordering = Option(DimensionOrderType.lexicographic)
    )

  def between(lower: String, upper: String): Bound =
    between(lower, upper, lowerStrict = false, upperStrict = false)

  def interval(values: String*): FilteringExpression = new Interval(this, values.toList)

  def contains(value: String, caseSensitive: Boolean = true): FilteringExpression =
    new Contains(this, value, caseSensitive)

  def containsIgnoreCase(value: String): FilteringExpression =
    new Contains(this, value, caseSensitive = false)

  def containsInsensitive(value: String): FilteringExpression =
    new InsensitiveContains(this, value)
}

object Dim {
  private final val ValidTypes = Set("STRING", "LONG", "FLOAT")
}
