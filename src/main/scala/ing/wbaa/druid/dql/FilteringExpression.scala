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

import ing.wbaa.druid.{ definitions, DimensionOrder }
import ing.wbaa.druid.definitions.SearchQuerySpecType.InsensitiveContains
import ing.wbaa.druid.definitions._

sealed trait Expression {

  protected[dql] def createFilter: Filter

  protected[dql] def createHaving: Having

  def asFilter: Filter = createFilter

  def asHaving: Having = this match {
    case _: FilterOnlyOperator => FilterHaving(this.asFilter)
    case _                     => createHaving
  }

  def or(others: Expression*): Expression = new Or(this :: others.toList)

  def and(others: Expression*): Expression = new And(this :: others.toList)
}

class TimestampDim(ts: Long = 0L) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter =
    SelectFilter(dimension = "__time", value = ts.toString)

  def ===(ts: Long): Expression = new TimestampDim(ts)
}

class And(expressions: List[Expression]) extends Expression {
  override protected[dql] def createFilter: Filter = AndFilter(expressions.map(_.createFilter))
  override protected[dql] def createHaving: Having = AndHaving(expressions.map(_.createHaving))
}

class Or(expressions: List[Expression]) extends Expression {
  override protected[dql] def createFilter: Filter = OrFilter(expressions.map(_.createFilter))
  override protected[dql] def createHaving: Having = OrHaving(expressions.map(_.createHaving))
}

class Not(val op: Expression) extends Expression {
  override protected[dql] def createFilter: Filter = NotFilter(op.createFilter)
  override protected[dql] def createHaving: Having =
    op match {
      case _: FilterOnlyOperator => FilterHaving(NotFilter(op.createFilter))
      case _                     => NotHaving(op.createHaving)
    }
}

class EqString(name: String, value: String) extends Expression {
  override protected[dql] def createFilter: Filter = SelectFilter(name, Option(value))
  override protected[dql] def createHaving: Having = DimSelectorHaving(name, value)
}

class EqDouble(name: String, value: Double) extends Expression {
  override protected[dql] def createFilter: Filter = SelectFilter(name, Option(value.toString))
  override protected[dql] def createHaving: Having = EqualToHaving(name, value)
}

trait FilterOnlyOperator extends Expression {
  override protected[dql] def createHaving: Having = FilterHaving(this.createFilter)
}

class In(name: String, values: List[String]) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = InFilter(name, values)
}

class Like(name: String, pattern: String) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = LikeFilter(name, pattern)
}

class Regex(name: String, pattern: String) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = RegexFilter(name, pattern)
}

class NullDim(name: String) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = SelectFilter(name, None)
}

class Gt(name: String, value: Double) extends Expression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = name,
      lower = Option(value.toString),
      lowerStrict = Some(false),
      ordering = Option(DimensionOrder.numeric)
    )

  override protected[dql] def createHaving: Having = GreaterThanHaving(name, value)
}

class GtEq(name: String, value: Double) extends Expression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = name,
      lower = Option(value.toString),
      lowerStrict = Some(true),
      ordering = Option(DimensionOrder.numeric)
    )

  override protected[dql] def createHaving: Having =
    OrHaving(EqualToHaving(name, value) :: GreaterThanHaving(name, value) :: Nil)
}

class Lt(name: String, value: Double) extends Expression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = name,
      upper = Option(value.toString),
      upperStrict = Some(false),
      ordering = Option(DimensionOrder.numeric)
    )

  override protected[dql] def createHaving: Having = LessThanHaving(name, value)
}

class LtEq(name: String, value: Double) extends Expression {

  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = name,
      upper = Option(value.toString),
      upperStrict = Some(true),
      ordering = Option(DimensionOrder.numeric)
    )

  override protected[dql] def createHaving: Having =
    OrHaving(EqualToHaving(name, value) :: LessThanHaving(name, value) :: Nil)

}

class ColumnComparison(dimensions: List[String]) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = ColumnComparisonFilter(dimensions)
}

case class Bound(dimension: String,
                 lower: Option[String] = None,
                 upper: Option[String] = None,
                 lowerStrict: Option[Boolean] = None,
                 upperStrict: Option[Boolean] = None,
                 ordering: Option[DimensionOrder] = None,
                 extractionFn: Option[ExtractionFn] = None)
    extends Expression
    with FilterOnlyOperator {

  def set(
      lowerStrict: Boolean = false,
      upperStrict: Boolean = false,
      ordering: DimensionOrder = DimensionOrder.lexicographic
  ): Expression =
    copy(lowerStrict = Option(lowerStrict),
         upperStrict = Option(upperStrict),
         ordering = Option(ordering))

  def withOrdering(v: DimensionOrder): Bound = copy(ordering = Option(v))

  def withExtractionFn(fn: ExtractionFn): Bound = copy(extractionFn = Option(fn))

  override protected[dql] def createFilter: Filter =
    BoundFilter(dimension, lower, upper, lowerStrict, upperStrict, ordering, extractionFn)

}

class Interval(dimension: String, values: List[String]) extends Expression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = IntervalFilter(dimension, values)
}

class Contains(dimension: String, value: String, caseSensitive: Boolean)
    extends Expression
    with FilterOnlyOperator {

  override protected[dql] def createFilter: Filter =
    SearchFilter(dimension, query = ContainsCaseSensitive(value, Option(caseSensitive)))
}
class InsensitiveContains(dimension: String, value: String)
    extends Expression
    with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter =
    SearchFilter(dimension, query = definitions.ContainsInsensitive(value))
}
