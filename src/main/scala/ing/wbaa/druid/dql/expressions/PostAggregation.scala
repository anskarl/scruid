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

package ing.wbaa.druid.dql.expressions

import ing.wbaa.druid.definitions._

sealed trait AnyPostAggregatorExpression[T <: AnyPostAggregatorExpression[T]] extends Named[T] {
  protected[dql] def build(complexAggNames: Set[String]): PostAggregation
}

sealed trait PostAggregatorExpression extends AnyPostAggregatorExpression[PostAggregatorExpression]

class FieldAccessPostAgg(fieldName: String, name: Option[String] = None)
    extends PostAggregatorExpression {

  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    if (complexAggNames.contains(fieldName)) FieldAccessPostAggregation(fieldName, name)
    else FieldAccessPostAggregation(fieldName, name)

  override def alias(name: String): PostAggregatorExpression =
    new FieldAccessPostAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(fieldName)
}

class ConstantPostAgg(value: Double, name: Option[String] = None) extends PostAggregatorExpression {

  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    ConstantPostAggregation(value, name)

  override def alias(name: String): PostAggregatorExpression =
    new ConstantPostAgg(value, Option(name))

  override def getName: String = name.getOrElse("c_" + value)
}

sealed trait PostAggregationExpression
    extends AnyPostAggregatorExpression[PostAggregationExpression]

case class ArithmeticPostAgg(leftField: PostAggregatorExpression,
                             rightField: PostAggregatorExpression,
                             fn: ArithmeticFunction,
                             name: Option[String] = None,
                             ordering: Option[Ordering] = None)
    extends PostAggregationExpression {

  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    ArithmeticPostAggregation(
      name = this.getName,
      fn = fn,
      fields = Seq(leftField.build(complexAggNames), rightField.build(complexAggNames))
    )

  def withOrdering(ordering: Ordering): PostAggregationExpression =
    copy(ordering = Option(ordering))

  def floatingPointOrdering: PostAggregationExpression =
    copy(ordering = Some(Ordering.FloatingPoint))

  def numericFirstOrdering: PostAggregationExpression = copy(ordering = Some(Ordering.NumericFirst))

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_${leftField}_${fn}_${rightField}")
}

case class HyperUniqueCardinalityPostAgg(fieldName: String, name: Option[String] = None)
    extends PostAggregationExpression {

  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    HyperUniqueCardinalityPostAggregation(this.getName, fieldName)

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_${fieldName}")
}

case class JavascriptPostAgg(fieldNames: Seq[String], function: String, name: Option[String] = None)
    extends PostAggregationExpression {

  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    JavascriptPostAggregation(this.getName, fieldNames, function)

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_${fieldNames.mkString("_")}")
}

case class QuantilePostAgg[T <: AnyPostAggregatorExpression[T]](
    field: AnyPostAggregatorExpression[T],
    fraction: Double,
    name: Option[String] = None
) extends PostAggregationExpression {
  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    QuantilePostAggregation(this.getName, field.build(complexAggNames), fraction)

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_quantile")
}

case class QuantilesPostAgg[T <: AnyPostAggregatorExpression[T]](
    field: AnyPostAggregatorExpression[T],
    fractions: Iterable[Double],
    name: Option[String] = None
) extends PostAggregationExpression {
  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    QuantilesPostAggregation(this.getName, field.build(complexAggNames), fractions)

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_quantiles")
}

case class HistogramPostAgg[T <: AnyPostAggregatorExpression[T]](
    field: AnyPostAggregatorExpression[T],
    splitPoints: Iterable[Double],
    name: Option[String] = None
) extends PostAggregationExpression {
  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    HistogramPostAggregation(this.getName, field.build(complexAggNames), splitPoints)

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_histogram")
}

case class SketchSummaryPostAgg[T <: AnyPostAggregatorExpression[T]](
    field: AnyPostAggregatorExpression[T],
    name: Option[String] = None
) extends PostAggregationExpression {
  override protected[dql] def build(complexAggNames: Set[String]): PostAggregation =
    SketchSummaryPostAggregation(this.getName, field.build(complexAggNames))

  override def alias(name: String): PostAggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"post_sketch_summary")
}
