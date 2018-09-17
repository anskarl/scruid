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

sealed trait AggregationExpression {
  protected[dql] def build(): Aggregation

  def alias(name: String): AggregationExpression
  def as(name: String): AggregationExpression = alias(name)
}

final class CountAgg(name: Option[String] = None) extends AggregationExpression {

  override protected[dql] def build(): Aggregation = CountAggregation(name.getOrElse("count"))

  override def alias(name: String): AggregationExpression = new CountAgg(Option(name))
}

final class LongSumAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongSumAggregation(name.getOrElse(s"long_sum_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression = new LongSumAgg(fieldName, Option(name))
}

final class LongMaxAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongMaxAggregation(name.getOrElse(s"long_max_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression = new LongMaxAgg(fieldName, Option(name))
}

final class LongMinAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongMinAggregation(name.getOrElse(s"long_min_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression = new LongMinAgg(fieldName, Option(name))
}

final class LongFirstAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongFirstAggregation(name.getOrElse(s"long_first_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new LongFirstAgg(fieldName, Option(name))
}

final class LongLastAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongLastAggregation(name.getOrElse(s"long_last_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression = new LongLastAgg(fieldName, Option(name))
}

final class DoubleSumAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleSumAggregation(name.getOrElse(s"double_sum_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleSumAgg(fieldName, Option(name))
}

final class DoubleMaxAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleMaxAggregation(name.getOrElse(s"double_max_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleMaxAgg(fieldName, Option(name))
}

final class DoubleMinAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleMinAggregation(name.getOrElse(s"double_min_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleMinAgg(fieldName, Option(name))
}

final class DoubleFirstAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleFirstAggregation(name.getOrElse(s"double_first_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleFirstAgg(fieldName, Option(name))
}

final class DoubleLastAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleLastAggregation(name.getOrElse(s"double_last_$fieldName"), fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleLastAgg(fieldName, Option(name))
}

final case class ThetaSketchAgg(fieldName: String,
                                name: Option[String] = None,
                                isInputThetaSketch: Boolean = false,
                                size: Long = 16384)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    ThetaSketchAggregation(
      name.getOrElse(s"theta_sketch_$fieldName"),
      fieldName,
      isInputThetaSketch,
      size
    )

  override def alias(name: String): ThetaSketchAgg = copy(name = Option(name))

  def isInputThetaSketch(v: Boolean): ThetaSketchAgg = copy(isInputThetaSketch = v)

  def withSize(size: Long): ThetaSketchAgg = copy(size = size)
}

final case class HyperUniqueAgg(fieldName: String,
                                name: Option[String] = None,
                                isInputHyperUnique: Boolean = false,
                                round: Boolean = false)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    HyperUniqueAggregation(
      name.getOrElse(s"hyper_unique_$fieldName"),
      fieldName,
      isInputHyperUnique,
      round
    )
  override def alias(name: String): HyperUniqueAgg = copy(name = Option(name))

  def isInputHyperUnique(v: Boolean): HyperUniqueAgg = copy(isInputHyperUnique = v)

  def setRound(v: Boolean): HyperUniqueAgg = copy(round = v)

}

final case class InFilteredAgg(dimension: String,
                               values: Seq[String],
                               aggregator: Aggregation,
                               name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    InFilteredAggregation(
      name.getOrElse(s"in_filtered_$dimension"),
      InFilter(dimension, values),
      aggregator
    )

  override def alias(name: String): InFilteredAgg = copy(name = Option(name))
}

final case class SelectorFilteredAgg(dimension: String,
                                     value: Option[String] = None,
                                     aggregator: Aggregation,
                                     name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    SelectorFilteredAggregation(
      name.getOrElse(s"selector_filtered_$dimension"),
      SelectFilter(dimension, value),
      aggregator
    )

  override def alias(name: String): SelectorFilteredAgg = copy(name = Option(name))
}
