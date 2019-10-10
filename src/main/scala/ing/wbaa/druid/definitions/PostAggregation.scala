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

package ing.wbaa.druid
package definitions

import ca.mrvisser.sealerate
import ing.wbaa.druid.definitions.ArithmeticFunction.{ DIV, MINUS, MULT, PLUS, QUOT }
import ing.wbaa.druid.definitions.Ordering.FloatingPoint
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait PostAggregationType extends Enum with CamelCaseEnumStringEncoder
object PostAggregationType extends EnumCodec[PostAggregationType] {
  case object Arithmetic                        extends PostAggregationType
  case object FieldAccess                       extends PostAggregationType
  case object FinalizingFieldAccess             extends PostAggregationType
  case object Constant                          extends PostAggregationType
  case object DoubleGreatest                    extends PostAggregationType
  case object LongGreatest                      extends PostAggregationType
  case object DoubleLeast                       extends PostAggregationType
  case object LongLeast                         extends PostAggregationType
  case object Javascript                        extends PostAggregationType
  case object HyperUniqueCardinality            extends PostAggregationType
  case object ThetaSketchEstimate               extends PostAggregationType
  case object ThetaSketchSetOp                  extends PostAggregationType
  case object ThetaSketchToString               extends PostAggregationType
  case object QuantilesDoublesSketchToQuantile  extends PostAggregationType
  case object QuantilesDoublesSketchToQuantiles extends PostAggregationType
  case object QuantilesDoublesSketchToHistogram extends PostAggregationType
  case object QuantilesDoublesSketchToRank      extends PostAggregationType
  case object QuantilesDoublesSketchToCDF       extends PostAggregationType
  case object QuantilesDoublesSketchToString    extends PostAggregationType

  override val values: Set[PostAggregationType] = sealerate.values[PostAggregationType]
}

sealed trait PostAggregation {
  val `type`: PostAggregationType
}

sealed trait Ordering
object Ordering {
  case object FloatingPoint extends Ordering
  case object NumericFirst  extends Ordering

  implicit val encoder: Encoder[Ordering] = new Encoder[Ordering] {
    override def apply(o: Ordering) = o match {
      case FloatingPoint => io.circe.Json.Null
      case NumericFirst  => "numericFirst".asJson
    }
  }
}

sealed trait ArithmeticFunction {
  val value: String
}

object ArithmeticFunction {
  case object PLUS extends ArithmeticFunction {
    override val value = "+"
  }
  case object MINUS extends ArithmeticFunction {
    override val value = "-"
  }
  case object MULT extends ArithmeticFunction {
    override val value = "*"
  }
  case object DIV extends ArithmeticFunction {
    override val value = "/"
  }
  case object QUOT extends ArithmeticFunction {
    override val value = "quotient"
  }

  implicit val encoder: Encoder[ArithmeticFunction] = new Encoder[ArithmeticFunction] {
    override def apply(a: ArithmeticFunction) = a.value.asJson
  }
}

sealed trait ThetaSketchField { self: PostAggregation =>
}

case class ArithmeticPostAggregation(
    name: String,
    fn: ArithmeticFunction,
    fields: Seq[PostAggregation],
    ordering: Option[Ordering] = Some(FloatingPoint)
) extends PostAggregation {
  override val `type` = PostAggregationType.Arithmetic

  def withName(name: String): ArithmeticPostAggregation = copy(name = name)

  def withOrdering(ordering: Ordering): ArithmeticPostAggregation = copy(ordering = Some(ordering))
}

case class FieldAccessPostAggregation(
    fieldName: String,
    name: Option[String] = None
) extends PostAggregation
    with ThetaSketchField {
  override val `type` = PostAggregationType.FieldAccess
}

case class FinalizingFieldAccessPostAggregation(
    fieldName: String,
    name: Option[String] = None
) extends PostAggregation {
  override val `type` = PostAggregationType.FinalizingFieldAccess
}

case class ConstantPostAggregation(
    value: Double,
    name: Option[String] = None
) extends PostAggregation {
  override val `type` = PostAggregationType.Constant
}

case class DoubleGreatestPostAggregation(
    name: String,
    fields: Seq[PostAggregation]
) extends PostAggregation {
  override val `type` = PostAggregationType.DoubleGreatest
}

case class LongGreatestPostAggregation(
    name: String,
    fields: Seq[PostAggregation]
) extends PostAggregation {
  override val `type` = PostAggregationType.LongGreatest
}

case class DoubleLeastPostAggregation(
    name: String,
    fields: Seq[PostAggregation]
) extends PostAggregation {
  override val `type` = PostAggregationType.DoubleLeast
}

case class LongLeastPostAggregation(
    name: String,
    fields: Seq[PostAggregation]
) extends PostAggregation {
  override val `type` = PostAggregationType.LongLeast
}

case class JavascriptPostAggregation(
    name: String,
    fieldNames: Seq[String],
    function: String
) extends PostAggregation {
  override val `type` = PostAggregationType.Javascript
}

case class HyperUniqueCardinalityPostAggregation(name: String, fieldName: String)
    extends PostAggregation {
  override val `type` = PostAggregationType.HyperUniqueCardinality
}

case class ThetaSketchEstimatePostAggregation(name: String, field: ThetaSketchField)
    extends PostAggregation {
  override val `type` = PostAggregationType.ThetaSketchEstimate
}

sealed trait ThetaSketchOperationType extends Enum with UpperCaseEnumStringEncoder
object ThetaSketchOperationType extends EnumCodec[ThetaSketchOperationType] {

  case object Union     extends ThetaSketchOperationType
  case object Intersect extends ThetaSketchOperationType
  case object Not       extends ThetaSketchOperationType

  override val values: Set[ThetaSketchOperationType] = sealerate.values[ThetaSketchOperationType]
}

case class ThetaSketchSetOpPostAggregation(name: String,
                                           func: ThetaSketchOperationType,
                                           fields: Iterable[ThetaSketchField],
                                           size: Int = 16384)
    extends PostAggregation
    with ThetaSketchField {
  override val `type` = PostAggregationType.ThetaSketchSetOp
}

case class ThetaSketchSummaryPostAggregation(name: String, field: ThetaSketchField)
    extends PostAggregation {
  override val `type` = PostAggregationType.ThetaSketchToString
}

case class QuantilePostAggregation(name: String, field: PostAggregation, fraction: Double)
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToQuantile
}

case class QuantilesPostAggregation(name: String,
                                    field: PostAggregation,
                                    fractions: Iterable[Double])
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToQuantiles
}

case class HistogramPostAggregation(name: String,
                                    field: PostAggregation,
                                    splitPoints: Iterable[Double])
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToHistogram
}

case class QuantilesDoublesSketchToRankPostAggregation(name: String,
                                                       field: PostAggregation,
                                                       value: Double)
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToRank
}

case class QuantilesDoublesSketchToCDFPostAggregation(name: String,
                                                      field: PostAggregation,
                                                      splitPoints: Iterable[Double])
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToCDF
}

case class QuantilesSummaryPostAggregation(name: String, field: PostAggregation)
    extends PostAggregation {
  override val `type` = PostAggregationType.QuantilesDoublesSketchToString
}

object PostAggregation {
  implicit val encoder: Encoder[PostAggregation] = new Encoder[PostAggregation] {
    override def apply(pa: PostAggregation) =
      (pa match {
        case x: ArithmeticPostAggregation                   => x.asJsonObject
        case x: FieldAccessPostAggregation                  => x.asJsonObject
        case x: FinalizingFieldAccessPostAggregation        => x.asJsonObject
        case x: ConstantPostAggregation                     => x.asJsonObject
        case x: DoubleGreatestPostAggregation               => x.asJsonObject
        case x: LongGreatestPostAggregation                 => x.asJsonObject
        case x: DoubleLeastPostAggregation                  => x.asJsonObject
        case x: LongLeastPostAggregation                    => x.asJsonObject
        case x: JavascriptPostAggregation                   => x.asJsonObject
        case x: HyperUniqueCardinalityPostAggregation       => x.asJsonObject
        case x: ThetaSketchEstimatePostAggregation          => x.asJsonObject
        case x: ThetaSketchSetOpPostAggregation             => x.asJsonObject
        case x: ThetaSketchSummaryPostAggregation           => x.asJsonObject
        case x: QuantilePostAggregation                     => x.asJsonObject
        case x: QuantilesPostAggregation                    => x.asJsonObject
        case x: HistogramPostAggregation                    => x.asJsonObject
        case x: HistogramPostAggregation                    => x.asJsonObject
        case x: QuantilesDoublesSketchToRankPostAggregation => x.asJsonObject
        case x: QuantilesDoublesSketchToCDFPostAggregation  => x.asJsonObject
      }).add("type", pa.`type`.asJson).asJson
  }
}

object ArithmeticFunctions {
  def -(l: PostAggregation, r: PostAggregation): ArithmeticPostAggregation =
    ArithmeticPostAggregation("", MINUS, Seq(l, r))

  def +(l: PostAggregation, r: PostAggregation): ArithmeticPostAggregation =
    ArithmeticPostAggregation("", PLUS, Seq(l, r))

  def *(l: PostAggregation, r: PostAggregation): ArithmeticPostAggregation =
    ArithmeticPostAggregation("", MULT, Seq(l, r))

  def /(l: PostAggregation, r: PostAggregation): ArithmeticPostAggregation =
    ArithmeticPostAggregation("", DIV, Seq(l, r))

  def quot(l: PostAggregation, r: PostAggregation): ArithmeticPostAggregation =
    ArithmeticPostAggregation("", QUOT, Seq(l, r))

  implicit class ArithmeticFunctionsOps(pa: PostAggregation) {
    def -(other: PostAggregation): ArithmeticPostAggregation = ArithmeticFunctions.-(pa, other)

    def +(other: PostAggregation): ArithmeticPostAggregation = ArithmeticFunctions.+(pa, other)

    def *(other: PostAggregation): ArithmeticPostAggregation = ArithmeticFunctions.*(pa, other)

    def /(other: PostAggregation): ArithmeticPostAggregation = ArithmeticFunctions./(pa, other)

    def quot(other: PostAggregation): ArithmeticPostAggregation =
      ArithmeticFunctions.quot(pa, other)
  }
}
