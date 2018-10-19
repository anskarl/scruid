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

import ca.mrvisser.sealerate
import ing.wbaa.druid.definitions._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe._

import scala.concurrent.Future

sealed trait QueryType extends Enum with CamelCaseEnumStringEncoder
object QueryType extends EnumCodec[QueryType] {
  case object TopN       extends QueryType
  case object GroupBy    extends QueryType
  case object Timeseries extends QueryType
  val values: Set[QueryType] = sealerate.values[QueryType]
}

sealed trait DruidQuery {
  val queryType: QueryType
  val dataSource: String
  val granularity: Granularity
  val aggregations: List[Aggregation]
  val filter: Option[Filter]
  val intervals: List[String]

  def execute(): Future[DruidResponse] = DruidClient.doQuery(this)
}

object DruidQuery {
  implicit val encoder: Encoder[DruidQuery] = new Encoder[DruidQuery] {
    final def apply(query: DruidQuery): Json =
      (query match {
        case x: GroupByQuery    => x.asJsonObject
        case x: TimeSeriesQuery => x.asJsonObject
        case x: TopNQuery       => x.asJsonObject
      }).add("queryType", query.queryType.asJson).asJson
  }
}

case class GroupByQuery(
    aggregations: List[Aggregation],
    intervals: List[String],
    filter: Option[Filter] = None,
    dimensions: List[Dimension] = List(),
    granularity: Granularity = GranularityType.All,
    dataSource: String = DruidConfig.datasource,
    having: Option[Having] = None,
    limitSpec: Option[LimitSpec] = None,
    postAggregations: List[PostAggregation] = List()
) extends DruidQuery {

  val queryType = QueryType.GroupBy
}

case class LimitSpec(limit: Int, columns: Seq[OrderByColumnSpec]) {
  val `type` = "default"
}

object LimitSpec {
  implicit val encoder: Encoder[LimitSpec] = new Encoder[LimitSpec] {
    override def apply(a: LimitSpec): Json = a.asJsonObject.add("type", a.`type`.asJson).asJson
  }
}

case class OrderByColumnSpec(
    dimension: String,
    direction: Direction = Direction.ascending,
    dimensionOrder: DimensionOrder = DimensionOrder()
)
case class DimensionOrder(`type`: DimensionOrderType = DimensionOrderType.lexicographic)

sealed trait Direction extends Enum with LowerCaseEnumStringEncoder
object Direction extends EnumCodec[Direction] {
  case object ascending  extends Direction
  case object descending extends Direction
  val values: Set[Direction] = sealerate.values[Direction]
}

sealed trait DimensionOrderType extends Enum with LowerCaseEnumStringEncoder
object DimensionOrderType extends EnumCodec[DimensionOrderType] {
  case object lexicographic extends DimensionOrderType
  case object alphanumeric  extends DimensionOrderType
  case object strlen        extends DimensionOrderType
  case object numeric       extends DimensionOrderType
  val values: Set[DimensionOrderType] = sealerate.values[DimensionOrderType]

}

case class TimeSeriesQuery(
    aggregations: List[Aggregation],
    intervals: List[String],
    filter: Option[Filter] = None,
    granularity: Granularity = GranularityType.Week,
    descending: String = "true",
    dataSource: String = DruidConfig.datasource,
    postAggregations: List[PostAggregation] = List()
) extends DruidQuery {
  val queryType = QueryType.Timeseries
}

case class TopNQuery(
    dimension: Dimension,
    threshold: Int,
    metric: String,
    aggregations: List[Aggregation],
    intervals: List[String],
    granularity: Granularity = GranularityType.All,
    filter: Option[Filter] = None,
    dataSource: String = DruidConfig.datasource,
    postAggregations: List[PostAggregation] = Nil
) extends DruidQuery {
  val queryType = QueryType.TopN
}
