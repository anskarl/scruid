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

package ing.wbaa.druid.definitions

import ca.mrvisser.sealerate
import ing.wbaa.druid._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait DatasourceType extends Enum with LowerCaseEnumStringEncoder
object DatasourceType extends EnumCodec[DatasourceType] {
  case object Table  extends DatasourceType
  case object Lookup extends DatasourceType
  case object Union  extends DatasourceType
  case object Inline extends DatasourceType
  case object Query  extends DatasourceType
  case object Join   extends DatasourceType

  val values: Set[DatasourceType] = sealerate.values[DatasourceType]

}

sealed trait Datasource {
  val `type`: DatasourceType
}

object Datasource {
  implicit val encoder: Encoder[Datasource] = new Encoder[Datasource] {
    override def apply(datasource: Datasource): Json =
      (datasource match {
        case d: DatasourceTable  => d.asJsonObject
        case d: DatasourceLookup => d.asJsonObject
        case d: DatasourceUnion  => d.asJsonObject
        case d: DatasourceInline => d.asJsonObject
        case d: DatasourceQuery  => d.asJsonObject
        case d: DatasourceJoin   => d.asJsonObject
      }).add("type", datasource.`type`.asJson).asJson
  }
}

sealed trait RightHandDatasource extends Datasource

case class DatasourceTable(name: String) extends Datasource {
  override val `type`: DatasourceType = DatasourceType.Table
}

case class DatasourceLookup(lookup: String) extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Lookup
}

case class DatasourceUnion(dataSources: Seq[String]) extends Datasource {
  override val `type`: DatasourceType = DatasourceType.Union
}

case class DatasourceInline(columnNames: Seq[String], rows: Seq[String])
    extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Inline
}

case class DatasourceQuery(query: DruidNativeQuery) extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Query
}

sealed trait JoinType extends Enum with UpperCaseEnumStringEncoder
object JoinType extends EnumCodec[JoinType] {

  case object Inner extends JoinType
  case object Left  extends JoinType

  val values: Set[JoinType] = sealerate.values[JoinType]
}

case class DatasourceJoin(
    left: Datasource,
    right: RightHandDatasource,
    rightPrefix: String,
    condition: String,
    joinType: JoinType
) extends Datasource {
  require(rightPrefix.trim.nonEmpty, "`rightPrefix` should not be empty")
  require(rightPrefix.trim != "__time", "`rightPrefix` should not match to '__time'")

  override val `type`: DatasourceType = DatasourceType.Join
}
