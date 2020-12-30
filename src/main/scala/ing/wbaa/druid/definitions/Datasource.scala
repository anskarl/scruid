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
import ing.wbaa.druid.dql.expressions.Expression
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import scala.reflect.ClassTag

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
        case d: Table  => d.asJsonObject
        case d: Lookup => d.asJsonObject
        case d: Union  => d.asJsonObject
        case d: Inline => d.asJsonObject
        case d: Query  => d.asJsonObject
        case d: Join   => d.asJsonObject
      }).add("type", datasource.`type`.asJson).asJson
  }
}

sealed trait RightHandDatasource extends Datasource

object RightHandDatasource {
  implicit val encoder: Encoder[RightHandDatasource] = new Encoder[RightHandDatasource] {
    override def apply(datasource: RightHandDatasource): Json =
      (datasource match {
        case d: Lookup => d.asJsonObject
        case d: Inline => d.asJsonObject
        case d: Query  => d.asJsonObject
      }).add("type", datasource.`type`.asJson).asJson
  }
}

case class Table(name: String) extends Datasource {
  override val `type`: DatasourceType = DatasourceType.Table

  def union(dataSources: Iterable[String]): Union =
    Union(dataSources.foldLeft(List(this.name))((acc, tblName) => tblName :: acc))

  def union[_ <: Table: ClassTag](dataSources: Iterable[Table]): Union =
    Union(dataSources.foldLeft(List(this.name))((acc, tbl) => tbl.name :: acc))
}

case class Lookup(lookup: String) extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Lookup
}

case class Union(dataSources: Iterable[String]) extends Datasource {
  override val `type`: DatasourceType = DatasourceType.Union
}

sealed trait InlineEntry
case class SingleValEntry(value: String)           extends InlineEntry
case class MultiValEntry(values: Iterable[String]) extends InlineEntry

object InlineEntry {
  implicit val encoder: Encoder[InlineEntry] = new Encoder[InlineEntry] {
    override def apply(entry: InlineEntry): Json =
      entry match {
        case SingleValEntry(value) => Json.fromString(value)
        case MultiValEntry(values) => Json.fromValues(values.map(Json.fromString))
      }
  }
}

case class Inline(columnNames: Iterable[String], rows: Iterable[Iterable[InlineEntry]])
    extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Inline
}

object Inline {
  def apply[_ <: String: ClassTag](columnNames: Iterable[String],
                                   rows: Iterable[Iterable[String]]): Inline =
    new Inline(columnNames, rows.map(_.map(SingleValEntry)))
}

case class Query(query: DruidNativeQuery) extends RightHandDatasource {
  override val `type`: DatasourceType = DatasourceType.Query
}

sealed trait JoinType extends Enum with UpperCaseEnumStringEncoder
object JoinType extends EnumCodec[JoinType] {

  case object Inner extends JoinType
  case object Left  extends JoinType

  val values: Set[JoinType] = sealerate.values[JoinType]
}

case class Join(
    left: Datasource,
    right: RightHandDatasource,
    rightPrefix: String,
    condition: String,
    joinType: JoinType
) extends Datasource {

  def this(left: String,
           right: RightHandDatasource,
           rightPrefix: String,
           condition: String,
           joinType: JoinType) =
    this(Table(left), right, rightPrefix, condition, joinType)

  def this(left: Datasource,
           right: RightHandDatasource,
           rightPrefix: String,
           condition: Expression,
           joinType: JoinType) = this(left, right, rightPrefix, condition.build(), joinType)

  def this(left: String,
           right: RightHandDatasource,
           rightPrefix: String,
           condition: Expression,
           joinType: JoinType) =
    this(Table(left), right, rightPrefix, condition.build(), joinType)

  require(rightPrefix.trim.nonEmpty, "`rightPrefix` should not be empty")
  require(rightPrefix.trim != "__time", "`rightPrefix` should not match to '__time'")

  override val `type`: DatasourceType = DatasourceType.Join
}
