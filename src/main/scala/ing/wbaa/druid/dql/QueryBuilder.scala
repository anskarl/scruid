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

import ing.wbaa.druid.{ Direction, _ }
import ing.wbaa.druid.definitions._

private[dql] sealed trait QueryBuilderCommons {

  protected var dataSourceOpt                   = Option.empty[String]
  protected var granularityOpt                  = Option.empty[Granularity]
  protected var aggregations: List[Aggregation] = Nil
  protected var intervals: List[String]         = Nil

  protected var filters: List[Filter] = Nil

  def from(dataSource: String): this.type = {
    dataSourceOpt = Option(dataSource)
    this
  }

  def withGranularity(granularity: Granularity): this.type = {
    granularityOpt = Option(granularity)
    this
  }

  def agg(aggs: AggregationExpression*): this.type = {
    aggregations =
      aggs.foldLeft(aggregations)((aggregations, currentAgg) => currentAgg.build() :: aggregations)
    this
  }

  def interval(interval: String): this.type = {
    intervals = interval :: intervals
    this
  }

  def intervals(ints: String*): this.type = {
    intervals = ints.foldLeft(intervals)((intervals, currentInt) => currentInt :: intervals)
    this
  }

  def where(filter: Expression): this.type = {

    filters = filter.asFilter :: filters
    this
  }

  protected def getFilters: Option[Filter] =
    if (filters.isEmpty) None
    else if (filters.size == 1) Option(filters.head)
    else Option(AndFilter(filters))

  protected def copyTo[T <: QueryBuilderCommons](other: T): T = {
    other.dataSourceOpt = dataSourceOpt
    other.granularityOpt = granularityOpt
    other.aggregations = aggregations
    other.intervals = intervals
    other.filters = filters
    other
  }

}

final class QueryBuilder private[dql] () extends QueryBuilderCommons {

  private var descending = true

  def setDescending(v: Boolean): this.type = {
    descending = v
    this
  }

  def build(): TimeSeriesQuery =
    TimeSeriesQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = this.getFilters,
      granularity = this.granularityOpt.getOrElse(GranularityType.Week),
      descending = this.descending.toString,
      dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource)
    )

  def topN(dimension: Dim, metric: String, threshold: Int): TopNQueryBuilder =
    copyTo(new TopNQueryBuilder(dimension, metric, threshold))

  def groupBy(dimensions: Dim*): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions))

}

final class TopNQueryBuilder private[dql] (dimension: Dim, metric: String, n: Int)
    extends QueryBuilderCommons {
  protected var isDescending                            = true
  protected var postAggregations: List[PostAggregation] = Nil

  def setDescending(v: Boolean): this.type = {
    isDescending = v
    this
  }

  def postAgg(aggs: PostAggregation*): this.type = {
    postAggregations =
      aggs.foldLeft(postAggregations)((postAggs, currentPostAgg) => currentPostAgg :: postAggs)
    this
  }

  def build(): TopNQuery =
    TopNQuery(
      dimension = this.dimension.build(),
      threshold = n,
      metric = metric,
      aggregations = this.aggregations,
      intervals = this.intervals,
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      filter = this.getFilters,
      dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
      postAggregations = Nil
    )

}

final class GroupByQueryBuilder private[dql] (dimensions: Seq[Dim]) extends QueryBuilderCommons {

  protected var limitOpt                        = Option.empty[Int]
  protected var limitCols                       = Seq.empty[OrderByColumnSpec]
  protected var havingExpressions: List[Having] = Nil

  protected var excludeNullsOpt = Option.empty[Boolean]

  def having(v: Expression): this.type = {
    havingExpressions = v.asHaving :: havingExpressions
    this
  }

  def limit(n: Int,
            direction: Direction,
            dimensionOrder: DimensionOrder = DimensionOrder.lexicographic): this.type = {
    limitOpt = Option(n)
    limitCols = dimensions.map(dim => OrderByColumnSpec(dim.name, direction, dimensionOrder))
    this
  }

  def limit(n: Int, cols: OrderByColumnSpec*): this.type = {
    limitOpt = Option(n)
    limitCols = cols
    this
  }

  def setExcludeNulls(v: Boolean): this.type = {
    excludeNullsOpt = Option(v)
    this
  }

  def build(): GroupByQuery = {

    val havingOpt =
      if (havingExpressions.isEmpty) None
      else if (havingExpressions.size == 1) Option(havingExpressions.head)
      else Option(definitions.AndHaving(havingExpressions))

    val limitSpecOpt =
      limitOpt.map { n =>
        if (limitCols.nonEmpty) LimitSpec(limit = n, columns = limitCols)
        else LimitSpec(limit = n, columns = dimensions.map(dim => OrderByColumnSpec(dim.name)))
      }

    if (excludeNullsOpt.contains(true)) {
      val excludeNullsExpressions = dimensions
        .map(dim => new Not(new NullDim(dim.name)))
        .toList

      where(new And(excludeNullsExpressions))
    }

    val resultingFilters = this.getFilters

    GroupByQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = resultingFilters,
      dimensions = this.dimensions.map(_.build()).toList,
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
      having = havingOpt,
      limitSpec = limitSpecOpt
    )
  }

}
