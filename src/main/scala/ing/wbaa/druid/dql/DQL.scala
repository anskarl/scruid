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
import ing.wbaa.druid.{ DruidConfig, GroupByQuery, TimeSeriesQuery, TopNQuery }
import ing.wbaa.druid.definitions._

object DQL {

  def Builder: QueryBuilder = new QueryBuilder

  sealed trait QueryBuilderCommons {

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

    def agg(aggs: Aggregation*): this.type = {
      aggregations =
        aggs.foldLeft(aggregations)((aggregations, currentAgg) => currentAgg :: aggregations)
      this
    }

    def addIntervals(ints: String*): this.type = {
      intervals = ints.foldLeft(intervals)((intervals, currentInt) => currentInt :: intervals)
      this
    }

    def where(filter: Filter): this.type = {
      filters = filter :: filters
      this
    }

    protected def copyTo[T <: QueryBuilderCommons](other: T): T = {
      other.dataSourceOpt = dataSourceOpt
      other.granularityOpt = granularityOpt
      other.aggregations = aggregations
      other.intervals = intervals
      other.filters = filters
      other
    }

  }

  final class QueryBuilder extends QueryBuilderCommons {

    private var descending = true

    def setDescending(v: Boolean): this.type = {
      descending = v
      this
    }

    def build(): TimeSeriesQuery =
      TimeSeriesQuery(
        aggregations = this.aggregations,
        intervals = this.intervals,
        filter = if (filters.nonEmpty) Option(AndFilter(filters)) else None,
        granularity = this.granularityOpt.getOrElse(GranularityType.Week),
        descending = this.descending.toString,
        dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource)
      )

    def topN(dimension: Symbol, threshold: Int): TopNQueryBuilder =
      copyTo(new TopNQueryBuilder(dimension.name, threshold))

    def groupBy(dimensions: Symbol*): GroupByQueryBuilder =
      copyTo(new GroupByQueryBuilder(dimensions.map(_.name)))

  }

  final class TopNQueryBuilder(dimensionName: String, n: Int) extends QueryBuilderCommons {
    protected var isDescending                            = true
    protected var metricOpt                               = Option.empty[String]
    protected var postAggregations: List[PostAggregation] = Nil

    def setDescending(v: Boolean): this.type = {
      isDescending = v
      this
    }

    def setMetric(metric: String): this.type = {
      metricOpt = Option(metric)
      this
    }

    def postAgg(aggs: PostAggregation*): this.type = {
      postAggregations =
        aggs.foldLeft(postAggregations)((postAggs, currentPostAgg) => currentPostAgg :: postAggs)
      this
    }

    def build(): TopNQuery =
      TopNQuery(
        dimension = Dimension(dimensionName),
        threshold = n,
        metric = metricOpt.get, // todo
        aggregations = this.aggregations,
        intervals = this.intervals,
        granularity = this.granularityOpt.getOrElse(GranularityType.All),
        filter = if (filters.nonEmpty) Option(AndFilter(filters)) else None,
        dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
        postAggregations = Nil
      )

  }

  final class GroupByQueryBuilder(dimensions: Seq[String]) extends QueryBuilderCommons {

    protected var havingOpt       = Option.empty[Having]
    protected var limitOpt        = Option.empty[Int]
    protected var thresholdOpt    = Option.empty[Int]
    protected var excludeNullsOpt = Option.empty[Boolean]

    def having(v: Having): this.type = {
      havingOpt = Option(v)
      this
    }

    def withLimit(v: Int): this.type = {
      limitOpt = Option(v)
      this
    }

    def withThreshold(v: Int): this.type = {
      thresholdOpt = Option(v)
      this
    }

    def setExcludeNulls(v: Boolean): this.type = {
      excludeNullsOpt = Option(v)
      this
    }

    def build(): GroupByQuery =
      GroupByQuery(
        aggregations = this.aggregations,
        intervals = this.intervals,
        filter = if (filters.nonEmpty) Option(AndFilter(filters)) else None,
        dimensions = dimensions.map(d => Dimension(d)).toList,
        granularity = this.granularityOpt.getOrElse(GranularityType.All),
        dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
        having = havingOpt,
        limit = limitOpt,
        threshold = thresholdOpt,
        excludeNulls = excludeNullsOpt
      )

  }

}
