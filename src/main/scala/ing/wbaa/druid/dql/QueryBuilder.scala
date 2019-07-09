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

import ing.wbaa.druid._
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.expressions._

import scala.concurrent.duration.FiniteDuration

/**
  * Collection of common functions for all query builders
  */
private[dql] sealed trait QueryBuilderCommons {

  protected var queryContextOpt                 = Option.empty[QueryContext]
  protected var dataSourceOpt                   = Option.empty[String]
  protected var granularityOpt                  = Option.empty[Granularity]
  protected var aggregations: List[Aggregation] = Nil

  protected var complexAggregationNames: Set[String] = Set.empty[String]

  protected var intervals: List[String] = Nil

  protected var filters: List[Filter] = Nil

  protected var postAggregationExpr: List[PostAggregationExpression] = Nil

  def queryContext(ctx: QueryContext): this.type = {
    queryContextOpt = Option(ctx)
    this
  }

  def queryContext(ctx: QueryContextBuilder => QueryContextBuilder): this.type = {
    val builder = ctx(new QueryContextBuilder)
    queryContextOpt = Option(builder.build())
    this
  }

  /**
    * Specify the datasource to use, other than the default one in the configuration
    */
  def from(dataSource: String): this.type = {
    dataSourceOpt = Option(dataSource)
    this
  }

  /**
    * Define the granularity to bucket query results
    */
  def granularity(granularity: Granularity): this.type = {
    granularityOpt = Option(granularity)
    this
  }

  /**
    * Specify one or more aggregations to use
    */
  def agg(aggs: AggregationExpression*): this.type = {

    complexAggregationNames ++= aggs.filter(_.isComplex).map(_.getName)
    aggregations = aggs.foldRight(aggregations)((agg, acc) => agg.build() :: acc)

    this
  }

  /**
    * Specify one or more post-aggregations to use
    */
  def postAgg(postAggs: PostAggregationExpression*): this.type = {
    postAggregationExpr = postAggs.foldRight(postAggregationExpr)((agg, acc) => agg :: acc)
    this
  }

  /**
    * Specify the time range to run the query over (should be expressed as ISO-8601 interval)
    */
  def interval(interval: String): this.type = {
    intervals = interval :: intervals
    this
  }

  /**
    * Specify multiple time ranges to run the query over (should be expressed as ISO-8601 intervals)
    */
  def intervals(ints: String*): this.type = {
    intervals ++= ints.toList
    this
  }

  /**
    * Specify one or more filter operations to use in the query
    */
  def where(filter: FilteringExpression): this.type = {

    filters = filter.asFilter :: filters
    this
  }

  protected def getFilters: Option[Filter] =
    if (filters.isEmpty) None
    else if (filters.size == 1) Option(filters.head)
    else Option(AndFilter(filters))

  protected def copyTo[T <: QueryBuilderCommons](other: T): T = {
    other.queryContextOpt = queryContextOpt
    other.dataSourceOpt = dataSourceOpt
    other.granularityOpt = granularityOpt
    other.aggregations = aggregations
    other.complexAggregationNames = complexAggregationNames
    other.intervals = intervals
    other.filters = filters
    other.postAggregationExpr = postAggregationExpr
    other
  }

  protected def getPostAggs: List[PostAggregation] =
    postAggregationExpr.map(expr => expr.build(complexAggregationNames))

}

/**
  * This is the default query build, in order to create Timeseries queries
  */
final class QueryBuilder private[dql] () extends QueryBuilderCommons {

  // Default is false (ascending)
  private var descending = false

  /**
    * Define whether to make descending ordered result
    */
  def setDescending(v: Boolean): this.type = {
    descending = v
    this
  }

  /**
    * Gives the resulting time-series query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting time-series query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): TimeSeriesQuery = {

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    TimeSeriesQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = this.getFilters,
      granularity = this.granularityOpt.getOrElse(GranularityType.Week),
      descending = this.descending.toString,
      postAggregations = this.getPostAggs,
      context = this.queryContextOpt.getOrElse(QueryContext.empty)
    )(conf)
  }

  /**
    * Define that the query will be a top-n query
    *
    * @param dimension the dimension that you want the top taken for
    * @param metric the metric to sort by for the top list
    * @param threshold an integer defining the N in the top-n
    *
    * @return the builder for top-n queries
    */
  def topN(dimension: Dim, metric: String, threshold: Int): TopNQueryBuilder =
    copyTo(new TopNQueryBuilder(dimension, metric, threshold))

  /**
    * Define that the query will be a group-by query
    *
    * @param dimensions the dimensions to perform a group-by query
    * @return the builder for group-by queries
    */
  def groupBy(dimensions: Dim*): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions))

  def groupBy(dimensions: Iterable[Dim]): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions))
}

/**
  * Builder for top-n queries
  *
  * @param dimension the dimension that you want the top taken for
  * @param metric the metric to sort by for the top list
  * @param n an integer defining the N in the top-n
  */
final class TopNQueryBuilder private[dql] (dimension: Dim, metric: String, n: Int)
    extends QueryBuilderCommons {

  // Default is true (descending)
  protected var isDescending = true

  /**
    * Define whether to make descending ordered result
    */
  def setDescending(v: Boolean): this.type = {
    isDescending = v
    this
  }

  /**
    * Gives the resulting top-n query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting top-n query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): TopNQuery = {

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    TopNQuery(
      dimension = this.dimension.build(),
      threshold = n,
      metric = metric,
      aggregations = this.aggregations,
      intervals = this.intervals,
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      filter = this.getFilters,
      postAggregations = this.getPostAggs,
      context = this.queryContextOpt.getOrElse(QueryContext.empty)
    )(conf)
  }

}

/**
  * Builder for group-by queries
  *
  * @param dimensions the dimensions to perform a group-by query
  */
final class GroupByQueryBuilder private[dql] (dimensions: Iterable[Dim])
    extends QueryBuilderCommons {

  protected var limitOpt                        = Option.empty[Int]
  protected var limitCols                       = Iterable.empty[OrderByColumnSpec]
  protected var havingExpressions: List[Having] = Nil

  protected var excludeNullsOpt = Option.empty[Boolean]

  /**
    * Specify which rows from a group-by query should be returned, by specifying conditions on aggregated values
    */
  def having(conditions: FilteringExpression): this.type = {
    havingExpressions = conditions.asHaving :: havingExpressions
    this
  }

  /**
    * Sort and limit the set of results of the group-by query
    *
    * @param n the upper limit of the number of results
    * @param direction specify the order of the results (i.e., ascending or descending) for all group-by dimensions
    * @param dimensionOrderType specify the type of the ordering (lexicographic, alphanumeric, etc.)
    *                           for all group-by dimensions. Default is lexicographic.
    */
  def limit(
      n: Int,
      direction: Direction,
      dimensionOrderType: DimensionOrderType = DimensionOrderType.Lexicographic
  ): this.type = {
    limitOpt = Option(n)
    limitCols = dimensions.map(
      dim => OrderByColumnSpec(dim.name, direction, DimensionOrder(dimensionOrderType))
    )
    this
  }

  /**
    * Sort and limit the set of results of the group-by query
    *
    * @param n the upper limit of the number of results
    * @param cols specify the ordering per group-by dimension
    */
  def limit(n: Int, cols: OrderByColumnSpec*): this.type = {
    limitOpt = Option(n)
    limitCols = cols
    this
  }

  /**
    * Specify whether or not to exclude null for all group-by dimensions
    */
  def setExcludeNulls(v: Boolean): this.type = {
    excludeNullsOpt = Option(v)
    this
  }

  /**
    * Gives the resulting group-by query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting group-by query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): GroupByQuery = {

    val havingOpt =
      if (havingExpressions.isEmpty) None
      else if (havingExpressions.size == 1) Option(havingExpressions.head)
      else Option(definitions.AndHaving(havingExpressions))

    val limitSpecOpt =
      limitOpt.map { n =>
        if (limitCols.nonEmpty) LimitSpec(limit = n, columns = limitCols)
        else LimitSpec(limit = n, columns = dimensions.map(dim => OrderByColumnSpec(dim.name)))
      }

    // when excludeNullsOpt is Some(true)
    // then set not null filtering for all dimensions
    if (excludeNullsOpt.contains(true)) {
      val excludeNullsExpressions = dimensions.map(dim => new Not(new NullDim(dim)))

      this.where(new And(excludeNullsExpressions))
    }

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    GroupByQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = this.getFilters,
      dimensions = this.dimensions.map(_.build()),
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      having = havingOpt,
      limitSpec = limitSpecOpt,
      postAggregations = this.getPostAggs,
      context = this.queryContextOpt.getOrElse(QueryContext.empty)
    )(conf)
  }

}

final class QueryContextBuilder { self =>

  // the following apply only to all queries
  private var timeoutOpt: Option[Long]                         = None
  private var priorityOpt: Option[Int]                         = None
  private var queryIdOpt: Option[String]                       = None
  private var useCacheOpt: Option[Boolean]                     = None
  private var populateCacheOpt: Option[Boolean]                = None
  private var useResultLevelCacheOpt: Option[Boolean]          = None
  private var populateResultLevelCacheOpt: Option[Boolean]     = None
  private var bySegmentOpt: Option[Boolean]                    = None
  private var finalizeAggregationResultsOpt: Option[Boolean]   = None
  private var chunkPeriodOpt: Option[String]                   = None
  private var maxScatterGatherBytesOpt: Option[Long]           = None
  private var maxQueuedBytesOpt: Option[Long]                  = None
  private var serializeDateTimeAsLongOpt: Option[Boolean]      = None
  private var serializeDateTimeAsLongInnerOpt: Option[Boolean] = None
  // the following apply only to time-series queries
  private var skipEmptyBucketsOpt: Option[Boolean] = None
  private var grandTotalOpt: Option[Boolean]       = None
  // the following apply only to top-n queries
  private var minTopNThresholdOpt: Option[Int] = None
  // the following apply only to group-by queries
  private var groupByStrategyOpt: Option[GroupByStrategyType] = None
  private var groupByIsSingleThreadedOpt: Option[Boolean]     = None
  // Group-by V2
  private var maxMergingDictionarySizeOpt: Option[Long]     = None
  private var maxOnDiskStorageOpt: Option[Long]             = None
  private var bufferGrouperInitialBucketsOpt: Option[Int]   = None
  private var bufferGrouperMaxLoadFactorOpt: Option[Double] = None
  private var forceHashAggregationOpt: Option[Boolean]      = None
  private var intermediateCombineDegreeOpt: Option[Int]     = None
  private var numParallelCombineThreadsOpt: Option[Int]     = None
  // todo check if it has any side-effect regarding how Scruid parses responses
  private var sortByDimsFirstOpt: Option[Boolean]    = None
  private var forceLimitPushDownOpt: Option[Boolean] = None

  // Group-by V1
  private var maxIntermediateRowsOpt: Option[Int] = None
  private var maxResultsOpt: Option[Int]          = None
  private var useOffheapOpt: Option[Boolean]      = None

  def timeout(v: Long): self.type = {
    timeoutOpt = Option(v)
    self
  }

  def timeout(duration: FiniteDuration): self.type = {
    timeoutOpt = Option(duration.toMillis)
    self
  }

  def priority(v: Int): self.type = {
    priorityOpt = Option(v)
    self
  }

  def queryId(v: String): self.type = {
    queryIdOpt = Option(v)
    self
  }

  def useCache(v: Boolean): self.type = {
    useCacheOpt = Option(v)
    self
  }

  def populateCache(v: Boolean): self.type = {
    populateCacheOpt = Option(v)
    self
  }

  def useResultLevelCache(v: Boolean): self.type = {
    useResultLevelCacheOpt = Option(v)
    self
  }

  def populateResultLevelCache(v: Boolean): self.type = {
    populateResultLevelCacheOpt = Option(v)
    self
  }

  def bySegment(v: Boolean): self.type = {
    bySegmentOpt = Option(v)
    self
  }

  def finalizeAggregationResults(v: Boolean): self.type = {
    finalizeAggregationResultsOpt = Option(v)
    self
  }

  def chunkPeriod(v: String): self.type = {
    chunkPeriodOpt = Option(v)
    self
  }

  def chunkPeriod(v: java.time.Duration): self.type = {
    chunkPeriodOpt = Option(v.toString)
    self
  }

  def maxScatterGatherBytes(v: Long): self.type = {
    maxScatterGatherBytesOpt = Option(v)
    self
  }

  def maxQueuedBytes(v: Long): self.type = {
    maxQueuedBytesOpt = Option(v)
    self
  }

  def serializeDateTimeAsLong(v: Boolean): self.type = {
    serializeDateTimeAsLongOpt = Option(v)
    self
  }

  def serializeDateTimeAsLongInner(v: Boolean): self.type = {
    serializeDateTimeAsLongInnerOpt = Option(v)
    self
  }

  def skipEmptyBuckets(v: Boolean): self.type = {
    skipEmptyBucketsOpt = Option(v)
    self
  }
  def grandTotal(v: Boolean): self.type = {
    grandTotalOpt = Option(v)
    self
  }

  def minTopNThreshold(v: Int): self.type = {
    minTopNThresholdOpt = Option(v)
    self
  }

  def groupByStrategy(v: GroupByStrategyType): self.type = {
    groupByStrategyOpt = Option(v)
    self
  }

  def groupByIsSingleThreaded(v: Boolean): self.type = {
    groupByIsSingleThreadedOpt = Option(v)
    self
  }

  def maxMergingDictionarySize(v: Long): self.type = {
    maxMergingDictionarySizeOpt = Option(v)
    self
  }

  def maxOnDiskStorage(v: Long): self.type = {
    maxOnDiskStorageOpt = Option(v)
    self
  }

  def bufferGrouperInitialBuckets(v: Int): self.type = {
    bufferGrouperInitialBucketsOpt = Option(v)
    self
  }
  def bufferGrouperMaxLoadFactor(v: Double): self.type = {
    bufferGrouperMaxLoadFactorOpt = Option(v)
    self
  }
  def forceHashAggregation(v: Boolean): self.type = {
    forceHashAggregationOpt = Option(v)
    self
  }
  def intermediateCombineDegree(v: Int): self.type = {
    intermediateCombineDegreeOpt = Option(v)
    self
  }
  def numParallelCombineThreads(v: Int): self.type = {
    numParallelCombineThreadsOpt = Option(v)
    self
  }
  def sortByDimsFirst(v: Boolean): self.type = {
    sortByDimsFirstOpt = Option(v)
    self
  }
  def forceLimitPushDown(v: Boolean): self.type = {
    forceLimitPushDownOpt = Option(v)
    self
  }
  def maxIntermediateRows(v: Int): self.type = {
    maxIntermediateRowsOpt = Option(v)
    self
  }
  def maxResults(v: Int): self.type = {
    maxResultsOpt = Option(v)
    self
  }
  def useOffheap(v: Boolean): self.type = {
    useOffheapOpt = Option(v)
    self
  }

  def build(): QueryContext =
    QueryContext(
      timeout = timeoutOpt,
      priority = priorityOpt,
      queryId = queryIdOpt,
      useCache = useCacheOpt,
      populateCache = populateCacheOpt,
      useResultLevelCache = useResultLevelCacheOpt,
      populateResultLevelCache = populateResultLevelCacheOpt,
      bySegment = bySegmentOpt,
      finalizeAggregationResults = finalizeAggregationResultsOpt,
      chunkPeriod = chunkPeriodOpt,
      maxScatterGatherBytes = maxScatterGatherBytesOpt,
      maxQueuedBytes = maxQueuedBytesOpt,
      serializeDateTimeAsLong = serializeDateTimeAsLongOpt,
      serializeDateTimeAsLongInner = serializeDateTimeAsLongInnerOpt,
      skipEmptyBuckets = skipEmptyBucketsOpt,
      grandTotal = grandTotalOpt,
      minTopNThreshold = minTopNThresholdOpt,
      groupByStrategy = groupByStrategyOpt,
      groupByIsSingleThreaded = groupByIsSingleThreadedOpt,
      maxMergingDictionarySize = maxMergingDictionarySizeOpt,
      maxOnDiskStorage = maxOnDiskStorageOpt,
      bufferGrouperInitialBuckets = bufferGrouperInitialBucketsOpt,
      bufferGrouperMaxLoadFactor = bufferGrouperMaxLoadFactorOpt,
      forceHashAggregation = forceHashAggregationOpt,
      intermediateCombineDegree = intermediateCombineDegreeOpt,
      numParallelCombineThreads = numParallelCombineThreadsOpt,
      sortByDimsFirst = sortByDimsFirstOpt,
      forceLimitPushDown = forceLimitPushDownOpt,
      maxIntermediateRows = maxIntermediateRowsOpt,
      maxResults = maxResultsOpt,
      useOffheap = useOffheapOpt
    )
}
