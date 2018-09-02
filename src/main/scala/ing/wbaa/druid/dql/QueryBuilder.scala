package ing.wbaa.druid.dql

import ing.wbaa.druid._
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

  def agg(aggs: Aggregation*): this.type = {
    aggregations =
      aggs.foldLeft(aggregations)((aggregations, currentAgg) => currentAgg :: aggregations)
    this
  }

  def addIntervals(ints: String*): this.type = {
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

  def topN(dimension: Symbol, threshold: Int): TopNQueryBuilder =
    copyTo(new TopNQueryBuilder(dimension.name, threshold))

  def groupBy(dimensions: Symbol*): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions.map(_.name)))

}

final class TopNQueryBuilder private[dql] (dimensionName: String, n: Int)
    extends QueryBuilderCommons {
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
      filter = this.getFilters,
      dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
      postAggregations = Nil
    )

}

final class GroupByQueryBuilder private[dql] (dimensions: Seq[String]) extends QueryBuilderCommons {

  protected var havingOpt    = Option.empty[Having]
  protected var limitSpecOpt = Option.empty[LimitSpec]
  protected var limitOpt     = Option.empty[Int]

  protected var excludeNullsOpt = Option.empty[Boolean]

  def having(v: Expression): this.type = {
    havingOpt = Option(v.asHaving)
    this
  }

  def withLimit(n: Int, cols: OrderByColumnSpec*): this.type = {
    limitSpecOpt = Option(LimitSpec(limit = n, columns = cols))
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
      filter = this.getFilters,
      dimensions = this.dimensions.map(d => Dimension(d)).toList,
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      dataSource = this.dataSourceOpt.getOrElse(DruidConfig.datasource),
      having = havingOpt,
      limitSpec = this.limitSpecOpt
    )

}
