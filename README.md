[![CircleCI](https://circleci.com/gh/ing-bank/scruid.svg?style=svg)](https://circleci.com/gh/ing-bank/scruid)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9b7c4adf8ad447efa9c7ea8a9ffda6b2)](https://www.codacy.com/app/fokko/scruid?utm_source=github.com&utm_medium=referral&utm_content=ing-bank/scruid&utm_campaign=Badge_Coverage)
[![Download](https://api.bintray.com/packages/ing-bank/maven-releases/scruid/images/download.svg)](https://bintray.com/ing-bank/maven-releases/scruid/_latestVersion)

# Scruid

Scruid (Scala+Druid) is an open source library that allows you to compose queries easily in Scala. The library will take care of the translation of the query into json, parse the result in the case class that you define.

Currently the API is under heavy development, so changes might occur.



## Example queries:

Scruid provides three query constructors: `TopNQuery`, `GroupByQuery` and `TimeSeriesQuery` (see below for details). You can call the `execute` method ona query to send the query to Druid. This will return a `Future[DruidResponse]`. This response contains the [Circe](http://circe.io) JSON data without having it parsed to a specific case class yet. To interpret this JSON data you can run two methods on a `DruidResponse`:

- `.list[T](implicit decoder: Decoder[T]): List[T]` : This decodes the JSON to a list with items of type `T`.
- `.series[T](implicit decoder: Decoder[T]): Map[ZonedDateTime, T]` : This decodes the JSON to a timeseries map with the timestamp as key and `T` as value.

Below the example queries supported by Scruid. For more information about how to query Druid, and what query to pick, please refer to the [Druid documentation](http://druid.io/docs/latest/querying/querying.html)

### TopN query
```scala
case class TopCountry(count: Int, countryName: String = null)

val response = TopNQuery(
  dimension = Dimension(
    dimension = "countryName"
  ),
  threshold = 5,
  metric = "count",
  aggregations = List(
    CountAggregation(name = "count")
  ),
  intervals = List("2011-06-01/2017-06-01")
).execute

val result: Future[Map[ZonedDateTime, List[TopCountry]]] = response.map(_.series[List[TopCountry]])
```


### GroupBy query

```scala
case class GroupByIsAnonymous(isAnonymous: Boolean, count: Int)

val response = GroupByQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  dimensions = List("isAnonymous"),
  intervals = List("2011-06-01/2017-06-01")
).execute()

val result: Future[List[GroupByIsAnonymous]] = response.map(_.list[GroupByIsAnonymous])
```

The returned `Future[DruidResponse]` will contain json data where `isAnonymouse` is either `true or false`. Please keep in mind that Druid is only able to handle strings, and recently also numerics. So Druid will be returning a string, and the conversion from a string to a boolean is done by the json parser.

### TimeSeries query

```scala
case class TimeseriesCount(count: Int)

val response = TimeSeriesQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  granularity = GranularityType.Hour,
  intervals = List("2011-06-01/2017-06-01")
).execute

val series: Future[Map[ZonedDateTime, TimeseriesCount]] = response.map(_.series[TimeseriesCount])
```

To get the timeseries data from this `Future[DruidRespones]` you can run `val series = result.series[TimeseriesCount]`.

## Druid query language (DQL)

Scruid also provides a rich Scala API for building queries using the fluent pattern.

```scala
case class GroupByIsAnonymous(isAnonymous: String, country: String, count: Int)

val query: GroupByQuery = DQL
    .granularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .where('countryName.isNotNull)
    .groupBy('isAnonymous, 'countryName.extract(UpperExtractionFn()) as "country")
    .having('count > 100 and 'count < 200)
    .limit(10, 'count.desc(DimensionOrderType.Numeric))
    .build()

val response: Future[List[GroupByIsAnonymous]] = query.execute().map(_.list[GroupByIsAnonymous])
```

For details and examples see the [DQL documentation](docs/dql.md).

## Handling large payloads with Akka Streams

For queries with large payload of results (e.g., half a million of records), Scruid can transform the corresponding response into an [Akka Stream](https://doc.akka.io/docs/akka/2.5/stream/) Source. 
The results can be processed, filtered and transformed using [Flows](https://doc.akka.io/docs/akka/2.5/stream/stream-flows-and-basics.html) and/or output to Sinks, as a continuous stream, without collecting the entire payload first.
To process the results with Akka Stream, you can call one of the following methods:

- `.stream`: gives a Source of `DruidResult`.
- `.streamAs[T](implicit decoder: Decoder[T])`: gives a Source where each JSON record is being decoded to the type of `T`.
- `.streamSeriesAs[T](implicit decoder: Decoder[T])`: gives a Source where each JSON record is being decoded to the type of `T` and it is accompanied by its corresponding timestamp.

All the methods above can be applied to any timeseries, group-by or top-N query created either directly by using query constructors or by DQL.

### Example

```scala
implicit val mat = DruidClient.materializer

case class TimeseriesCount(count: Int)

val query = TimeSeriesQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  granularity = GranularityType.Hour,
  intervals = List("2011-06-01/2017-06-01")
)

// Decode each record into the type of `TimeseriesCount` and sum all `count` results
val result: Future[Int] = query
        .streamAs[TimeseriesCount]
        .map(_.count)
        .runWith(Sink.fold(0)(_ + _))
```

## Configuration

The configuration is done by [Typesafe config](https://github.com/typesafehub/config). The configuration can be overridden by using environment variables, e.g. `DRUID_HOST`, `DRUID_PORT` and `DRUID_DATASOURCE`. Or by placing an application.conf in your own project and this will override the reference.conf of the scruid library.

```
druid = {
  host = "localhost"
  host = ${?DRUID_HOST}
  port = 8082
  port = ${?DRUID_PORT}
  secure = false
  secure = ${?DRUID_USE_SECURE_CONNECTION}
  url = "/druid/v2/"
  url = ${?DRUID_URL}

  datasource = "wikiticker"
  datasource = ${?DRUID_DATASOURCE}

  response-parsing-timeout = 5 seconds
  response-parsing-timeout = ${?DRUID_RESPONSE_PARSING_TIMEOUT}
}
```

Alternatively it can be programmatically overridden by defining an implicit instance of `ing.wbaa.druid.DruidConfig`:

```scala
import java.time.ZonedDateTime
import ing.wbaa.druid._
import ing.wbaa.druid.definitions._
import scala.concurrent.duration._


implicit val druidConf = DruidConfig(
  host = "localhost",
  port = 8082,
  datasource = "wikiticker",
  responseParsingTimeout = 10.seconds
)


case class TimeseriesCount(count: Int)

val response = TimeSeriesQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  granularity = GranularityType.Week,
  intervals = List("2011-06-01/2017-06-01")
).execute

val series: Map[ZonedDateTime, TimeseriesCount] = response.series[TimeseriesCount]
```

All parameters of `DruidConfig` are optional, and in case that some parameter is missing then the default behaviour is to use the value that is defined in the configuration file.

## Tests

To run the tests, please make sure that you have the Druid instance running:

```
docker-compose run --rm wait_for_druid && docker-compose up -d chaos_proxies
```
