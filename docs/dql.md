# Druid query language (DQL)

Scruid provides a rich Scala API for building queries using the fluent pattern.

In order to use DQL, you have to import `ing.wbaa.druid.dql.DSL._` and thereafter build a query using the `DQL` query 
builder. The query can be time-series (default), group-by and top-n. 

For all three types of queries you can define the following:
 
 - The datasource name to perform the query, or defaults to the one that has been defined in the configuration.
 - The granularity of the query, e.g., `Hour`, `Day`, `Week`, etc (default is `Week` for time-series and `All` 
 for top-n and group-by).
 - The interval of the query, expressed as [ISO-8601 intervals](https://en.wikipedia.org/wiki/ISO_8601).
 - Filter dimensions
 - Define aggregations and post-aggregations

For example, consider the following fragment of a DQL query:

```scala
import ing.wbaa.druid.definitions.GranularityType
import ing.wbaa.druid.dql.DSL._

DQL
  .from("wikiticker")
  .withGranularity(GranularityType.Hour)
  .interval("2011-06-01/2017-06-01")
  .where('countryName === "Italy" or 'countryName === "Greece")
  .agg(count as "agg_count")
  .postAgg(('agg_count / 2) as "halfCount")
```

Function `from` defines the datasource to use, `withGranularity` defines the granularity of the query, `interval` the 
temporal interval of the data expressed in ISO-8601, `where` defines which rows of data should be included in the 
computation for a query, `agg` defines functions that summarize data (e.g., count of rows) and `postAgg` defines 
specifications of processing that should happen on aggregated values. 

In the above example we are performing a query over the datasource `wikiticker`, using hourly granularity, for the 
interval `2011-06-01` until `2017-06-01`. We are considering rows of data where the value of dimension `countryName` 
is either `Italy` or `Greece`. Furthermore, are interested in half counting the rows, thus we define the aggregation
function `count` and we are giving the name `agg_count` and thereafter we define a post-aggregation named `halfCount`
that takes the result of `agg_count` and divides it by `2`.

The equivalent fragment of a Druid query expressed in JSON is the one below:

```
{
  "dataSource" : "wikiticker",
  "granularity" : "hour",
  "intervals" : [ "2011-06-01/2017-06-01"],
  "filter" : {
      "fields" : [
        {
          "dimension" : "countryName",
          "value" : "Italy",
          "type" : "selector"
        },
        {
          "dimension" : "countryName",
          "value" : "Greece",
          "type" : "selector"
        }
      ],
      "type" : "or"
   },
  "aggregations" : [
    {
      "name" : "agg_count",
      "type" : "count"
    }
  ],
  "postAggregations" : [
    {
      "name" : "halfCount",
      "fn" : "/",
      "fields" : [
        {
          "name" : "agg_count",
          "fieldName" : "agg_count",
          "type" : "fieldAccess"
        },
        {
          "name" : "c_2.0",
          "value" : 2.0,
          "type" : "constant"
        }
      ],
      "type" : "arithmetic"
    }
  ]
}
```

Dimensions can be represented using Scala symbols, e.g., \`countryName, or by using function `dim`, 
e.g., `dim("countryName")` or as a String prefix function symbol `d`, e.g., `d"countryName"`. In the latter case
it is possible to pass a string with variables in order to perform string interpolation, for example:

```
val dimName = "countryName"

DQL.where(d"${dimName}" === "Greece")
```

### Operators

In `where` function you can define the following operators to filter the rows of data in a query.


#### Equals

| Example                                    | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `'countryName === "Greece"`                | the value of dimension `countryName` equals to "Greece"                 |
| `'dim === 10`                              | the value of dimension `dom` equals to 10                               |
| `'dim === 10.1`                            | the value of dimension `dom` equals to 10.1                             |
| `'dim1 === 'dim2`                          | the values of `'dim1` and `'dim2` are equal                             |

#### Not equals

| Example                                    | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `'countryName =!= "Greece"`                | the value of dimension `countryName` not equals to "Greece"             |
| `'dim =!= 10`                              | the value of dimension `dom` not equals to 10                           |
| `'dim =!= 10.1`                            | the value of dimension `dom` not equals to 10.1                         |
| `'dim1 =!= 'dim2`                          | the values of `'dim1` and `'dim2` are not equal                         |


#### Regular expression

It matches the specified dimension with the given pattern. The pattern can be any standard 
[Java regular expression](https://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html). For example,
match a floating point number from a string:

```
'dim regex "\\d+(\\.\\d+)"
```

#### Like

Like operators can be used for basic wildcard searches. They are equivalent to the `SQL LIKE` operator. For example,
match all last names that start with character 'S'. 

```
'lastName like "S%"
```

#### Search

Search operators can be used to filter on partial string matches. For example, for case sensitive search (default):

```
'dim contains "some string"

// which is equivalent to:
'dim contains("some string", caseSensitive = true)
```

Similarly, to ignore case sensitivity in search:

```
'dim containsIgnoreCase "some string"

// which is equivalent to:
'dim contains("some string", caseSensitive = false)
```

#### In

To express the `SQL IN` operator, in order to match the value of a dimension into any value of a specified set of values.
In the example below, we would like that the dimension `outlaw` matches to any of 'Good', 'Bad' or 'Ugly' values: 

```
'outlaw in ("Good", "Bad", "Ugly")
```

We can easily express a negation of the `in` operator, by directly using the `notIn` operator.

```
'outlaw notIn ("Good", "Bad", "Ugly")
```

#### Bound 

Bound operator can be used to filter on ranges of dimension values. It can be used for comparison filtering like 
greater than, less than, greater than or equal to, less than or equal to, and "between" (if both "lower" and 
"upper" are set). For details see the examples below.

When numbers are given to bound operators, then the ordering is numeric:

```
'dim > 10
'dim >= 10.0
'dim < 1.1
'dim <= 1

// 0.0 < dim < 10.0
'dim between(0.0, 10.0) 

// 0.0 <= dim <= 10.0
'dim between(0.0, 10.0, lowerStrict = true, upperStrict = true)

// 0.0 <= dim < 10.0
'dim between(0.0, 10.0, lowerStrict = true, upperStrict = false)
```

When strings are given to bound operators, then the ordering is lexicographic:

```
'dim > "10"
'dim >= "10.0"
'dim < "1.1"
'dim <= "1"

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0")

// "0.0" <= dim <= "10.0"
'dim between("0.0", "10.0", lowerStrict = true, upperStrict = true)

// "0.0" <= dim < "10.0"
'dim between("0.0", "10.0", lowerStrict = true, upperStrict = false)
```

Additionally, only when strings are given to bound operators, you can to specify a the ordering type and
extraction function:

```
'dim > "10" withOrdering(DimensionOrderType.alphanumeric) 

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0") withOrdering(DimensionOrderType.alphanumeric) 
```

```
'dim > "10" withOrdering(DimensionOrderType.alphanumeric)

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0") withOrdering(DimensionOrderType.alphanumeric)

// apply some extraction function
'dim > "10" withExtractionFn(<some ExtractionFn>) 
```

#### Interval 

Interval operator performs range filtering on dimensions that contain long millisecond values, 
with the boundaries specified as ISO 8601 time intervals.
 
```
'dim interval "2011-06-01/2012-06-01" 

'dim interval("2011-06-01/2012-06-01", "2012-06-01/2013-06-01", ...) 

TS interval "2011-06-01/2012-06-01"

TS interval("2011-06-01/2012-06-01", "2012-06-01/2013-06-01", ...)
```

#### Logical operators

###### AND

To define a logical `AND` between other operators you can use the operator `and`, for example:

```
('dim1 === 10) and ('dim2 interval "2011-06-01/2012-06-01") and ('dim3 === "foo")
```

Alternatively, you can use the function `conjunction`, as follows:

```
conjunction('dim1 === 10, 'dim2 interval "2011-06-01/2012-06-01", 'dim3 === "foo")
```

###### OR

To define a logical `OR` between other operators you can use the operator `or`, for example:

```
('dim1 === 10) or ('dim2 interval "2011-06-01/2012-06-01") or ('dim3 === "foo")
```

Alternatively, you can use the function `disjunction`, as follows:

```
disjunction('dim1 === 10, 'dim2 interval "2011-06-01/2012-06-01", 'dim3 === "foo")
```

###### NOT

To define a negation of an operator, you can use the operator `not`:

```
not('dim1 between (10, 100))
```

## Time-series query

This is simplest form of query and takes all the common DQL parameters.

For example, the following query is a time-series that counts the number of rows by hour.

```
case class TimeseriesCount(count: Long)

val query: TimeSeriesQuery = DQL
    .withGranularity(GranularityType.Hour)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .build()

val response = query.execute()
val result: List[TimeseriesCount] = response.list[TimeseriesCount]
```

## Group-by query

```
case class GroupByIsAnonymous(isAnonymous: String, count: Int)

val query: GroupByQuery = DQL
    .withGranularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .groupBy('isAnonymous)
    .build()
            
val response = query.execute()
val result: List[GroupByIsAnonymous] = response.list[GroupByIsAnonymous]
```

## Top-N query

```
case class PostAggregationAnonymous(count: Int, isAnonymous: String, halfCount: Double)

val query: TopNQuery = DQL
    .withGranularity(GranularityType.Week)
    .topN('isAnonymous, metric = "count", threshold = 5)
    .agg(count as "agg_count")
    .postAgg(('agg_count / 2) as "halfCount")
    .interval("2011-06-01/2017-06-01")
    .build()
    
val response = query.execute()
val result: List[PostAggregationAnonymous] = response.list[PostAggregationAnonymous]    
```

## Aggregations

todo

## Post-aggregations

todo

## Extract functions

todo