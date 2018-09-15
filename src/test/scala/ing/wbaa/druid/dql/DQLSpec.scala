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

import ing.wbaa.druid.{ GroupByQuery, TimeSeriesQuery }
import ing.wbaa.druid.definitions._
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent._
import org.scalatest.time.{ Millis, Seconds, Span }
import ing.wbaa.druid.dql.DSL._
import io.circe.generic.auto._

class DQLSpec extends WordSpec with Matchers with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries = 39244

  case class TimeseriesCount(count: Int)
  case class GroupByIsAnonymous(isAnonymous: String, count: Int)
  case class TopCountry(count: Int, countryName: Option[String])
  case class AggregatedFilteredAnonymous(count: Int, isAnonymous: String, filteredCount: Int)
  case class PostAggregationAnonymous(count: Int, isAnonymous: String, halfCount: Double)

  "DQL TimeSeriesQuery" should {
    "successfully be interpreted by Druid" in {

      val query: TimeSeriesQuery = DQL.Builder
        .withGranularity(GranularityType.Hour)
        .addIntervals("2011-06-01/2017-06-01")
        .agg(count(name = "count"))
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "extract the data and return a map with the timestamps as keys" in {
      val query: TimeSeriesQuery = DQL.Builder
        .withGranularity(GranularityType.Hour)
        .addIntervals("2011-06-01/2017-06-01")
        .agg(count(name = "count"))
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response
          .series[TimeseriesCount]
          .flatMap { case (_, items) => items.map(_.count) }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }

  "GroupByQuery" should {
    "successfully be interpreted by Druid" in {
      val query = DQL.Builder
        .addIntervals("2011-06-01/2017-06-01")
        .agg(count("count"))
        .groupBy('isAnonymous)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when using lower granularity" in {
      val query = DQL.Builder
        .addIntervals("2011-06-01/2017-06-01")
        .agg(count("count"))
        .groupBy('isAnonymous)
        .withGranularity(GranularityType.Hour)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

}
