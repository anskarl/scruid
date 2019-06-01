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

import ing.wbaa.druid.client.{ DruidAdvancedHttpClient, DruidCachedHttpClient, DruidHttpClient }
import ing.wbaa.druid.definitions._
import io.circe.generic.auto._
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.time._

import scala.concurrent.Future

class LoadBalanceSpec extends WordSpec with Matchers with ScalaFutures with Inspectors {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(3, Minutes), interval = Span(5, Millis))

  private val totalNumberOfEntries      = 39244
  private val numberOfConcurrentQueries = 4096

  case class TimeseriesCount(count: Int)

  s"When executing multiple ($numberOfConcurrentQueries) concurrent druid queries on multiple Brokers" should {

    val query = TimeSeriesQuery(
      aggregations = List(
        CountAggregation(name = "count")
      ),
      granularity = GranularityType.Hour,
      intervals = List("2011-06-01/2017-06-01")
    )

    "successfully be load-balanced across multiple query nodes" in {

      implicit val config = DruidConfig(
        hosts = Seq(
          QueryHost("localhost", 8082),
          QueryHost("localhost", 8083),
          QueryHost("localhost", 8084)
        ),
        clientBackend = classOf[DruidAdvancedHttpClient].getName
      )

      implicit val ec = query.config.system.dispatcher

      val requests = Future.sequence((1 to numberOfConcurrentQueries).map(_ => query.execute))

      whenReady(requests) { responses =>
        forAll(responses) { response =>
          response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
        }
      }
    }

    "successfully be load-balanced across multiple query nodes with failing nodes" in {

      implicit val config = DruidConfig(
        hosts = Seq(
          QueryHost("localhost", 8082),
          QueryHost("localhost", 8083),
          QueryHost("localhost", 8084),
          // the following nodes should randomly fail
          QueryHost("localhost", 8085),
          QueryHost("localhost", 8086)
        ),
        clientBackend = classOf[DruidAdvancedHttpClient].getName
      )

      implicit val ec = query.config.system.dispatcher

      val requests = Future.sequence((1 to numberOfConcurrentQueries).map(_ => query.execute))

      whenReady(requests) { responses =>
        forAll(responses) { response =>
          response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
        }
      }
    }

  }

}
