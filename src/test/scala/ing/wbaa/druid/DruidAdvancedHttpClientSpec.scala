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

import ing.wbaa.druid.client.{ DruidAdvancedHttpClient, DruidHttpClient }
import ing.wbaa.druid.definitions._
import io.circe.generic.auto._
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.time._

import scala.concurrent.Future

class DruidAdvancedHttpClientSpec extends WordSpec with Matchers with ScalaFutures with Inspectors {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(3, Minutes), interval = Span(5, Millis))

  private val totalNumberOfEntries      = 39244
  private val numberOfConcurrentQueries = 4096

  case class TimeseriesCount(count: Int)

  s"When executing multiple ($numberOfConcurrentQueries) concurrent druid queries" should {
    val query = TimeSeriesQuery(
      aggregations = List(
        CountAggregation(name = "count")
      ),
      granularity = GranularityType.Hour,
      intervals = List("2011-06-01/2017-06-01")
    )

    s"fail to execute ${numberOfConcurrentQueries} concurrent queries when using '${classOf[DruidHttpClient].getName}'" in {
      implicit val config = DruidConfig(clientBackend = classOf[DruidHttpClient])

      implicit val ec = query.config.system.dispatcher

      val requests = Future.sequence((1 to numberOfConcurrentQueries).map(_ => query.execute))

      assertThrows[org.scalatest.exceptions.TestFailedException] {
        whenReady(requests) { responses =>
          forAll(responses) { response =>
            response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
          }
        }
      }

    }

    s"execute all ${numberOfConcurrentQueries} concurrent queries when using '${classOf[DruidAdvancedHttpClient].getName}'" in {
      implicit val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient])

      implicit val ec = query.config.system.dispatcher

      val requests = Future.sequence((1 to numberOfConcurrentQueries).map(_ => query.execute))

      whenReady(requests) { responses =>
        forAll(responses) { response =>
          response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
        }
      }

    }

  }

  "DruidAdvancedHttpClient" should {

    "indicate when Druid is healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient])
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe true
      }
    }

    "indicate when Druid is not healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                               hosts = Seq(QueryHost("localhost", 8087)))
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }
    }

    "correctly report the health status of all Druid brokers" in {

      // A map with the expected QueryHost to health status.
      // When the status is true the corresponding Broker is healthy, otherwise is false
      val expectedHealthCheck = Map(
        QueryHost("localhost", 8082) -> true,
        QueryHost("localhost", 8083) -> true,
        QueryHost("localhost", 8084) -> true,
        // the following node always fails with Internal Server Error
        QueryHost("localhost", 8087) -> false
      )

      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                               hosts = expectedHealthCheck.keys.toSeq)

      val client = config.client

      // since localhost:8087 is always failing the health status should be false
      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }

      whenReady(client.healthCheck) { outcome =>
        forAll(expectedHealthCheck) {
          case (broker, expectedResult) =>
            outcome(broker) shouldBe expectedResult
        }

      }

    }
  }

}
