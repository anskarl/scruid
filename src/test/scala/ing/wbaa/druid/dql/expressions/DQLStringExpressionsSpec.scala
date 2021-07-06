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

package ing.wbaa.druid.dql.expressions

import ing.wbaa.druid.DruidConfig
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions.{ GranularityType, Table }
import ing.wbaa.druid.dql.{ CountryCodeData, ScanQueryBuilder }
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.expressions.ExpressionFunctions._
import io.circe.generic.auto._
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._
import scala.language.postfixOps

class DQLStringExpressionsSpec extends AnyWordSpec with Matchers with ScalaFutures with Inspectors {

  case class JoinedScanResult(
      channel: Option[String],
      cityName: Option[String],
      countryIsoCode: Option[String],
      user: Option[String],
      mapped_country_iso3_code: Option[String],
      mapped_country_name: Option[String]
  )

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  implicit val config = DruidConfig(clientBackend = classOf[DruidHttpClient])

  implicit val mat = config.client.actorMaterializer

  val numberOfResults = 100

  def baseScan: ScanQueryBuilder =
    DQL
      .scan()
      .granularity(GranularityType.All)
      .interval("0000/4000")
      .batchSize(10)
      .limit(numberOfResults)

  "DQL join query with string function expressions" should {
    "lowercase and uppercase over countryIsoCode" in {
      val query = baseScan
        .from(
          Table("wikipedia")
            .join(
              right = CountryCodeData.datasource,
              prefix = "mapped_country_",
              (l, r) =>
                lower(l("countryIsoCode")) === r("iso2_code_lowercase") and
                upper(l("countryIsoCode")) === r("iso2_code_uppercase")
            )
        )
        .build()

      val request = query.execute()
      whenReady(request) { response =>
        val resultList = response.list[JoinedScanResult]
        resultList.size shouldBe numberOfResults

        resultList.forall(_.mapped_country_iso3_code.isDefined) shouldBe true
        resultList.forall(_.mapped_country_name.isDefined) shouldBe true

        val resultSeries = response.series[JoinedScanResult]
        resultSeries.size shouldBe numberOfResults
      }
    }

  }

  "DQL query with string function expressions" should {
    "concat two columns" in {
      val query = baseScan
        .from("wikipedia")
        .where(d"cityName".isNotNull and d"countryIsoCode".isNotNull)
        .virtualColumn("concat_country_city", concat(d"cityName", "_", d"countryIsoCode"))
        .build()
    }
  }

}
