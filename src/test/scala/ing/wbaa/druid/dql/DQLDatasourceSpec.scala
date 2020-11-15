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

// scalastyle:off

import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL._
import io.circe.generic.auto._
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class DQLDatasourceSpec extends AnyWordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  implicit val config = DruidConfig(clientBackend = classOf[DruidHttpClient])

  implicit val mat = config.client.actorMaterializer

  private val totalNumberOfEntries = 39244

  case class GroupByIsAnonymous(isAnonymous: String, count: Int)

  case class CountryCity(country: String, city: String)
  case class CountryCodes(iso2_code_uppercase: String,
                          iso2_code_lowercase: String,
                          iso3_code: String,
                          name: String)

  case class JoinedScanResult(
      channel: Option[String],
      cityName: Option[String],
      countryIsoCode: Option[String],
      user: Option[String],
      mapped_country_iso3_code: Option[String],
      mapped_country_name: Option[String]
  )
//Inline(Seq("iso2_code_uppercase", "iso2_code_lowercase", "iso3_code", "name"), countryData)
  "DQL query with datasource" should {

    "successfully be interpreted by Druid when datasource is Table" in {
      val query: GroupByQuery = DQL
        .interval("2011-06-01/2017-06-01")
        .from(Table("wikipedia"))
        .agg(count as "count")
        .groupBy(d"isAnonymous")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when datasource is inline" in {

      val query: ScanQuery = DQL
        .scan()
        .interval("0000/3000")
        .from(InlineDatasources.countryDatasource)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val expected = InlineDatasources.countryData
          .map {
            case code2_uppercased :: code2_lowercased :: code3 :: name :: Nil =>
              CountryCodes(code2_uppercased, code2_lowercased, code3, name)
          }

        response.list[CountryCodes] shouldEqual expected
      }
    }

    "successfully be interpreted by Druid when performing join" in {
      val numberOfResults = 100

      val query: ScanQuery = DQL
        .scan()
        .columns("channel",
                 "cityName",
                 "countryIsoCode",
                 "user",
                 "mapped_country_iso3_code",
                 "mapped_country_name")
        .granularity(GranularityType.All)
        .interval("0000/4000")
        .batchSize(10)
        .limit(numberOfResults)
        .from(
          Table("wikipedia")
            .join(
              right = InlineDatasources.countryDatasource,
              prefix = "mapped_country_",
              condition = (l, r) => l("countryIsoCode") === r("iso2_code_uppercase")
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

}

// scalastyle:on
