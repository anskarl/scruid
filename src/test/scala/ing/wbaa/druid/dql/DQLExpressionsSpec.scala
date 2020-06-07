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
import java.util.Locale

import ing.wbaa.druid.{ DruidConfig, GroupByQuery }
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions.{ GranularityType, Inline, Table }
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.expressions.ExpressionFunctions._
import ing.wbaa.druid.dql.expressions.ExpressionOps._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.generic.auto._

import scala.concurrent.duration._
import scala.language.postfixOps

class DQLExpressionsSpec extends AnyWordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  implicit val config = DruidConfig(clientBackend = classOf[DruidHttpClient])

  implicit val mat = config.client.actorMaterializer

  private val countryData = Locale.getISOCountries.toList
    .map { code =>
      val locale = new Locale("en", code)
      List(code, locale.getISO3Country, locale.getDisplayCountry)
    }

  val numberOfResults = 100

  def baseScan =
    DQL
      .scan()
      .granularity(GranularityType.All)
      .interval("0000/4000")
      .batchSize(10)
      .limit(numberOfResults)

  case class JoinedScanResult(
      channel: Option[String],
      cityName: Option[String],
      countryIsoCode: Option[String],
      user: Option[String],
      mapped_country_iso3_code: Option[String],
      mapped_country_name: Option[String]
  )

  "DQL join query with expressions" should {
    "lower" in {
      val query = baseScan
        .from(
          Table("wikipedia")
            .joinNew(
              right = Inline(Seq("iso2_code", "iso3_code", "name"), countryData),
              prefix = "mapped_country_",
              (l, r) => upper(l("countryIsoCode")) === r("iso2_code") //upper(d"countryIsoCode") === d"mapped_country_iso2_code"
            )
//            .join(
//              right = Inline(Seq("iso2_code", "iso3_code", "name"), countryData),
//              prefix = "mapped_country_",
//              condition = upper(d"countryIsoCode") === d"mapped_country_iso2_code"
//            )
        )
        .build()

      println(query.toDebugString)

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
