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
// scalastyle:off
import akka.stream.ActorMaterializer
import ing.wbaa.druid.{ DruidConfig, ScanQuery }
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions.{ GranularityType, Table }
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.LanguageCodeData.LanguageCodeRow
import ing.wbaa.druid.dql.expressions.ExpressionFunctions._
import ing.wbaa.druid.dql.{ CountryCodeData, LanguageCodeData, ScanQueryBuilder }
import io.circe.Json
import io.circe.generic.auto._
import org.scalatest.{ Ignore, Inspectors }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class DQLArrayExpressionsSpec extends AnyWordSpec with Matchers with ScalaFutures with Inspectors {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  implicit val config: DruidConfig = DruidConfig(clientBackend = classOf[DruidHttpClient])

  implicit val mat: ActorMaterializer = config.client.actorMaterializer

  val numberOfResults = 100

  def baseScan: ScanQueryBuilder =
    DQL
      .scan()
      .granularity(GranularityType.All)
      .interval("0000/4000")
      .batchSize(10)
      .limit(numberOfResults)

  "foo" should {
//    "all" in {
//      val query = baseScan
//        .from(LanguageCodeData.datasource)
//        .columns("iso2_lang", "csv_iso2_countries", "arr_iso2_countries")
//        .build()
//
//      println("=" * 80)
//      println(query.toDebugString)
//      println("=" * 80)
//
//      val resultsF = query.execute()
//
//      whenReady(resultsF) { results =>
//        results.list[LanguageCodeRow].foreach(println)
//        assert(true)
//      }
//    }

    "function `arrayToString`" in {
      val query = baseScan
        .from(
          LanguageCodeData.datasource
            .join(
              right = LanguageCodeData.datasource,
              prefix = "right_",
              (l, r) => arrayToString(l("arr_iso2_countries"), ",") === r("csv_iso2_countries")
            )
        )
        .build()

//      println("=" * 80)
//      println(query.toDebugString)
//      println("=" * 80)

      val resultsF = query.execute()
      whenReady(resultsF) { results =>
        results.list[Json].foreach(println)
        assert(true)
      }

    }

    "function `stringToArray`" in {
      val query = baseScan
        .from(LanguageCodeData.datasourceSmall)
        .virtualColumn("virtual_column_test",
                       expression = "string_to_array(csv_iso2_countries, ',')")
        .virtualColumn("virtual_column_test2",
                       expression = stringToArray(d"csv_iso2_countries", ","))
        .virtualColumn("virtual_column_test3", expression = "string_to_array('foo,bar', ',')")
        //.virtualColumn("virtual_column_test4", expression = stringToArray(lit("foo,bar"), ","))
        .virtualColumn("virtual_column_test5", expression = stringToArray("foo,bar", ","))
        .columns("virtual_column_test",
                 "virtual_column_test2",
                 "virtual_column_test3",
                 //"virtual_column_test4",
                 "virtual_column_test5")
        .build()

//      println("=" * 80)
//      println(query.toDebugString)
//      println("=" * 80)

      val resultsF = query.execute()
      whenReady(resultsF) { results =>
        results.list[Json].foreach(println)
        assert(true)
      }

    }
  }

}
// scalastyle:on
