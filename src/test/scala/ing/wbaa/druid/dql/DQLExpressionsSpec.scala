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

import ing.wbaa.druid.{ DruidConfig, ScanQuery }
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions.{ GranularityType, Inline, Table }
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.expressions.{ CastType, Expression, LeftExpression, MathFunctions }
import ing.wbaa.druid.dql.expressions.ExpressionFunctions._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.generic.auto._
import org.scalatest.Inspectors
import org.scalatest.Assertion

import scala.concurrent.duration._
import scala.language.postfixOps

class DQLExpressionsSpec extends AnyWordSpec with Matchers with ScalaFutures with Inspectors {

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

  case class JoinedScanResult(
      channel: Option[String],
      cityName: Option[String],
      countryIsoCode: Option[String],
      user: Option[String],
      mapped_country_iso3_code: Option[String],
      mapped_country_name: Option[String]
  )

//  "DQL join query with expressions" should {
//    "lowercase and uppercase over countryIsoCode" in {
//      val query = baseScan
//        .from(
//          Table("wikipedia")
//            .join(
//              right = InlineDatasources.countryDatasource,
//              prefix = "mapped_country_",
//              (l, r) =>
//                lower(l("countryIsoCode")) === r("iso2_code_lowercase") and
//                upper(l("countryIsoCode")) === r("iso2_code_uppercase")
//            )
//        )
//        .build()
//
//      println(query.toDebugString)
//
//      val request = query.execute()
//      whenReady(request) { response =>
//        val resultList = response.list[JoinedScanResult]
//        resultList.size shouldBe numberOfResults
//
//        resultList.forall(_.mapped_country_iso3_code.isDefined) shouldBe true
//        resultList.forall(_.mapped_country_name.isDefined) shouldBe true
//
//        val resultSeries = response.series[JoinedScanResult]
//        resultSeries.size shouldBe numberOfResults
//      }
//    }
//
//  }

  case class Numbers(number: Double, right_number: Double)

  "DQL join query with math expressions" should {

//    def baseQuery(condition: (LeftPart, RightPart) => Expression): ScanQuery =
//      baseScan
//        .from(
//          InlineDatasources.numericDatasourceNegatives
//            .join(
//              right = InlineDatasources.numericDatasource,
//              prefix = "right_",
//              condition = condition
//            )
//        )
//        .columns("number", "right_number")
//        .build()

    def baseQuery(leftFun: LeftExpression => Expression, rightDim: String): ScanQuery =
      baseScan
        .from(
          InlineDatasources.numericDatasourceNegatives
            .join(
              right = InlineDatasources.numericDatasource,
              prefix = "right_",
              condition = (l, r) => leftFun(l("number")) === r(rightDim)
            )
        )
        .columns("number", "right_number")
        .build()

    case class TestScenario(
        name: String,
        leftPart: LeftExpression => Expression,
        expectedNumResults: Int,
        mathFunction: Double => Double
    )

    val scenarios = Seq(
      TestScenario(
        name = "abs",
        leftPart = leftExpr => MathFunctions.abs(leftExpr, CastType.Double),
        expectedNumResults = 4,
        mathFunction = math.abs
      ),
      TestScenario(
        name = "acos",
        leftPart = leftExpr => MathFunctions.acos(leftExpr, CastType.Double),
        expectedNumResults = 2,
        mathFunction = math.acos
      ),
      TestScenario(
        name = "asin",
        leftPart = leftExpr => MathFunctions.asin(leftExpr, CastType.Double),
        expectedNumResults = 2,
        mathFunction = math.asin
      ),
      TestScenario(
        name = "atan",
        leftPart = leftExpr => MathFunctions.atan(leftExpr, CastType.Double),
        expectedNumResults = 2,
        mathFunction = math.atan
      ),
      TestScenario(
        name = "atan2",
        leftPart =
          leftExpr => MathFunctions.atan2(leftExpr, leftExpr, CastType.Double, CastType.Double),
        expectedNumResults = 4,
        mathFunction = x => math.atan2(x, x)
      ),
      TestScenario(
        name = "cbrt",
        leftPart = leftExpr => MathFunctions.cbrt(leftExpr, CastType.Double),
        expectedNumResults = 2,
        mathFunction = math.cbrt
      ),
      TestScenario(
        name = "ceil",
        leftPart = leftExpr => MathFunctions.cbrt(leftExpr, CastType.Double),
        expectedNumResults = 1,
        mathFunction = math.ceil
      ),
      TestScenario(
        name = "copySign",
        leftPart =
          leftExpr => MathFunctions.copysign(leftExpr, leftExpr, CastType.Double, CastType.Double),
        expectedNumResults = 2,
        mathFunction = x => java.lang.Math.copySign(x, x)
      ),
      TestScenario(
        name = "cos",
        leftPart = leftExpr => MathFunctions.cos(leftExpr, CastType.Double),
        expectedNumResults = 4,
        mathFunction = math.cos
      ),
      TestScenario(
        name = "cot",
        leftPart = leftExpr => MathFunctions.cot(leftExpr, CastType.Double),
        expectedNumResults = 1,
        mathFunction = x => 1.0 / math.tan(x)
      )
//      TestScenario(
//        name = "div",
//        leftPart = leftExpr => MathFunctions.div(leftExpr, CastType.Double),
//        expectedNumResults = 2,
//        mathFunction = x => (x / 2).toInt
//      )
    )

    scenarios.foreach { scenario =>
      s"join using the `${scenario.name}` math function" in {
        val query = baseQuery(scenario.leftPart, scenario.name)
        println(query.toDebugString)
        val request = query.execute()

        whenReady(request) { response =>
          val numbers: List[Numbers] = response.list[Numbers]
          numbers.foreach(println)

          numbers.size shouldEqual scenario.expectedNumResults

          forAll(numbers) {
            case Numbers(number, right_number) =>
              scenario.mathFunction(number) shouldBe
              scenario.mathFunction(right_number)
          }
        }
      }
    }

  }

}
// scalastyle:on
