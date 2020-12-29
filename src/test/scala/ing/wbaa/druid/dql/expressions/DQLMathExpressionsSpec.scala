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

import akka.stream.ActorMaterializer
import ing.wbaa.druid.{ DruidConfig, ScanQuery }
import ing.wbaa.druid.client.DruidHttpClient
import ing.wbaa.druid.definitions.{ GranularityType, Inline }
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.expressions.ExpressionFunctions._
import io.circe.generic.auto._
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._
import scala.language.postfixOps

class DQLMathExpressionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Inspectors
    with DQLMathExpressionsScenarios {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  implicit val config: DruidConfig = DruidConfig(clientBackend = classOf[DruidHttpClient])

  implicit val mat: ActorMaterializer = config.client.actorMaterializer

  "DQL join inline datasources over the result of math function" should {

    for { scenario <- scenarios } {
      s"join when using function `${scenario.functionName}`" in {

        val request = baseQuery(scenario.leftPart, scenario.functionName).execute()

        whenReady(request) { response =>
          val resultingNumbers: List[Numbers] = response.list[Numbers]

          resultingNumbers.size shouldEqual scenario.expectedNumResults

          forAll(resultingNumbers) {
            case Numbers(number, right_number) =>
              // apply the current math function to dimension `number` of the left part
              val leftResult = scenario.mathFunction(number)

              // apply the current math function to dimension `number` of the joined right part
              val rightResult = scenario.mathFunction(right_number)

              // both results should be equal
              leftResult shouldBe rightResult
          }
        }
      }
    }
  }
}

private[expressions] trait DQLMathExpressionsScenarios {

  /**
    * Helper class to define a join test scenario using some math expression
    *
    * @param functionName the name of the math function to test
    * @param leftPart a function that gives the expected expression from a left side druid expression
    * @param expectedNumResults the expected number of results
    * @param mathFunction the math function that takes a Double and gives the expected result
    * @tparam N the type of the expected result
    */
  case class MathExpressionTestScenario[N](
      functionName: String,
      leftPart: LeftExpression => Expression,
      expectedNumResults: Int,
      mathFunction: Double => N
  )

  case class Numbers(number: Double, right_number: Double)

  protected def baseQuery(leftFun: LeftExpression => Expression, rightDim: String): ScanQuery =
    DQL
      .scan()
      .granularity(GranularityType.All)
      .interval("0000/4000")
      .batchSize(10)
      .limit(100)
      .from(
        numericDatasourceNegatives
          .join(
            right = numericDatasource,
            prefix = "right_",
            condition = (l, r) => leftFun(l("number")) === r(rightDim)
          )
      )
      .columns("number", "right_number")
      .build()

  final val scenarios: List[MathExpressionTestScenario[_]] = List(
    MathExpressionTestScenario(
      functionName = "abs",
      leftPart = leftExpr => abs(leftExpr, CastType.Double),
      expectedNumResults = 4,
      mathFunction = math.abs
    ),
    MathExpressionTestScenario(
      functionName = "acos",
      leftPart = leftExpr => acos(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.acos
    ),
    MathExpressionTestScenario(
      functionName = "asin",
      leftPart = leftExpr => asin(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.asin
    ),
    MathExpressionTestScenario(
      functionName = "atan",
      leftPart = leftExpr => atan(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.atan
    ),
    MathExpressionTestScenario(
      functionName = "atan2",
      leftPart = leftExpr => atan2(leftExpr, leftExpr, CastType.Double, CastType.Double),
      expectedNumResults = 4,
      mathFunction = x => math.atan2(x, x)
    ),
    MathExpressionTestScenario(
      functionName = "cbrt",
      leftPart = leftExpr => cbrt(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.cbrt
    ),
    MathExpressionTestScenario(
      functionName = "ceil",
      leftPart = leftExpr => ceil(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.ceil
    ),
    MathExpressionTestScenario(
      functionName = "copySign",
      leftPart = leftExpr => copysign(leftExpr, leftExpr, CastType.Double, CastType.Double),
      expectedNumResults = 2,
      mathFunction = x => java.lang.Math.copySign(x, x)
    ),
    MathExpressionTestScenario(
      functionName = "cos",
      leftPart = leftExpr => cos(leftExpr, CastType.Double),
      expectedNumResults = 4,
      mathFunction = math.cos
    ),
    MathExpressionTestScenario(
      functionName = "cosh",
      leftPart = leftExpr => cosh(leftExpr, CastType.Double),
      expectedNumResults = 4,
      mathFunction = math.cosh
    ),
    MathExpressionTestScenario(
      functionName = "cot",
      leftPart = leftExpr => cot(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = x => math.cos(x) / math.sin(x)
    ),
    MathExpressionTestScenario(
      functionName = "div",
      leftPart = leftExpr => div(lit(1), leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = x => (1 / x).toLong
    ),
    MathExpressionTestScenario(
      functionName = "exp",
      leftPart = leftExpr => exp(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.exp
    ),
    MathExpressionTestScenario(
      functionName = "expm1",
      leftPart = leftExpr => expm1(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = math.expm1
    ),
    MathExpressionTestScenario(
      functionName = "floor",
      leftPart = leftExpr => floor(leftExpr, CastType.Double),
      expectedNumResults = 4,
      mathFunction = math.floor
    ),
    MathExpressionTestScenario(
      functionName = "getExponent",
      leftPart = leftExpr => getExponent(leftExpr, CastType.Double),
      expectedNumResults = 5,
      mathFunction = x => java.lang.Math.getExponent(x).toLong
    ),
    MathExpressionTestScenario(
      functionName = "hypot",
      leftPart = leftExpr => hypot(leftExpr, leftExpr, CastType.Double, CastType.Double),
      expectedNumResults = 4,
      mathFunction = x => math.hypot(x, x)
    ),
    // Note: cannot use negatives to compute log, log10 and log1p
    MathExpressionTestScenario(
      functionName = "log",
      leftPart = leftExpr => log(abs(leftExpr, CastType.Double)),
      expectedNumResults = 4,
      mathFunction = x => math.log(math.abs(x))
    ),
    MathExpressionTestScenario(
      functionName = "log10",
      leftPart = leftExpr => log10(abs(leftExpr, CastType.Double)),
      expectedNumResults = 4,
      mathFunction = x => math.log10(math.abs(x))
    ),
    MathExpressionTestScenario(
      functionName = "log1p",
      leftPart = leftExpr => log1p(abs(leftExpr, CastType.Double)),
      expectedNumResults = 4,
      mathFunction = x => math.log1p(math.abs(x))
    ),
    MathExpressionTestScenario(
      functionName = "max",
      leftPart = leftExpr =>
        max(cast(leftExpr, CastType.Double),
            cast(leftExpr, CastType.Double) + cast(leftExpr, CastType.Double)),
      expectedNumResults = 2,
      mathFunction = x => math.max(x, x + x)
    ),
    MathExpressionTestScenario(
      functionName = "min",
      leftPart = leftExpr =>
        min(cast(leftExpr, CastType.Double),
            cast(leftExpr, CastType.Double) + cast(leftExpr, CastType.Double)),
      expectedNumResults = 2,
      mathFunction = x => math.min(x, x + x)
    ),
    MathExpressionTestScenario(
      functionName = "nextAfter",
      leftPart = leftExpr =>
        nextAfter(cast(leftExpr, CastType.Double),
                  cast(leftExpr, CastType.Double) + cast(leftExpr, CastType.Double)),
      expectedNumResults = 2,
      mathFunction = x => java.lang.Math.nextAfter(x, x + x)
    ),
    MathExpressionTestScenario(
      functionName = "nextUp",
      leftPart = leftExpr => nextUp(cast(leftExpr, CastType.Double)),
      expectedNumResults = 2,
      mathFunction = x => java.lang.Math.nextUp(x)
    ),
    MathExpressionTestScenario(
      functionName = "pi",
      leftPart = _ => pi,
      expectedNumResults = 10,
      mathFunction = _ => math.Pi
    ),
    MathExpressionTestScenario(
      functionName = "pow",
      leftPart =
        leftExpr => pow(abs(leftExpr.cast(CastType.Double)), leftExpr.cast(CastType.Double)),
      expectedNumResults = 3,
      mathFunction = number => math.pow(math.abs(number), number)
    ),
    MathExpressionTestScenario(
      functionName = "remainder",
      leftPart = leftExpr =>
        remainder(leftExpr.cast("DOUBLE"), leftExpr.cast("DOUBLE") + leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = x => math.IEEEremainder(x, x + x)
    ),
    MathExpressionTestScenario(
      functionName = "rint",
      leftPart = leftExpr => rint(leftExpr, CastType.Double),
      expectedNumResults = 2,
      mathFunction = number => math.rint(number)
    ),
    MathExpressionTestScenario(
      functionName = "round",
      leftPart = leftExpr => round(leftExpr.cast("DOUBLE"), 1),
      expectedNumResults = 2,
      mathFunction =
        number => BigDecimal.decimal(number).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    ),
    MathExpressionTestScenario(
      functionName = "scalb",
      leftPart = leftExpr => scalb(leftExpr.cast("DOUBLE"), 2),
      expectedNumResults = 2,
      mathFunction = number => java.lang.Math.scalb(number, 2)
    ),
    MathExpressionTestScenario(
      functionName = "signum",
      leftPart = leftExpr => signum(leftExpr.cast("DOUBLE")),
      expectedNumResults = 4,
      mathFunction = number => math.signum(number)
    ),
    MathExpressionTestScenario(
      functionName = "sin",
      leftPart = leftExpr => sin(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.sin(number)
    ),
    MathExpressionTestScenario(
      functionName = "sinh",
      leftPart = leftExpr => sinh(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.sinh(number)
    ),
    MathExpressionTestScenario(
      functionName = "sqrt",
      leftPart = leftExpr => sqrt(abs(leftExpr.cast("DOUBLE"))),
      expectedNumResults = 4,
      mathFunction = number => math.sqrt(math.abs(number))
    ),
    MathExpressionTestScenario(
      functionName = "tan",
      leftPart = leftExpr => tan(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.tan(number)
    ),
    MathExpressionTestScenario(
      functionName = "tanh",
      leftPart = leftExpr => tanh(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.tanh(number)
    ),
    MathExpressionTestScenario(
      functionName = "todegrees",
      leftPart = leftExpr => todegrees(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.toDegrees(number)
    ),
    MathExpressionTestScenario(
      functionName = "toradians",
      leftPart = leftExpr => toradians(leftExpr.cast("DOUBLE")),
      expectedNumResults = 2,
      mathFunction = number => math.toRadians(number)
    ),
    MathExpressionTestScenario(
      functionName = "ulp",
      leftPart = leftExpr => ulp(leftExpr.cast("DOUBLE")),
      expectedNumResults = 5,
      mathFunction = number => math.ulp(number)
    )
  )

  val numericDataOrigin: Seq[Double]          = Seq(-1.0, -0.5, 0.5, 0.75, 1.0)
  val numericDataOriginNegatives: Seq[Double] = numericDataOrigin.filter(_ < 0.0)

  val numericDimNames: Seq[String] = "number" :: scenarios.map(_.functionName)
  val numericData =
    numericDataOrigin.map(number => number :: scenarios.map(_.mathFunction(number)))

  val numericDatasource: Inline =
    Inline(numericDimNames, numericData.map(_.map(_.toString)))

  val numericDatasourceNegatives: Inline =
    Inline(numericDimNames,
           numericData.filter(_.head.asInstanceOf[Double] < 0.0).map(_.map(_.toString)))

  val numericDatasourcePositives: Inline =
    Inline(numericDimNames,
           numericData.filter(_.head.asInstanceOf[Double] > 0.0).map(_.map(_.toString)))
}
