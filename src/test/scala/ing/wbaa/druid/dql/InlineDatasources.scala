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

import ing.wbaa.druid.definitions.Inline

object InlineDatasources {

  val countryData: Seq[List[String]] = Locale.getISOCountries.toList
    .map { code =>
      val locale = new Locale("en", code)
      List(code.toUpperCase, code.toLowerCase, locale.getISO3Country, locale.getDisplayCountry)
    }

  val countryDatasource: Inline =
    Inline(Seq("iso2_code_uppercase", "iso2_code_lowercase", "iso3_code", "name"), countryData)

  val numericData: Seq[List[Double]] = {
    //val sample: Seq[Double] = (-1.0 to 1.0 by 0.5).toSeq
    val sample: Seq[Double] = Seq(-1.0, -0.5, 0.5, 0.75, 1.0)

    sample
      .map { number =>
        List(
          number,
          math.abs(number),
          math.acos(number),
          math.asin(number),
          math.atan(number),
          math.atan2(number, number),
          math.cbrt(number),
          math.ceil(number),
          math.copySign(number, number),
          math.cos(number),
          math.cosh(number),
          1.0 / math.tan(number), // Druid `cot` function, i.e., trigonometric cotangent
          (number / 2).toInt, // integer division of number by 2
          math.exp(number),
          math.exp(number) - 1.0 //todo expm1, is  math.exp(number) - 1.0 or math.exp(number - 1.0) ???
        )
      }
  }

  val numericDimNames =
    Seq("number",
        "abs",
        "acos",
        "asin",
        "atan",
        "atan2",
        "cbrt",
        "ceil",
        "copySign",
        "cos",
        "cosh",
        "cot",
        "div",
        "exp",
        "expm1")

  val numericDatasource: Inline =
    Inline(numericDimNames, numericData.map(_.map(_.toString)))

  val numericDatasourceNegatives: Inline =
    Inline(numericDimNames, numericData.filter(_.head < 0.0).map(_.map(_.toString)))

  val numericDatasourcePositives: Inline =
    Inline(numericDimNames, numericData.filter(_.head > 0.0).map(_.map(_.toString)))
}
// scalastyle:on
