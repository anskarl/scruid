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

import ing.wbaa.druid.definitions.Inline
import java.util.Locale

object CountryCodeData {

  final val data: Seq[List[String]] = Locale.getISOCountries.toList
    .map { code =>
      val locale = new Locale("en", code)
      List(code.toUpperCase, code.toLowerCase, locale.getISO3Country, locale.getDisplayCountry)
    }

  final val datasource: Inline =
    Inline(Seq("iso2_code_uppercase", "iso2_code_lowercase", "iso3_code", "name"), data)
}
