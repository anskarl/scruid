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

import ing.wbaa.druid.definitions._

case class Dim(name: String,
               outputNameOpt: Option[String] = None,
               outputTypeOpt: Option[String] = None,
               extractionFnOpt: Option[ExtractionFn] = None) {

  def alias(name: String): Dim =
    copy(outputNameOpt = Option(name))

  def as(name: String): Dim = alias(name)

  def cast(outputType: String): Dim = {
    val uppercase = outputType.toUpperCase
    require(Dim.ValidNames.contains(uppercase))
    copy(outputTypeOpt = Option(uppercase))
  }

  def toFloat: Dim = cast("FLOAT")
  def toLong: Dim  = cast("LONG")

  def extractionFn(extractionFn: ExtractionFn): Dim =
    copy(extractionFnOpt = Option(extractionFn))

  protected[dql] def build(): Dimension =
    extractionFnOpt match {
      case Some(extractionFn) =>
        ExtractionDimension(name, outputNameOpt, outputTypeOpt, extractionFn)

      case None =>
        DefaultDimension(name, outputNameOpt, outputTypeOpt)
    }
}

object Dim {
  private final val ValidNames = Set("STRING", "LONG", "FLOAT")
}
