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

package ing.wbaa.druid.definitions

import io.circe.generic.extras._
import io.circe.generic.auto._
import ca.mrvisser.sealerate
import ing.wbaa.druid.{ Enum, EnumCodec, LowerCaseEnumStringEncoder }
// DO NOT REMOVE: required @ConfiguredJsonCodec
import QueryContext.circeConfig

@ConfiguredJsonCodec
case class QueryContext(
    // the following apply only to all queries
    timeout: Option[Long] = None,
    priority: Option[Int] = None,
    queryId: Option[String] = None,
    useCache: Option[Boolean] = None,
    populateCache: Option[Boolean] = None,
    useResultLevelCache: Option[Boolean] = None,
    populateResultLevelCache: Option[Boolean] = None,
    bySegment: Option[Boolean] = None,
    finalizeAggregationResults: Option[Boolean] = None,
    chunkPeriod: Option[String] = None,
    maxScatterGatherBytes: Option[Long] = None,
    maxQueuedBytes: Option[Long] = None,
    serializeDateTimeAsLong: Option[Boolean] = None,
    serializeDateTimeAsLongInner: Option[Boolean] = None,
    // the following apply only to time-series queries
    skipEmptyBuckets: Option[Boolean] = None,
    grandTotal: Option[Boolean] = None, //todo
    // the following apply only to top-n queries
    minTopNThreshold: Option[Int] = None,
    // the following apply only to group-by queries
    groupByStrategy: Option[GroupByStrategyType] = None,
    groupByIsSingleThreaded: Option[Boolean] = None,
    // Group-by V2
    maxMergingDictionarySize: Option[Long] = None,
    maxOnDiskStorage: Option[Long] = None,
    bufferGrouperInitialBuckets: Option[Int] = None,
    bufferGrouperMaxLoadFactor: Option[Double] = None,
    forceHashAggregation: Option[Boolean] = None,
    intermediateCombineDegree: Option[Int] = None,
    numParallelCombineThreads: Option[Int] = None,
    // todo check if it has any side-effect regarding how Scruid parses responses
    sortByDimsFirst: Option[Boolean] = None,
    forceLimitPushDown: Option[Boolean] = None,
    // Group-by V1
    maxIntermediateRows: Option[Int] = None,
    maxResults: Option[Int] = None,
    useOffheap: Option[Boolean] = None
)

object QueryContext {
  final val empty = QueryContext()

  implicit val circeConfig: Configuration = Configuration.default.copy(
    transformMemberNames = {
      case "finalizeAggregationResults" => "finalize"
      case other                        => other
    }
  )
}

sealed trait GroupByStrategyType extends Enum with LowerCaseEnumStringEncoder
object GroupByStrategyType extends EnumCodec[GroupByStrategyType] {
  case object V1 extends GroupByStrategyType
  case object V2 extends GroupByStrategyType
  val values: Set[GroupByStrategyType] = sealerate.values[GroupByStrategyType]
}
