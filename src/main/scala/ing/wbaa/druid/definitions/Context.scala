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

sealed trait Context {
  val timeout: Option[Long] = None
  val priority: Option[Int] = None
  val queryId: Option[String] = None
  val useCache: Option[Boolean] = None
  val populateCache: Option[Boolean] = None
  val useResultLevelCache: Option[Boolean] = None
  val populateResultLevelCache: Option[Boolean] = None
  val bySegment: Option[Boolean] = None
  val finalise: Option[Boolean] = None // todo rename to finalize
  val chunkPeriod: Option[String] = None
  val maxScatterGatherBytes: Option[Long] = None
  val maxQueuedBytes: Option[Long] = None
  val serializeDateTimeAsLong: Option[Boolean] = None
  val serializeDateTimeAsLongInner: Option[Boolean] = None
}

case class TimeseriesContext(
  skipEmptyBuckets: Option[Boolean] = None,
  grandTotal: Option[Boolean] = None
) extends Context

case class TopNContext(
  minTopNThreshold: Option[Int] = None
) extends Context

case class GroupByContext(
  maxMergingDictionarySize: Option[Long] = None,
  maxOnDiskStorage: Option[Long] = None,
  groupByStrategy: String = "v2",
  groupByIsSingleThreaded: Option[Boolean] = None,
  bufferGrouperInitialBuckets: Option[Int] = None,
  bufferGrouperMaxLoadFactor: Option[Double] = None,
  forceHashAggregation: Option[Boolean] = None,
  intermediateCombineDegree: Option[Int] = None,
  numParallelCombineThreads: Option[Int] = None,
  sortByDimsFirst: Option[Boolean] = None,
  forceLimitPushDown: Option[Boolean] = None,
) extends Context


case class Ctx(
  // all
  timeout: Option[Long] = None,
  priority: Option[Int] = None,
  queryId: Option[String] = None,
  useCache: Option[Boolean] = None,
  populateCache: Option[Boolean] = None,
  useResultLevelCache: Option[Boolean] = None,
  populateResultLevelCache: Option[Boolean] = None,
  bySegment: Option[Boolean] = None,
  finalise: Option[Boolean] = None, // todo rename to finalize
  chunkPeriod: Option[String] = None,
  maxScatterGatherBytes: Option[Long] = None,
  maxQueuedBytes: Option[Long] = None,
  serializeDateTimeAsLong: Option[Boolean] = None,
  serializeDateTimeAsLongInner: Option[Boolean] = None,
  // ts
  skipEmptyBuckets: Option[Boolean] = None,
  grandTotal: Option[Boolean] = None,
  // topN
  minTopNThreshold: Option[Int] = None,
  // GroupBy
  maxMergingDictionarySize: Option[Long] = None,
  maxOnDiskStorage: Option[Long] = None,
  groupByIsSingleThreaded: Option[Boolean] = None,
  bufferGrouperInitialBuckets: Option[Int] = None,
  bufferGrouperMaxLoadFactor: Option[Double] = None,
  forceHashAggregation: Option[Boolean] = None,
  intermediateCombineDegree: Option[Int] = None,
  numParallelCombineThreads: Option[Int] = None,
  sortByDimsFirst: Option[Boolean] = None,
  forceLimitPushDown: Option[Boolean] = None
)