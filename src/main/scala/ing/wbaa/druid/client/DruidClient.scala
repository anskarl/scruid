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

package ing.wbaa.druid.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ HttpResponse, StatusCodes }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import ing.wbaa.druid._
import io.circe.java8.time._
import org.mdedetrich.akka.http.support.CirceHttpSupport
import org.slf4j.LoggerFactory
import io.circe.parser.decode
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.typelevel.jawn.AsyncParser

import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.concurrent.duration.FiniteDuration

trait DruidClient extends CirceHttpSupport with JavaTimeDecoders {

  protected val logger = LoggerFactory.getLogger(getClass)

  def actorSystem: ActorSystem
  def actorMaterializer: ActorMaterializer
  def isHealthy(): Future[Boolean]
  def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse]
  def doQueryAsStream(q: DruidQuery)(
      implicit druidConfig: DruidConfig
  ): Source[DruidResult, NotUsed]

}

trait DruidResponseHandler {
  self: DruidClient =>

  protected def handleResponse(
      response: HttpResponse,
      queryType: QueryType,
      responseParsingTimeout: FiniteDuration
  )(implicit materializer: ActorMaterializer,
    ec: ExecutionContextExecutor): Future[DruidResponse] = {

    val body =
      response.entity
        .toStrict(responseParsingTimeout)
        .map(_.data.decodeString("UTF-8"))
    body.onComplete(b => logger.debug(s"Druid response: $b"))

    if (response.status != StatusCodes.OK) {
      body.flatMap { b =>
        Future.failed(new Exception(s"Got unexpected response from Druid: $b"))
      }
    } else {
      body
        .map(decode[List[DruidResult]])
        .map {
          case Left(error)  => throw new Exception(s"Unable to parse json response: $error")
          case Right(value) => DruidResponse(results = value, queryType = queryType)
        }
    }
  }

  protected def handleResponseAsStream(
      response: HttpResponse
  ): Source[DruidResult, NotUsed] =
    response.entity
      .withoutSizeLimit()
      .dataBytes
      .via(CirceStreamSupport.decode[DruidResult](AsyncParser.UnwrapArray))
      .mapMaterializedValue(_ => NotUsed)
}
