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

package ing.wbaa.druid

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.mdedetrich.akka.http.support.CirceHttpSupport
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import io.circe.java8.time._
import io.circe.parser.decode
import org.slf4j.LoggerFactory
import org.typelevel.jawn.AsyncParser

import scala.concurrent.Future

trait DruidClient extends CirceHttpSupport with JavaTimeDecoders {

  def actorSystem: ActorSystem
  def actorMaterializer: ActorMaterializer

  def isHealthy(): Future[Boolean]
  def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse]
  def doQueryAsStream(q: DruidQuery)(
      implicit druidConfig: DruidConfig
  ): Source[DruidResult, NotUsed]

}

class DruidHttpClient private (connectionFlow: DruidHttpClient.ConnectionFlowType)(
    implicit system: ActorSystem
) extends DruidClient {

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val materializer = ActorMaterializer()
  private implicit val ec           = system.dispatcher

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy(): Future[Boolean] = {
    val request = HttpRequest(HttpMethods.GET, uri = DruidConfig.HealthEndpoint)
    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .map(_.status == StatusCodes.OK)
      .recover { case _ => false }
  }

  override def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    createHttpRequest(q)
      .flatMap { request =>
        executeRequest(request).flatMap(handleResponse(q.queryType))
      }

  override def doQueryAsStream(
      q: DruidQuery
  )(implicit druidConfig: DruidConfig): Source[DruidResult, NotUsed] =
    Source
      .fromFuture(createHttpRequest(q))
      .via(connectionFlow)
      .flatMapConcat(handleResponseAsStream)

  private def handleResponse(
      queryType: QueryType
  )(response: HttpResponse)(implicit druidConfig: DruidConfig): Future[DruidResponse] = {
    val body =
      response.entity
        .toStrict(druidConfig.responseParsingTimeout)
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

  private def handleResponseAsStream(
      response: HttpResponse
  ): Source[DruidResult, NotUsed] =
    response.entity
      .withoutSizeLimit()
      .dataBytes
      .via(CirceStreamSupport.decode[DruidResult](AsyncParser.UnwrapArray))
      .mapMaterializedValue(_ => NotUsed)

  private def executeRequest(
      request: HttpRequest
  )(implicit druidConfig: DruidConfig): Future[HttpResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
  }

  private def createHttpRequest(
      q: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[HttpRequest] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }
}

object DruidHttpClient {

  type ConnectionFlowType = Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]]

  def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system = ActorSystem()
    val flow            = createConnectionFlow(druidConfig)

    new DruidHttpClient(flow)
  }

  private def createConnectionFlow(
      druidConfig: DruidConfig
  )(implicit actorSystem: ActorSystem): ConnectionFlowType =
    if (druidConfig.secure)
      Http().outgoingConnectionHttps(host = druidConfig.host, port = druidConfig.port)
    else Http().outgoingConnection(host = druidConfig.host, port = druidConfig.port)

}
