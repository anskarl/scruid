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
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import ing.wbaa.druid.{ DruidConfig, DruidQuery, DruidResponse, DruidResult, QueryType }

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class DruidCachedHttpClient private (connectionFlow: DruidCachedHttpClient.QueryConnectionFlowType,
                                     responseParsingTimeout: FiniteDuration,
                                     url: String)(implicit system: ActorSystem)
    extends DruidClient
    with DruidResponseHandler {

  private implicit val materializer = ActorMaterializer()
  private implicit val ec           = system.dispatcher

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy(): Future[Boolean] = {
    val request = HttpRequest(HttpMethods.GET, uri = DruidConfig.HealthEndpoint)

    Source
      .single(request)
      .mapAsync(1)(req => Future((req, req)))
      .via(connectionFlow)
      .runWith(Sink.head)
      .flatMap {
        case (Success(response), _) => Future(response.status == StatusCodes.OK)
        case (Failure(ex), _)       => Future.failed(ex)
      }
      .recover { case _ => false }

  }

  override def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = url)
          .withEntity(entity.withContentType(`application/json`))
      }
      .flatMap(executeRequest(q.queryType)(_))

  override def doQueryAsStream(q: DruidQuery)(
      implicit druidConfig: DruidConfig
  ): Source[DruidResult, NotUsed] =
    Source
      .fromFuture(createHttpRequest(q))
      .mapAsync(1)(req => Future((req, req)))
      .via(connectionFlow)
      .flatMapConcat {
        case (Success(response), _) => handleResponseAsStream(response)
        case (Failure(ex), _)       => Source.failed(ex)
      }

  private def executeRequest(
      queryType: QueryType
  )(request: HttpRequest): Future[DruidResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    Source
      .single(request)
      .mapAsync(1)(req => Future((req, req)))
      .via(connectionFlow)
      .runWith(Sink.head)
      .flatMap {
        case (Success(response), _) => handleResponse(response, queryType, responseParsingTimeout)
        case (Failure(ex), _)       => Future.failed(ex)
      }
  }

  def createHttpRequest(
      q: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[HttpRequest] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }

}

object DruidCachedHttpClient {

  type QueryConnectionFlowType =
    Flow[(HttpRequest, HttpRequest), (Try[HttpResponse], HttpRequest), Http.HostConnectionPool]

  def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system = ActorSystem()
    val connectionFlow =
      createConnectionFlow(druidConfig.host, druidConfig.port, druidConfig.secure)

    new DruidCachedHttpClient(connectionFlow, druidConfig.responseParsingTimeout, druidConfig.url)
  }

  private def createConnectionFlow(host: String, port: Int, secure: Boolean)(
      implicit system: ActorSystem
  ): Flow[(HttpRequest, HttpRequest), (Try[HttpResponse], HttpRequest), Http.HostConnectionPool] =
    if (secure) Http().cachedHostConnectionPoolHttps(host, port)
    else Http().cachedHostConnectionPool[HttpRequest](host, port)
}
