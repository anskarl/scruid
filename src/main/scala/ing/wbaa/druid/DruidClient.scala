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
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.{ ActorMaterializer, FlowShape, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.java8.time._
import io.circe.parser.decode
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future, Promise }
import scala.util.{ Failure, Success, Try }

sealed trait DruidClient {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

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
  def actorSystem: ActorSystem
  def doQuery(q: DruidQuery): Future[DruidResponse]

}

class ClientSingleHost private (connectionFlow: ClientSingleHost.QueryConnectionFlow,
                                responseParsingTimeout: FiniteDuration,
                                url: String)(implicit system: ActorSystem)
    extends DruidClient
    with FailFastCirceSupport
    with JavaTimeDecoders {

  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

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

  override def doQuery(q: DruidQuery): Future[DruidResponse] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = url)
          .withEntity(entity.withContentType(`application/json`))
      }
      .flatMap(executeRequest(q.queryType))

  override def actorSystem: ActorSystem = this.system
}

object ClientSingleHost {

  type QueryConnectionFlow =
    Flow[(HttpRequest, HttpRequest), (Try[HttpResponse], HttpRequest), Http.HostConnectionPool]

  def apply(druidConfig: DruidConfig): DruidClient = {
    val druidHost = druidConfig.hosts.head

    ClientSingleHost(host = druidHost.host,
                     port = druidHost.port,
                     secure = druidConfig.secure,
                     responseParsingTimeout = druidConfig.responseParsingTimeout,
                     url = druidConfig.url)(druidConfig.system)
  }

  def apply(host: String,
            port: Int,
            secure: Boolean,
            responseParsingTimeout: FiniteDuration,
            url: String)(implicit system: ActorSystem): DruidClient = {

    val connectionFlow = createConnectionFlow(host, port, secure)

    new ClientSingleHost(connectionFlow, responseParsingTimeout, url)
  }

  private def createConnectionFlow(host: String, port: Int, secure: Boolean)(
      implicit system: ActorSystem
  ): Flow[(HttpRequest, HttpRequest), (Try[HttpResponse], HttpRequest), Http.HostConnectionPool] =
    if (secure) Http().cachedHostConnectionPoolHttps(host, port)
    else Http().cachedHostConnectionPool[HttpRequest](host, port)

}

class ClientWithLoadBalancer private (connectionFlow: ClientWithLoadBalancer.QueryConnectionFlow,
                                      responseParsingTimeout: FiniteDuration,
                                      url: String)(implicit val system: ActorSystem)
    extends DruidClient
    with FailFastCirceSupport
    with JavaTimeDecoders {

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor    = system.dispatcher
  private implicit val scheduler: Scheduler            = system.scheduler

  private val queue: SourceQueueWithComplete[(HttpRequest, Promise[HttpResponse])] =
    Source
      .queue[(HttpRequest, Promise[HttpResponse])](bufferSize = 2048, //Int.MaxValue,
                                                   overflowStrategy = OverflowStrategy.backpressure)
      .named("druid-client-queue")
      .via(connectionFlow)
      .toMat(Sink.foreach {
        case (Success(r), p) => p.success(r)
        case (Failure(e), p) => p.failure(e)
      })(Keep.left)
      .run()

  private def executeRequest(q: DruidQuery)(request: HttpRequest): Future[DruidResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    val responsePromise = Promise[HttpResponse]()

    queue
      .offer(request -> responsePromise)
      .flatMap {
        case QueueOfferResult.Enqueued =>
          responsePromise.future.flatMap { response =>
            handleResponse(response, q.queryType, responseParsingTimeout)
          }
        case QueueOfferResult.Dropped =>
          Future.failed[DruidResponse](new RuntimeException("Queue overflowed. Try again later."))
        case QueueOfferResult.Failure(ex) =>
          Future.failed[DruidResponse](ex)
        case QueueOfferResult.QueueClosed =>
          Future.failed[DruidResponse](
            new RuntimeException(
              "Queue was closed (pool shut down) while running the request. Try again later."
            )
          )
      }
  }

  override def doQuery(q: DruidQuery): Future[DruidResponse] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(method = HttpMethods.POST, uri = url)
          .withEntity(entity.withContentType(`application/json`))
      }
      .flatMap(
        request => akka.pattern.retry(() => executeRequest(q)(request), 200, 200.milliseconds)
      )

  override def actorSystem: ActorSystem = this.system
}

object ClientWithLoadBalancer {

  type QueryConnectionFlow =
    Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed]

  def apply(druidConfig: DruidConfig): DruidClient =
    ClientWithLoadBalancer(hosts = druidConfig.hosts,
                           secureConnection = druidConfig.secure,
                           responseParsingTimeout = druidConfig.responseParsingTimeout,
                           url = druidConfig.url)(druidConfig.system)

  def apply(hosts: Seq[QueryHost],
            secureConnection: Boolean,
            responseParsingTimeout: FiniteDuration,
            url: String)(implicit system: ActorSystem): DruidClient = {

    val connectionFlow = createConnectionFlow(hosts, secureConnection)

    new ClientWithLoadBalancer(connectionFlow, responseParsingTimeout, url)

  }

  private def balancer[In, Out](workers: Seq[Flow[In, Out, Any]]): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      val balancer = b.add(Balance[In](outputPorts = workers.size, waitForAllDownstreams = false))
      val merge    = b.add(Merge[Out](workers.size))

      workers.foreach { worker ⇒
        balancer ~> worker ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  private def createConnectionFlow(hosts: Seq[QueryHost], secureConnection: Boolean)(
      implicit system: ActorSystem
  ): QueryConnectionFlow = {

    val settings: ConnectionPoolSettings = ConnectionPoolSettings(system)
    val log: LoggingAdapter              = system.log

    val workers: Seq[Flow[(HttpRequest, Promise[HttpResponse]),
                          (Try[HttpResponse], Promise[HttpResponse]),
                          NotUsed]] = hosts.map { queryHost ⇒
      Flow[(HttpRequest, Promise[HttpResponse])]
        .log("scruid-load-balancer", _ => s"Sending query to ${queryHost.host}:${queryHost.port}")
        .via {

          if (secureConnection) {
            Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](
              host = queryHost.host,
              port = queryHost.port,
              settings = settings,
              log = log
            )
          } else {
            Http().cachedHostConnectionPool[Promise[HttpResponse]](
              host = queryHost.host,
              port = queryHost.port,
              settings = settings,
              log = log
            )
          }

        }
    }

    balancer[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse])](
      workers
    )
  }
}
