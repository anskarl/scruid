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
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.{ ActorMaterializer, FlowShape, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl._

import ing.wbaa.druid.{ DruidConfig, DruidQuery, DruidResponse, DruidResult, QueryHost }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future, Promise }
import scala.util.{ Failure, Success, Try }

class DruidLoadBalancerHttpClient private (
    connectionFlow: DruidLoadBalancerHttpClient.QueryConnectionFlowType,
    responseParsingTimeout: FiniteDuration,
    url: String
)(implicit val system: ActorSystem)
    extends DruidClient
    with DruidResponseHandler {

  logger.info("!!! DruidLoadBalancerHttpClient")

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor    = system.dispatcher
  private implicit val scheduler: Scheduler            = system.scheduler

  private val queue: SourceQueueWithComplete[(HttpRequest, Promise[HttpResponse])] =
    Source
      .queue[(HttpRequest, Promise[HttpResponse])](bufferSize = 32768, // Int.MaxValue
                                                   overflowStrategy = OverflowStrategy.backpressure)
      .named("druid-client-queue")
      .via(connectionFlow)
      .toMat(Sink.foreach {
        case (Success(r), p) => p.success(r)
        case (Failure(e), p) => p.failure(e)
      })(Keep.left)
      .run()

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy(): Future[Boolean] = {
    val responsePromise = Promise[HttpResponse]()

    val request = HttpRequest(HttpMethods.GET, uri = DruidConfig.HealthEndpoint)

    Source
      .single(request -> responsePromise)
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
      .flatMap { entity =>
        val request = HttpRequest(HttpMethods.POST, url)
          .withEntity(entity.withContentType(`application/json`))

        akka.pattern.retry(() => executeRequest(q)(request),
                           druidConfig.hosts.size * 10,
                           10.milliseconds) // todo

      }

  override def doQueryAsStream(
      q: DruidQuery
  )(implicit druidConfig: DruidConfig): Source[DruidResult, NotUsed] = {
    val responsePromise = Promise[HttpResponse]()

    Source
      .fromFuture(createHttpRequest(q).map(request => request -> responsePromise))
      .via(connectionFlow)
      .flatMapConcat {
        case (Success(response), _) => handleResponseAsStream(response)
        case (Failure(ex), _)       => Source.failed(ex)
      }
  }

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

object DruidLoadBalancerHttpClient extends DruidClientConstructor {

  type QueryConnectionFlowType =
    Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed]

  override val supportsMultipleBrokers: Boolean = true

  override def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system = druidConfig.system
    val connectionFlow  = createConnectionFlow(druidConfig.hosts, druidConfig.secure)

    new DruidLoadBalancerHttpClient(connectionFlow,
                                    druidConfig.responseParsingTimeout,
                                    druidConfig.url)
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
  ): QueryConnectionFlowType = {

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
