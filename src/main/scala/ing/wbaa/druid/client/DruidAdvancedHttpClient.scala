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
import akka.pattern.retry
import com.typesafe.config.{ Config, ConfigException, ConfigFactory }
import ing.wbaa.druid.{ DruidConfig, DruidQuery, DruidResponse, DruidResult, QueryHost }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future, Promise }
import scala.util.{ Failure, Success, Try }

class DruidAdvancedHttpClient private (
    connectionFlow: DruidAdvancedHttpClient.QueryConnectionFlowType,
    flowsPerBroker: Map[QueryHost, DruidAdvancedHttpClient.QueryConnectionFlowType],
    responseParsingTimeout: FiniteDuration,
    url: String,
    bufferSize: Int,
    bufferOverflowStrategy: OverflowStrategy,
    requestRetriesPerHost: Int,
    requestRetryDelay: FiniteDuration
)(implicit val system: ActorSystem)
    extends DruidClient
    with DruidResponseHandler {

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor    = system.dispatcher
  private implicit val scheduler: Scheduler            = system.scheduler

  private val queue: SourceQueueWithComplete[(HttpRequest, Promise[HttpResponse])] =
    Source
      .queue[(HttpRequest, Promise[HttpResponse])](bufferSize, bufferOverflowStrategy)
      .named("druid-client-queue")
      .via(connectionFlow)
      .toMat(Sink.foreach {
        case (Success(r), p) => p.success(r)
        case (Failure(e), p) => p.failure(e)
      })(Keep.left)
      .run()

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy()(implicit druidConfig: DruidConfig): Future[Boolean] =
    healthCheck.map(_.forall { case (_, isHealthyBroker) => isHealthyBroker })

  override def healthCheck(implicit druidConfig: DruidConfig): Future[Map[QueryHost, Boolean]] = {

    val request = HttpRequest(HttpMethods.GET, uri = druidConfig.healthEndpoint)

    val checksF = Future.sequence {
      flowsPerBroker.map {
        case (broker, flow) =>
          val responsePromise = Promise[HttpResponse]()

          Source
            .single(request -> responsePromise)
            .via(flow)
            .runWith(Sink.head)
            .flatMap {
              case (Success(response), _) => Future(broker -> (response.status == StatusCodes.OK))
              case (Failure(ex), _)       => Future.failed(ex)
            }
            .recover { case _ => broker -> false }
      }
    }

    checksF.map(_.toMap)
  }

  override def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    Marshal(q)
      .to[RequestEntity]
      .flatMap { entity =>
        val request = HttpRequest(HttpMethods.POST, url)
          .withEntity(entity.withContentType(`application/json`))

        retry(
          () => executeRequest(q)(request),
          druidConfig.hosts.size * requestRetriesPerHost,
          requestRetryDelay
        )

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

object DruidAdvancedHttpClient extends DruidClientConstructor {

  object Parameters {
    final val DruidAdvancedHttpClient = "druid-advanced-http-client"
    final val ConnectionPoolSettings  = "host-connection-pool"
    final val QueueSize               = "queue-size"
    final val QueueOverflowStrategy   = "queue-overflow-strategy"
    final val RequestRetries          = "request-retries-per-host"
    final val RequestRetryDelay       = "retry-delay"
  }

  type QueryConnectionIn       = (HttpRequest, Promise[HttpResponse])
  type QueryConnectionOut      = (Try[HttpResponse], Promise[HttpResponse])
  type QueryConnectionFlowType = Flow[QueryConnectionIn, QueryConnectionOut, NotUsed]

  override val supportsMultipleBrokers: Boolean = true

  override def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system = druidConfig.system
    val clientConfig    = druidConfig.clientConfig.getConfig(Parameters.DruidAdvancedHttpClient)

    val akkaHostConnectionPoolConf = ConfigFactory.load("akka.http.host-connection-pool")

    val poolConfig = Try(clientConfig.getConfig(Parameters.ConnectionPoolSettings))
      .map { conf =>
        conf
          .atPath("akka.http.host-connection-pool")
          .withFallback(akkaHostConnectionPoolConf)
      }
      .getOrElse(akkaHostConnectionPoolConf)

    val flowsPerBroker = createConnectionFlows(druidConfig.hosts, druidConfig.secure, poolConfig)
    val connectionFlow = createLoadBalanceFlow(flowsPerBroker)

    val bufferSize = clientConfig.getInt(Parameters.QueueSize)

    val bufferOverflowStrategy = clientConfig.getString(Parameters.QueueOverflowStrategy) match {
      case "DropHead"     => OverflowStrategy.dropHead
      case "DropTail"     => OverflowStrategy.dropTail
      case "DropBuffer"   => OverflowStrategy.dropBuffer
      case "DropNew"      => OverflowStrategy.dropNew
      case "Fail"         => OverflowStrategy.fail
      case "Backpressure" => OverflowStrategy.backpressure
      case name =>
        throw new ConfigException.Generic(
          s"Unknown overflow strategy ($name) for client config parameter '${Parameters.QueueOverflowStrategy}'"
        )
    }

    val requestRetries = clientConfig.getInt(Parameters.RequestRetries)

    val requestRetryDelay = clientConfig
      .getDuration(Parameters.RequestRetryDelay)
      .toMillis
      .milliseconds

    new DruidAdvancedHttpClient(connectionFlow,
                                flowsPerBroker,
                                druidConfig.responseParsingTimeout,
                                druidConfig.url,
                                bufferSize,
                                bufferOverflowStrategy,
                                requestRetries,
                                requestRetryDelay)
  }

  private def balancer[In, Out](brokers: Iterable[Flow[In, Out, Any]]): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      val balancer = b.add(Balance[In](outputPorts = brokers.size, waitForAllDownstreams = false))
      val merge    = b.add(Merge[Out](brokers.size))

      brokers.foreach(broker ⇒ balancer ~> broker ~> merge)

      FlowShape(balancer.in, merge.out)
    })
  }

  private def createLoadBalanceFlow(brokerFlows: Map[QueryHost, QueryConnectionFlowType])(
      implicit system: ActorSystem
  ): QueryConnectionFlowType = {

    val flows = brokerFlows.values

    if (flows.size > 1) balancer[QueryConnectionIn, QueryConnectionOut](flows) else flows.head

  }

  private def createConnectionFlows(
      hosts: Seq[QueryHost],
      secureConnection: Boolean,
      connectionPoolConfig: Config
  )(implicit system: ActorSystem): Map[QueryHost, QueryConnectionFlowType] = {

    require(hosts.nonEmpty)

    val settings: ConnectionPoolSettings = ConnectionPoolSettings(connectionPoolConfig)
    val log: LoggingAdapter              = system.log

    hosts.map { queryHost ⇒
      val flow = Flow[QueryConnectionIn]
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

      queryHost -> flow
    }.toMap
  }
}
