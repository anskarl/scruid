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

import com.typesafe.config.ConfigFactory
import ing.wbaa.druid.client.{ DruidCachedHttpClient, DruidClient, DruidClientConstructor }

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions
import scala.reflect.runtime.universe

/*
 * Druid API Config Immutable
 */
class DruidConfig(val host: String,
                  val port: Int,
                  val secure: Boolean,
                  val url: String,
                  val datasource: String,
                  val responseParsingTimeout: FiniteDuration,
                  val clientBackend: String) {
  def copy(
      host: String = this.host,
      port: Int = this.port,
      secure: Boolean = this.secure,
      url: String = this.url,
      datasource: String = this.datasource,
      responseParsingTimeout: FiniteDuration = this.responseParsingTimeout,
      clientBackend: String = this.clientBackend
  ): DruidConfig =
    new DruidConfig(host, port, secure, url, datasource, responseParsingTimeout, clientBackend)

  // todo
  //lazy val client: DruidClient = DruidHttpClient(this)
  lazy val client: DruidClient = loadClient(clientBackend)

  private def loadClient(name: String): DruidClient = {
    val runtimeMirror     = universe.runtimeMirror(getClass.getClassLoader)
    val module            = runtimeMirror.staticModule(name)
    val obj               = runtimeMirror.reflectModule(module)
    val clientConstructor = obj.instance.asInstanceOf[DruidClientConstructor]
    clientConstructor(this)

  }
}

object DruidConfig {

  private val config = ConfigFactory.load()

  private val druidConfig = config.getConfig("druid")

  val HealthEndpoint = "/status/health"

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val DefaultConfig: DruidConfig = apply()

  def apply(host: String = druidConfig.getString("host"),
            port: Int = druidConfig.getInt("port"),
            secure: Boolean = druidConfig.getBoolean("secure"),
            url: String = druidConfig.getString("url"),
            datasource: String = druidConfig.getString("datasource"),
            responseParsingTimeout: FiniteDuration =
              druidConfig.getDuration("response-parsing-timeout"),
            clientBackend: String = druidConfig.getString("client-backend")): DruidConfig =
    new DruidConfig(host, port, secure, url, datasource, responseParsingTimeout, clientBackend)

}
