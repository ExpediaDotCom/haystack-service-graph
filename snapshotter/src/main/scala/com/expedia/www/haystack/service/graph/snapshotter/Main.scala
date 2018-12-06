/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.www.haystack.service.graph.snapshotter

import java.time.{Clock, Instant}

import com.expedia.www.haystack.service.graph.snapshot.store.StringStore
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpRequest}

object Main {
  val StringStoreClassRequiredMsg =
    "The first argument must specify the fully qualified class name of a class that implements StringStore"
  val ServiceGraphUrl = "http://apis/graph/servicegraph"
  val appConfiguration = new AppConfiguration()

  var logger: Logger = LoggerFactory.getLogger(Main.getClass)
  var clock: Clock = Clock.systemUTC()
  var factory: Factory = new Factory

  /** Main method
    * @param args specifies the class to run and its parameters.
    * ==args(0)==
    * The first parameter is the fully qualified class name of the implementation of
    * [[com.expedia.www.haystack.service.graph.snapshot.store.StringStore]] to run.
    * There are currently two implementations:
    *   - [[com.expedia.www.haystack.service.graph.snapshot.store.FileStore]]
    *   - [[com.expedia.www.haystack.service.graph.snapshot.store.S3Store]]
    * ==args(1+)==
    * The rest of the arguments are passed to the constructor of the class specified by args(0).
    * See the documentation in the build() method of the desired implementation for argument details.
    * ===Examples===
    * ====FileStore====
    * To run FileStore and use /var/snapshots for snapshot storage, the arguments would be:
    *   - com.expedia.www.haystack.service.graph.snapshot.store.FileStore
    *   - /var/snapshots
    * ====S3Store====
    * To run S3Store and use the "Haystack" bucket with subfolder "snapshots" for snapshot storage, and a batch size
    * of 10,000 when calling the S3 "listObjectsV2" API, the arguments would be:
    *   - com.expedia.www.haystack.service.graph.snapshot.store.S3Store
    *   - Haystack
    *   - snapshots
    *   - 10000
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error(StringStoreClassRequiredMsg)
    } else {
      val stringStore = instantiateStringStore(args)
      val now = clock.instant()
      val json = getCurrentServiceGraph(stringStore)
      storeServiceGraphInTheStringStore(stringStore, now, json)
      purgeOldSnapshots(stringStore, now)
    }
  }

  private def instantiateStringStore(args: Array[String]): StringStore = {
    def createStringStoreInstanceWithDefaultConstructor: StringStore = {
      val fullyQualifiedClassName = args(0)
      val klass = Class.forName(fullyQualifiedClassName)
      val instanceBuiltByDefaultConstructor = klass.newInstance().asInstanceOf[StringStore]
      instanceBuiltByDefaultConstructor
    }

    val stringStore = createStringStoreInstanceWithDefaultConstructor.build(args.drop(1))
    stringStore
  }

  private def getCurrentServiceGraph(stringStore: StringStore): String = {
    val request = factory.createHttpRequest(ServiceGraphUrl)
    val httpResponse = request.asString
    httpResponse.body
  }

  private def storeServiceGraphInTheStringStore(stringStore: StringStore,
                                                instant: Instant,
                                                json: String): AnyRef = {
    stringStore.write(instant, json)
  }

  private def purgeOldSnapshots(stringStore: StringStore,
                                instant: Instant): Integer = {
    stringStore.purge(instant.minusMillis(appConfiguration.purgeAgeMs))
  }
}

class Factory {
  def createHttpRequest(url: String): HttpRequest = {
    Http(url)
  }
}
