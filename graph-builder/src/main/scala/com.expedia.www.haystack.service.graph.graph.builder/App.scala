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
package com.expedia.www.haystack.service.graph.graph.builder

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.health.{HealthStatusController, UpdateHealthStatusFile}
import com.expedia.www.haystack.commons.kstreams.app.ManagedKafkaStreams
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.graph.builder.config.AppConfiguration
import com.expedia.www.haystack.service.graph.graph.builder.config.entities.{KafkaConfiguration, ServiceConfiguration}
import com.expedia.www.haystack.service.graph.graph.builder.service.resources.{GlobalServiceGraphResource, IsWorkingResource, LocalServiceGraphResource}
import com.expedia.www.haystack.service.graph.graph.builder.service.{HttpService, ManagedHttpService}
import com.expedia.www.haystack.service.graph.graph.builder.stream.{ServiceGraphStream, StreamSupplier}
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory

/**
  * Starting point for graph-builder application
  */
object App extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(App.getClass)

  var app: ManagedApplication = _

  def main(args: Array[String]): Unit = {
    val jmxReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    val appConfiguration = new AppConfiguration()
    val healthStatusController = new HealthStatusController
    healthStatusController.addListener(new UpdateHealthStatusFile(appConfiguration.healthStatusFilePath))

    try {
      // build kafka stream to create service graph
      // it ingests graph edges and create service graph out of it
      // graphs are stored as materialized ktable in stream state store
      val stream = createStream(appConfiguration.kafkaConfig, healthStatusController)

      // build http service to query current service graph
      // it performs interactive query on ktable
      val service = createService(appConfiguration.serviceConfig, stream, appConfiguration.kafkaConfig.producerTopic)

      // create managed application
      app = new ManagedApplication(
        new ManagedHttpService(service),
        new ManagedKafkaStreams(stream),
        jmxReporter)

      //add a shutdown hook
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = app.stop()
      })

      //start the application
      app.start()
    } catch {
      case ex: Exception =>
        LOGGER.error("Observed fatal exception while running the app", ex)
        if(app != null) app.stop()
        System.exit(1)
    }
  }

  @VisibleForTesting
  def createStream(kafkaConfig: KafkaConfiguration, healthStatusController: HealthStatusController): KafkaStreams = {
    // create kstream using application topology
    val streamsFactory = new StreamSupplier(
      new ServiceGraphStream(kafkaConfig),
      healthStatusController,
      kafkaConfig.streamsConfig,
      kafkaConfig.consumerTopic)

    // build kstream app
    streamsFactory.get()
  }

  @VisibleForTesting
  def createService(serviceConfig: ServiceConfiguration, stream: KafkaStreams, storeName: String): HttpService = {
    val servlets = Map(
      "/servicegraph/local" -> new LocalServiceGraphResource(stream, storeName),
      "/servicegraph" -> new GlobalServiceGraphResource(stream, storeName),
      "/isWorking" -> new IsWorkingResource
    )

    new HttpService(serviceConfig, servlets)
  }
}