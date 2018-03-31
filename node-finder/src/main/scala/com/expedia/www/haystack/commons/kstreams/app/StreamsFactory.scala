package com.expedia.www.haystack.commons.kstreams.app

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

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

import org.slf4j.LoggerFactory

import scala.util.Try

class StreamsFactory(topologySupplier: Supplier[Topology], streamsConfig: StreamsConfig, consumerTopicName: Option[String]) {

  require(topologySupplier != null, "streamsBuilder is required")
  require(streamsConfig != null, "streamsConfig is required")

  def this(streamsSupplier: Supplier[Topology], streamsConfig: StreamsConfig) = this(streamsSupplier, streamsConfig, None)

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamsFactory])

  def create(listener: StateChangeListener): ManagedLifeCycle = {
    checkConsumerTopic()

    val streams = new KafkaStreams(topologySupplier.get(), streamsConfig)
    streams.setStateListener(listener)
    streams.setUncaughtExceptionHandler(listener)
    streams.cleanUp()
    new ManagedKafkaStreams(streams)
  }

  /*
  def create(runner: StreamsRunner): ManagedLifeCycle = {
    new ManagedLifeCycle {
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def isRunning: Boolean = true
    }
  }
  */

  def checkConsumerTopic(): Unit = {
    if (consumerTopicName.nonEmpty) {
      LOGGER.info(s"checking for the consumer topic ${consumerTopicName.get}")
      val adminClient = AdminClient.create(getBootstrapProperties)
      try {
        val present = adminClient.listTopics().names().get().contains(consumerTopicName.get)
        if (!present) {
          throw new ConsumerTopicNotPresentException(s"Topic $consumerTopicName is not present")
        }
      }
      finally {
        Try(adminClient.close(5, TimeUnit.SECONDS))
      }
    }
  }

  def getBootstrapProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
      streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
    properties
  }

  class ConsumerTopicNotPresentException(message: String) extends RuntimeException(message) {
  }

}

