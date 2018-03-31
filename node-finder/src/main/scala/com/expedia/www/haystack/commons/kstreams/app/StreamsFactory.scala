package com.expedia.www.haystack.commons.kstreams.app

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.slf4j.LoggerFactory

import scala.util.Try

class StreamsFactory(streamsSupplier: Supplier[Topology], streamsConfig: StreamsConfig, consumerTopicName: Option[String]) {

  require(streamsSupplier != null, "streamsBuilder is required")
  require(streamsConfig != null, "streamsConfig is required")

  def this(streamsSupplier: Supplier[Topology], streamsConfig: StreamsConfig) = this(streamsSupplier, streamsConfig, None)

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamsFactory])

  def create(runner: StreamsRunner): ManagedLifeCycle = {
//    checkConsumerTopic()
//
//    val streams = new KafkaStreams(streamsSupplier.get(), streamsConfig)
//    streams.setStateListener(runner)
//    streams.setUncaughtExceptionHandler(runner)
//    streams.cleanUp()
//    new ManagedKafkaStreams(streams)
    new ManagedLifeCycle {override def start(): Unit = {}

      override def stop(): Unit = {}

      override def isRunning: Boolean = true
    }
  }

  def checkConsumerTopic(): Boolean = {
    val adminClient = AdminClient.create(getBootstrapProperties)
    try {
      val present = adminClient.listTopics().names().get().contains(consumerTopicName)
      if (!present) {
        throw new ConsumerTopicNotPresentException(s"Topic $consumerTopicName is not present")
      }
      present
    }
    finally {
      Try(adminClient.close(5, TimeUnit.SECONDS))
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

