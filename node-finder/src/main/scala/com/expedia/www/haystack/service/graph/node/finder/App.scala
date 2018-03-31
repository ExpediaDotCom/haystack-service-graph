package com.expedia.www.haystack.service.graph.node.finder

import com.expedia.www.haystack.commons.health.{HealthStatusController, UpdateHealthStatusFile}
import com.expedia.www.haystack.commons.kstreams.app.{Main, StreamsFactory, StreamsRunner}
import com.expedia.www.haystack.service.graph.node.finder.app.Streams
import com.expedia.www.haystack.service.graph.node.finder.config.AppConfiguration
import org.slf4j.LoggerFactory

object App extends Main {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  def createStreamsRunner() : StreamsRunner = {
    val appConfiguration = new AppConfiguration()

    val healthStatusController = new HealthStatusController
    healthStatusController.addListener(new UpdateHealthStatusFile(appConfiguration.healthStatusFilePath))

    val streamsFactory = new StreamsFactory(new Streams(appConfiguration.kafkaConfig),
      appConfiguration.kafkaConfig.streamsConfig,
      Some(appConfiguration.kafkaConfig.protoSpanTopic))

    new StreamsRunner(streamsFactory, healthStatusController)
  }
}
