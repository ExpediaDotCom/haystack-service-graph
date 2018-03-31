package com.expedia.www.haystack.service.graph.node.finder.config

import java.util.Properties

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.expedia.www.haystack.commons.kstreams.SpanTimestampExtractor
import com.typesafe.config.Config
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.TimestampExtractor

import scala.collection.JavaConverters._

class AppConfiguration {

  private val config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName = "app.conf")

  val healthStatusFilePath: String = config.getString("health.status.path")

  lazy val kafkaConfig: KafkaConfiguration = {

    // verify if the applicationId and bootstrap server config are non empty
    def verifyRequiredProps(props: Properties): Unit = {
      require(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG).nonEmpty)
      require(props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)
    }

    def addProps(config: Config, props: Properties, prefix: (String) => String = identity): Unit = {
      config.entrySet().asScala.foreach(kv => {
        val propKeyName = prefix(kv.getKey)
        props.setProperty(propKeyName, kv.getValue.unwrapped().toString)
      })
    }

    val kafka = config.getConfig("kafka")
    val producerConfig = kafka.getConfig("producer")
    val consumerConfig = kafka.getConfig("consumer")
    val streamsConfig = kafka.getConfig("streams")

    val props = new Properties

    // add stream specific properties
    addProps(streamsConfig, props)

    // validate props
    verifyRequiredProps(props)

    val timestampExtractor =  Option(props.getProperty("timestamp.extractor")) match {
      case Some(timeStampExtractorClass) =>
        Class.forName(timeStampExtractorClass).newInstance().asInstanceOf[TimestampExtractor]
      case None =>
        new SpanTimestampExtractor
    }

    //set timestamp extractor
    props.setProperty("timestamp.extractor", timestampExtractor.getClass.getName)

    KafkaConfiguration(new StreamsConfig(props),
      producerConfig.getString("metrics.topic"),
      producerConfig.getString("service.call.topic"),
      consumerConfig.getString("topic"),
      if (streamsConfig.hasPath("auto.offset.reset"))
        AutoOffsetReset.valueOf(streamsConfig.getString("auto.offset.reset").toUpperCase)
      else
        AutoOffsetReset.LATEST,
      timestampExtractor,
      kafka.getInt("aggregator.interval"),
      kafka.getLong("close.timeout.ms")
    )
  }
}
