package com.expedia.www.haystack.commons.kafka

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}

class KafkaLocal(val kafkaProperties: Properties) {
  val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(kafkaProperties)
  val kafka: KafkaServerStartable = new KafkaServerStartable(kafkaConfig)

  def start(): Unit = {
    kafka.startup()
  }

  def stop(): Unit = {
    kafka.shutdown()
  }
}
