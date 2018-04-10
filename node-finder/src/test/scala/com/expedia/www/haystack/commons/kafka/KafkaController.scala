package com.expedia.www.haystack.commons.kafka

import java.util.Properties

import org.slf4j.LoggerFactory

class KafkaController(kafkaProperties : Properties, zooKeeperProperties: Properties) {
  require(kafkaProperties != null)
  require(zooKeeperProperties != null)

  private val LOGGER = LoggerFactory.getLogger(classOf[KafkaController])

  private val zkPort = zooKeeperProperties.getProperty("clientPort").toInt
  private val kafkaPort = kafkaProperties.getProperty("port").toInt

  lazy val zkUrl: String = "localhost:" + zkPort
  lazy val kafkaUrl: String = "localhost" + kafkaPort

  private val kafkaPropertiesWithZk = new Properties(kafkaProperties)
  kafkaPropertiesWithZk.put("zookeeper.connect", zkUrl)
  private val kafkaServer = new KafkaLocal(kafkaPropertiesWithZk)

  def startService() : Unit = {
    //start zk
    val zookeeper = new ZooKeeperLocal(zooKeeperProperties)
    new Thread(zookeeper).start()

    //start kafka
    kafkaServer.start()

    //lifecycle message
    LOGGER.info("Kafka started and listening : {}", kafkaUrl)
  }

  def stopService() : Unit = {
    //stop kafka
    kafkaServer.stop()

    //lifecycle message
    LOGGER.info("Kafka stopped")
  }
}
