package com.expedia.www.haystack.commons.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.RunningAsBroker
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

class KafkaController(kafkaProperties: Properties, zooKeeperProperties: Properties) {
  require(kafkaProperties != null)
  require(zooKeeperProperties != null)

  private val LOGGER = LoggerFactory.getLogger(classOf[KafkaController])

  private val zkPort = zooKeeperProperties.getProperty("clientPort").toInt
  private val kafkaPort = kafkaProperties.getProperty("port").toInt

  lazy val zkUrl: String = "localhost:" + zkPort
  lazy val kafkaUrl: String = "localhost:" + kafkaPort

  private val kafkaPropertiesWithZk = new Properties
  kafkaPropertiesWithZk.putAll(kafkaProperties)
  kafkaPropertiesWithZk.put("zookeeper.connect", zkUrl)
  private val kafkaServer = new KafkaLocal(kafkaPropertiesWithZk)

  private val isRunning = new AtomicBoolean(false)

  def startService(): Unit = {
    //start zk
    val zookeeper = new ZooKeeperLocal(zooKeeperProperties)
    new Thread(zookeeper).start()

    //start kafka
    kafkaServer.start()
    Thread.sleep(1000)
    if (kafkaServer.state().currentState != RunningAsBroker.state) {
      throw new IllegalStateException("Kafka server is not in a running state")
    }

    //lifecycle message
    LOGGER.info("Kafka started and listening : {}", kafkaUrl)
  }

  def stopService(): Unit = {
    //stop kafka
    kafkaServer.stop()

    //lifecycle message
    LOGGER.info("Kafka stopped")
  }

  def createTopics(topics: List[String]): Unit = {
    if (topics.nonEmpty) {
      val adminClient = AdminClient.create(getBootstrapProperties)
      try {
        adminClient.createTopics(topics.map(topic => new NewTopic(topic, 1, 1)).asJava)
        adminClient.listTopics().names().get().forEach(s => LOGGER.info("Available topic : {}", s))
      }
      finally {
        Try(adminClient.close(5, TimeUnit.SECONDS))
      }
    }
  }

  def createProducer[K, V] (topic: String, keySerializer: Class[_ <: Serializer[K]],
                            valueSerializer: Class[_ <: Serializer[V]]) : KafkaProducer[K, V] = {
    val properties = getBootstrapProperties
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, topic + "Producer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName)
    new KafkaProducer[K, V](properties)
  }

  def createConsumer[K, V] (topic: String, keySerializer: Class[_ <: Deserializer[K]],
                            valueSerializer: Class[_ <: Deserializer[V]]) : KafkaConsumer[K, V] = {
    val properties = getBootstrapProperties
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, topic + "Consumer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "ConsumerGroup")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializer.getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializer.getName)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = new KafkaConsumer[K, V](properties)
    consumer.subscribe(List(topic).asJava)
    consumer
  }

  private def getBootstrapProperties: Properties = {
    val properties = new Properties()
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, List(kafkaUrl).asJava)
    properties
  }
}

class InvalidStateException(message: String) extends RuntimeException(message) {}
