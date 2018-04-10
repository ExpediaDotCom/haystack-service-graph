package com.expedia.www.haystack.service.graph.node.finder

import java.util.Properties

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.kafka.KafkaController
import com.expedia.www.haystack.service.graph.node.finder.config.AppConfiguration
import org.expedia.www.haystack.commons.scalatest.IntegrationSuite
import org.scalatest.BeforeAndAfter

@IntegrationSuite
class AppSpec extends TestSpec with BeforeAndAfter {

  private val streamsRunner = App.createStreamsRunner(new AppConfiguration("integration/local.conf"))

  val zkProperties = new Properties
  zkProperties.load(classOf[AppSpec].getClassLoader.getResourceAsStream("integration/zookeeper.properties"))

  val kafkaProperties = new Properties
  kafkaProperties.load(classOf[AppSpec].getClassLoader.getResourceAsStream("integration/kafka-server.properties"))

  val KafkaController = new KafkaController(kafkaProperties, zkProperties)

  before {
    //start kafka and zk
    KafkaController.startService()

    //kafka to finish initializing
    Thread.sleep(1000)

    //ensure test topics are present

    //start topology
    streamsRunner.start()
  }

  describe("node finder application") {
    it("should process spans from kafka and produce latency metrics and graph edges") {

      //send test data to source topic

      //read data from output topics

      //check if they are as expected

    }
  }

  after {
    //stop topology
    streamsRunner.close()

    //stop kafka and zk
    KafkaController.stopService()
  }

}
