package com.expedia.www.haystack.commons.kstreams.app

import com.expedia.www.haystack.UnitTestSpec
import org.apache.kafka.streams.KafkaStreams
import org.easymock.EasyMock._

class ManagedKafkaStreamsSpec extends UnitTestSpec {
  describe("ManagedKafkaStreams") {
    it("should start the underlying kafkaStreams when started") {
      Given("a fully configured ManagedKafkaStreams instance")
      val kafkaStreams = mock[KafkaStreams]
      val managedKafkaStreams = new ManagedKafkaStreams(kafkaStreams)
      When("start is invoked")
      expecting {
        kafkaStreams.start().once()
      }
      replay(kafkaStreams)
      managedKafkaStreams.start()
      Then("it should start the KafkaStreams application")
      verify(kafkaStreams)
    }
    it("should close the KafkaStreams when stopped") {
      Given("a fully configured ManagedKafkaStreams instance")
      val kafkaStreams = mock[KafkaStreams]
      val managedKafkaStreams = new ManagedKafkaStreams(kafkaStreams)
      When("stop is invoked")
      expecting {
        kafkaStreams.start().once()
        kafkaStreams.close().once()
      }
      replay(kafkaStreams)
      managedKafkaStreams.start()
      Then("it should close the KafkaStreams application")
      managedKafkaStreams.stop()
      verify(kafkaStreams)
    }
    it("should not do anything when stop is called without starting") {
      Given("a fully configured ManagedKafkaStreams instance")
      val kafkaStreams = mock[KafkaStreams]
      val managedKafkaStreams = new ManagedKafkaStreams(kafkaStreams)
      When("stop is invoked without starting")
      replay(kafkaStreams)
      Then("it should do nothing")
      managedKafkaStreams.stop()
      verify(kafkaStreams)
    }
  }

}
