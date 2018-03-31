package com.expedia.www.haystack.commons.kstreams.app

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.UnitTestSpec
import org.easymock.EasyMock._

class ApplicationSpec extends UnitTestSpec {
  describe("Application") {
    it("should require an instance of StreamsRunner") {
      Given("only a valid instance of jmxReporter")
      val streamsRunner : StreamsRunner = null
      val jmxReporter = mock[JmxReporter]
      When("an instance of Application is created")
      Then("it should throw an exception")
      intercept[IllegalArgumentException] {
        new Application(streamsRunner, jmxReporter)
      }
    }
    it("should require an instance of JmxReporter") {
      Given("only a valid instance of StreamsRunner")
      val streamsRunner : StreamsRunner = mock[StreamsRunner]
      val jmxReporter = null
      When("an instance of Application is created")
      Then("it should throw an exception")
      intercept[IllegalArgumentException] {
        new Application(streamsRunner, jmxReporter)
      }
    }
    it("should start both JmxReporter and StreamsRunner at start") {
      Given("a fully configured application")
      val streamsRunner : StreamsRunner = mock[StreamsRunner]
      val jmxReporter = mock[JmxReporter]
      val application = new Application(streamsRunner, jmxReporter)
      When("application is started")
      expecting {
        streamsRunner.start().once()
        jmxReporter.start().once()
      }
      replay(streamsRunner, jmxReporter)
      application.start()
      Then("it should call start on both streamsRunner and jmxReporter")
      verify(streamsRunner, jmxReporter)
    }
    it("should close both JmxReporter and StreamsRunner at stop") {
      Given("a fully configured and running application")
      val streamsRunner : StreamsRunner = mock[StreamsRunner]
      val jmxReporter = mock[JmxReporter]
      val application = new Application(streamsRunner, jmxReporter)
      When("application is stopped")
      expecting {
        streamsRunner.start().once()
        jmxReporter.start().once()
        streamsRunner.close().once()
        jmxReporter.close().once()
      }
      replay(streamsRunner, jmxReporter)
      application.start()
      application.stop()
      Then("it should call close on both streamsRunner and jmxReporter")
      verify(streamsRunner, jmxReporter)
    }
  }
}
