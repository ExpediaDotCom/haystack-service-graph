package com.expedia.www.haystack.service.graph.graph.builder

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.kstreams.app.ManagedService
import com.expedia.www.haystack.service.graph.graph.builder.ManagedApplication._
import org.mockito.Mockito.{verify, verifyNoMoreInteractions}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.slf4j.Logger

class ManagedApplicationSpec extends FunSpec with MockitoSugar with BeforeAndAfter {

  var service: ManagedService = _
  var stream: ManagedService = _
  var jmxReporter: JmxReporter = _
  var logger: Logger = _

  before {
    service = mock[ManagedService]
    stream = mock[ManagedService]
    jmxReporter = mock[JmxReporter]
    logger = mock[Logger]
  }

  after {
    verifyNoMoreInteractions(service)
    verifyNoMoreInteractions(stream)
    verifyNoMoreInteractions(jmxReporter)
    verifyNoMoreInteractions(logger)
  }

  describe("ManagedApplication constructor") {
    it ("should throw an IllegalArgumentException if passed a null service") {
      assertThrows[IllegalArgumentException] {
        new ManagedApplication(null, stream, jmxReporter, logger)
      }
    }
    it ("should throw an IllegalArgumentException if passed a null stream") {
      assertThrows[IllegalArgumentException] {
        new ManagedApplication(service, null, jmxReporter, logger)
      }
    }
    it ("should throw an IllegalArgumentException if passed a null jmxReporter") {
      assertThrows[IllegalArgumentException] {
        new ManagedApplication(service, stream, null, logger)
      }
    }
    it ("should throw an IllegalArgumentException if passed a null logger") {
      assertThrows[IllegalArgumentException] {
        new ManagedApplication(service, stream, jmxReporter, null)
      }
    }
  }

  describe("ManagedApplication start") {
    it ("should start all dependencies when called") {
      val managedApplication = new ManagedApplication(service, stream, jmxReporter, logger)
      managedApplication.start()
      verify(service).start()
      verify(logger).info(StartMessage)
      verify(stream).start()
      verify(logger).info(HttpStartMessage)
      verify(jmxReporter).start()
      verify(logger).info(StreamStartMessage)
    }
  }

  describe("ManagedApplication stop") {
    it ("should stop all dependencies when called") {
      val managedApplication = new ManagedApplication(service, stream, jmxReporter, logger)
      managedApplication.stop()
      verify(logger).info(HttpStopMessage)
      verify(service).stop()
      verify(logger).info(StreamStopMessage)
      verify(stream).stop()
      verify(logger).info(JmxReporterStopMessage)
      verify(jmxReporter).close()
      verify(logger).info(LoggerStopMessage)
    }
  }
}
