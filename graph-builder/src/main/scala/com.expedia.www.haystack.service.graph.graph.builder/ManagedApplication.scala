package com.expedia.www.haystack.service.graph.graph.builder

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.kstreams.app.ManagedService
import com.expedia.www.haystack.commons.logger.LoggerUtils
import org.slf4j.LoggerFactory

import scala.util.Try

class ManagedApplication(service: ManagedService, stream: ManagedService, jmxReporter: JmxReporter) {

  private val LOGGER = LoggerFactory.getLogger(classOf[ManagedApplication])

  require(service != null)
  require(stream != null)
  require(jmxReporter != null)

  def start(): Unit = {
    try {
      jmxReporter.start()
      LOGGER.info("Starting the given topology and service")

      service.start()
      LOGGER.info("http service started successfully")

      stream.start()
      LOGGER.info("kafka stream started successfully")
    } catch {
      case ex: Exception =>
        LOGGER.error("Observed fatal exception while starting the app", ex)
        stop()
        System.exit(1)
    }
  }

  /**
    * This method stops the given `StreamsRunner` and `JmxReporter` is they have been
    * previously started. If not, this method does nothing
    */
  def stop(): Unit = {
      LOGGER.info("Shutting down http service")
      Try(service.stop())

      LOGGER.info("Shutting down kafka stream")
      Try(stream.stop())

      LOGGER.info("Shutting down jmxReporter")
      Try(jmxReporter.close())

      LOGGER.info("Shutting down logger. Bye!")
      Try(LoggerUtils.shutdownLogger())
  }
}
