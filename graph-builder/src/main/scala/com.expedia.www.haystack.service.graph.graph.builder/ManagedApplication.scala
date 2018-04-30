package com.expedia.www.haystack.service.graph.graph.builder

import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.kstreams.app.ManagedService
import com.expedia.www.haystack.commons.logger.LoggerUtils
import org.slf4j.LoggerFactory

class ManagedApplication(service: ManagedService, stream: ManagedService, jmxReporter: JmxReporter) {

  private val LOGGER = LoggerFactory.getLogger(classOf[ManagedApplication])
  private val running = new AtomicBoolean(false)

  require(service != null)
  require(stream != null)
  require(jmxReporter != null)

  // TODO handle exceptions in start step
  def start(): Unit = {
    jmxReporter.start()
    LOGGER.info("Starting the given topology and service")

    service.start()
    LOGGER.info("http service started successfully")

    stream.start()
    LOGGER.info("kafka stream started successfully")

    running.set(true)
  }

  /**
    * This method stops the given `StreamsRunner` and `JmxReporter` is they have been
    * previously started. If not, this method does nothing
    */
  def stop(): Unit = {
    if (running.getAndSet(false)) {
      LOGGER.info("Shutting down http service")
      service.stop()

      LOGGER.info("Shutting down kafka stream")
      stream.stop()

      LOGGER.info("Shutting down jmxReporter")
      jmxReporter.close()

      LOGGER.info("Shutting down logger. Bye!")
      LoggerUtils.shutdownLogger()
    }
  }
}