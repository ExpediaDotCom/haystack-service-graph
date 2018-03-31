package com.expedia.www.haystack.commons.kstreams.app

import com.expedia.www.haystack.commons.health.HealthStatusController
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.slf4j.LoggerFactory

class StateChangeListener(healthStatusController: HealthStatusController) extends StateListener
  with Thread.UncaughtExceptionHandler {

  require(healthStatusController != null)

  private val LOGGER = LoggerFactory.getLogger(classOf[StateChangeListener])

  def state(healthy : Boolean) : Unit =
    if (healthy) {
      healthStatusController.setHealthy()
    }
    else {
      healthStatusController.setUnhealthy()
    }

  override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    LOGGER.info(s"State change event called with newState=$newState and oldState=$oldState")
  }

  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    LOGGER.error(s"uncaught exception occurred running kafka streams for thread=${t.getName}", e)
    state(false)
  }
}
