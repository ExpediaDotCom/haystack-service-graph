/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.www.haystack.commons.kstreams.app

import com.expedia.www.haystack.commons.health.HealthStatusController
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class StreamsRunner(streamsFactory: StreamsFactory, healthStatusController: HealthStatusController) extends StateListener
  with Thread.UncaughtExceptionHandler with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamsRunner])
  private var kafkaStreams : ManagedLifeCycle = _

  require(streamsFactory != null, "valid streamsFactory is required")
  require(healthStatusController != null, "valid healthStatusController is required")

  override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    LOGGER.info(s"State change event called with newState=$newState and oldState=$oldState")
  }

  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    LOGGER.error(s"uncaught exception occurred running kafka streams for thread=${t.getName}", e)
    healthStatusController.setUnhealthy()
  }

  def start(): Unit = {
    LOGGER.info("Starting the given topology.")

    Try(streamsFactory.create(this)) match {
      case Success(streams) =>
        kafkaStreams = streams
        kafkaStreams.start()
        healthStatusController.setHealthy()
        LOGGER.info("KafkaStreams started successfully")
      case Failure(e) =>
        LOGGER.error(s"KafkaStreams failed to start : ${e.getMessage}", e)
        healthStatusController.setUnhealthy()
    }
  }

  def close(): Unit = {
    if (kafkaStreams != null) {
      kafkaStreams.stop()
    }
  }
}
