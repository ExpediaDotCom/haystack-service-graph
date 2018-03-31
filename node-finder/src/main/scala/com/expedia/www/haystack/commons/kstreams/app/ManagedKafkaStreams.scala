package com.expedia.www.haystack.commons.kstreams.app

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.streams.KafkaStreams

class ManagedKafkaStreams(kafkaStreams: KafkaStreams) extends ManagedLifeCycle {
  require(kafkaStreams != null)
  private val hasStarted: AtomicBoolean = new AtomicBoolean(false)

  override def start(): Unit = {
    kafkaStreams.start()
    hasStarted.set(true)
  }

  override def stop(): Unit = {
    if (hasStarted.getAndSet(false)) {
      kafkaStreams.close()
    }
  }

  override def isRunning: Boolean = hasStarted.get()
}
