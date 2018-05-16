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
package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.commons.entities.encoders.Encoder
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.node.finder.model.SpanPair
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.slf4j.LoggerFactory

class LatencyProducerSupplier(metricPointEncoder: Encoder) extends ProcessorSupplier[String, SpanPair] {
  require(metricPointEncoder != null)
  override def get(): Processor[String, SpanPair] = new LatencyProducer(metricPointEncoder)
}

class LatencyProducer(metricPointEncoder: Encoder) extends Processor[String, SpanPair] with MetricsSupport {
  private var context: ProcessorContext = _
  private val processMeter = metricRegistry.meter("latency.producer.process")
  private val forwardMeter = metricRegistry.meter("latency.producer.emit")
  private val LOGGER = LoggerFactory.getLogger(classOf[LatencyProducer])
  require(metricPointEncoder != null)


  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def process(key: String, spanPair: SpanPair): Unit = {
    processMeter.mark()

    if (LOGGER.isDebugEnabled) {
      LOGGER.debug(s"Received message ($key, $spanPair)")
    }

    spanPair.getLatency match {
      case Some(metricPoint) =>
        context.forward(metricPoint.getMetricPointKey(encoder = metricPointEncoder), metricPoint)
        forwardMeter.mark()
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(s"Latency Metric: (${metricPoint.metric}, $metricPoint")
        }
      case None =>
    }

    context.commit()
  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}
}
