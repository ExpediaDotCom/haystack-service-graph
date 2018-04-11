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

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.node.finder.model.SpanLite
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.streams.processor._
import org.slf4j.LoggerFactory

class SpanAggregatorSupplier(aggregatorInterval : Int) extends ProcessorSupplier[String, Span] {
  override def get() : Processor[String, Span] = new SpanAggregator(aggregatorInterval)
}

class SpanAggregator(aggregatorInterval : Int) extends Processor[String, Span] with Punctuator with MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanAggregator])
  private val processMeter = metricRegistry.meter("span.aggregator.process")
  private val aggregateMeter = metricRegistry.meter("span.aggregator.aggregate")
  private val forwardMeter = metricRegistry.meter("span.aggregator.emit")
  private val aggregateHistogram = metricRegistry.histogram("span.aggregator.buffered.spans")

  private var context: ProcessorContext = _
  private var map : Map[String, SpanLite] = Map.empty

  override def init(context: ProcessorContext): Unit =  {
    this.context = context
    this.context.schedule(aggregatorInterval, PunctuationType.STREAM_TIME, this)
    LOGGER.info(s"${this.getClass.getSimpleName} initialized")
  }

  override def process(key: String, span: Span): Unit = {
    processMeter.mark()

    val spanType = SpanUtils.getSpanType(span)

    LOGGER.info(s"Received $spanType span : ${span.getTraceId} :: ${span.getSpanId} :: ${span.getStartTime}")

    if (spanType != SpanType.OTHER) {
      val spanLite = map.getOrElse(span.getSpanId, {
        val s = new SpanLite(span.getSpanId)
        map = map.updated(span.getSpanId, s)
        aggregateMeter.mark()
        s
      })

      spanLite.merge(span, spanType)
      LOGGER.info(s"Processed $spanType span : $spanLite")
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    LOGGER.info(s"Punctuate called with $timestamp")

    //time to emit. Swap the map here
    val mapToEmit = map
    map = Map.empty

    //iterate and forward
    LOGGER.info(s"Punctuate processing ${mapToEmit.size} spans")
    mapToEmit.values.filter(s => s.isComplete).foreach(s => {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(s"Forwarding complete SpanLite: $s")
      }
      context.forward(s.spanId, s)
      forwardMeter.mark()
    })

    // commit the current processing progress
    context.commit()

    //add gauge
    aggregateHistogram.update(mapToEmit.size)
  }

  override def close(): Unit = {}

  @VisibleForTesting
  def spanCount : Int = map.size
}
