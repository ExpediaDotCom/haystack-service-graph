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
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import com.expedia.www.haystack.service.graph.node.finder.model.SpanLite
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import org.apache.kafka.streams.processor._
import org.slf4j.LoggerFactory

class SpanAggregatorSupplier(kafkaConfig: KafkaConfiguration) extends ProcessorSupplier[String, Span] {
  override def get() : Processor[String, Span] = new SpanAggregator(kafkaConfig)
}

class SpanAggregator(kafkaConfig: KafkaConfiguration) extends Processor[String, Span] with Punctuator {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanAggregator])
  private var context: ProcessorContext = _
  private var map : Map[String, SpanLite] = Map.empty

  override def init(context: ProcessorContext): Unit =  {
    this.context = context
    this.context.schedule(kafkaConfig.aggregatorInterval, PunctuationType.STREAM_TIME, this)
    LOGGER.info(s"${this.getClass.getSimpleName} initialized")
  }

  override def process(key: String, span: Span): Unit = {
    val spanType = SpanUtils.getSpanType(span)
    LOGGER.info(s"Received $spanType span : ${span.getTraceId} :: ${span.getSpanId}")

    if (spanType != SpanType.OTHER) {
      val spanLite = map.getOrElse(span.getSpanId, {
        val s = new SpanLite(span.getSpanId)
        map = map.updated(span.getSpanId, s)
        s
      })
      spanLite.merge(span, spanType)
      LOGGER.info(s"Received $spanType span : $spanLite")
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    //time to emit. Swap the map here
    val mapToEmit = map
    map = Map.empty

    //iterate and forward
    mapToEmit.values.filter(s => s.isComplete).foreach(s => context.forward(s.spanId, s))

    // commit the current processing progress
    context.commit()
  }

  override def close(): Unit = {}
}
