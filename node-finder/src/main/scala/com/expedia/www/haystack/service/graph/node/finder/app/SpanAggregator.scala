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
import com.expedia.www.haystack.service.graph.node.finder.model.{BottomHeavyHeap, LightSpan, WeighableSpan}
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.streams.processor._
import org.slf4j.LoggerFactory

class SpanAggregatorSupplier(aggregatorInterval: Int) extends ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanAggregator(aggregatorInterval)
}

class SpanAggregator(aggregatorInterval: Int) extends Processor[String, Span] with MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanAggregator])
  private val processMeter = metricRegistry.meter("span.aggregator.process")
  private val aggregateMeter = metricRegistry.meter("span.aggregator.aggregate")
  private val forwardMeter = metricRegistry.meter("span.aggregator.emit")
  private val aggregateHistogram = metricRegistry.histogram("span.aggregator.buffered.spans")

  //Bottom-heavy heap is the opposite of a top-heavy heap data structure
  //in here, heaviest object will be at the end of the queue. dequeue will
  //return the lightest element first - this is backed by a PriorityQueue
  private val weightedQueue = BottomHeavyHeap[WeighableSpan]()

  override def init(context: ProcessorContext): Unit = {
    context.schedule(aggregatorInterval, PunctuationType.STREAM_TIME, getPunctuator(context))
    LOGGER.info(s"${this.getClass.getSimpleName} initialized")
  }

  override def process(key: String, span: Span): Unit = {
    processMeter.mark()

    //find the span type
    val spanType = SpanUtils.getSpanType(span)
    LOGGER.info(s"Received $spanType span : ${span.getTraceId} :: ${span.getSpanId} :: ${span.getStartTime}")

    if (spanType != SpanType.OTHER) {

      //startTime is in microseconds, so divide it by 1000 to send MS
      val weighableSpan = WeighableSpan(span.getSpanId,
        span.getStartTime / 1000,
        span.getServiceName,
        span.getOperationName,
        span.getDuration, spanType)

      //add it to the weighted queue
      weightedQueue.enqueue(weighableSpan)

      aggregateMeter.mark()
      LOGGER.info(s"Processed $spanType span : $weighableSpan")
    }
  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}

  private def forward(context: ProcessorContext, lightSpan: LightSpan): Unit = {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(s"Forwarding complete LightSpan: $lightSpan")
    }
    context.forward(lightSpan.spanId, lightSpan)
    forwardMeter.mark()
  }

  private def drainQueueAndMapAsSpanPairs() : Map[String, LightSpan] = {
    var mapOfLightSpans : Map[String, LightSpan] = Map.empty

    //dequeue everything and add to map. if the span is already there, then merge it.
    while (weightedQueue.nonEmpty) {
      val weighableSpan = weightedQueue.dequeue()
      val spanLite = mapOfLightSpans.getOrElse(weighableSpan.spanId, {
        val ls = new LightSpan(weighableSpan.spanId)
        mapOfLightSpans = mapOfLightSpans.updated(weighableSpan.spanId, ls)
        ls
      })

      spanLite.merge(weighableSpan)
    }

    mapOfLightSpans
  }

  @VisibleForTesting
  def getPunctuator(context: ProcessorContext): Punctuator = {
    (timestamp: Long) => {
      //add gauge
      aggregateHistogram.update(spanCount)

      //we process only until cutoff time and leave the rest in place and see
      //if they get their matching span pair in the next punctuation
      val cutOffTime = timestamp - (aggregatorInterval * 0.5).asInstanceOf[Long]

      LOGGER.info(s"Punctuate called with $timestamp. CutOff is $cutOffTime. Queue size is ${weightedQueue.size} spans")

      //dequeue weighableSpans and add to map of [spanId, LightSpan]
      //if a span is already there, then merge it
      val mapOfLightSpans : Map[String, LightSpan] = drainQueueAndMapAsSpanPairs()

      //iterate map values and forward all complete spans. If the incomplete one is within
      //the last few TimeUnits, we will retain it by enqueuing again to see if there is a matching
      //span in the next batch. If the incomplete one is over the time limit, we will discard them
      var count = 0
      mapOfLightSpans.values.foreach(lightSpan => {
        if (lightSpan.isComplete) {
          forward(context, lightSpan)
          count += 1
        }
        else {
          lightSpan.getBackingSpans.foreach({
            weighableSpan => if (weighableSpan.isLaterThan(cutOffTime)) weightedQueue.enqueue(weighableSpan)
          })
        }
      })

      // commit the current processing progress
      context.commit()
    }
  }

  @VisibleForTesting
  def spanCount: Int = weightedQueue.size
}
