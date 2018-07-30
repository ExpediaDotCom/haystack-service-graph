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
import com.expedia.www.haystack.commons.graph.GraphEdgeTagCollector
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.node.finder.model.{LightSpan, SpanPair, SpanPairBuilder}
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import com.netflix.servo.util.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.streams.processor._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class SpanAccumulatorSupplier(accumulatorInterval: Int, tagCollector: GraphEdgeTagCollector) extends
  ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanAccumulator(accumulatorInterval, tagCollector)
}

class SpanAccumulator(accumulatorInterval: Int, tagCollector: GraphEdgeTagCollector)
  extends Processor[String, Span] with MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanAccumulator])
  private val processMeter = metricRegistry.meter("span.accumulator.process")
  private val aggregateMeter = metricRegistry.meter("span.accumulator.aggregate")
  private val forwardMeter = metricRegistry.meter("span.accumulator.emit")
  private val aggregateHistogram = metricRegistry.histogram("span.accumulator.buffered.spans")

  //queue to store incoming spans
  //private val spansQueue = mutable.Queue[LightSpan]()

  private var parentSpanMap = mutable.HashMap[String, LightSpan]()
  private var spanMap = mutable.HashMap[String, mutable.HashSet[LightSpan]]()
  private var completedSpanPairs = mutable.ListBuffer[SpanPair]()


  override def init(context: ProcessorContext): Unit = {
    context.schedule(accumulatorInterval, PunctuationType.STREAM_TIME, getPunctuator(context))
    LOGGER.info(s"${this.getClass.getSimpleName} initialized")
  }

  override def process(key: String, span: Span): Unit = {
    processMeter.mark()

    //find the span type
    val spanType = SpanUtils.getSpanType(span)

    if (SpanUtils.isAccumulableSpan(span)) {

      val lightSpan = LightSpan(span.getSpanId,
        span.getParentSpanId,
        span.getStartTime / 1000, //startTime is in microseconds, so divide it by 1000 to send MS
        span.getServiceName,
        span.getOperationName,
        span.getDuration,
        spanType,
        tagCollector.collectTags(span))

      //add it to the span map
      spanMap.getOrElseUpdate(span.getSpanId, mutable.HashSet[LightSpan]()).add(lightSpan)
      if (StringUtils.isNotEmpty(span.getParentSpanId)) {
        parentSpanMap.put(span.getParentSpanId, lightSpan)
      }

      processSpan(lightSpan.spanId, spanMap(lightSpan.spanId))

      aggregateMeter.mark()
    }
  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}

  private def forward(context: ProcessorContext, spanPair: SpanPair): Unit = {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(s"Forwarding complete SpanPair: $spanPair")
    }
    context.forward(spanPair.getId, spanPair)
    forwardMeter.mark()
  }

  private def processSpan(spanId: String, spans: mutable.Set[LightSpan]) = {

    var completedSpanPair: Option[SpanPair] = None
    //dequeue everything and add to map. if the span is already there, then merge it.
    if (spans.size > 1) {
      completedSpanPair = Option(SpanPairBuilder.createSpanPair(spans.head, spans.tail.head))
    } else {
      parentSpanMap.get(spanId) match {
        case Some(parentSpan) if parentSpan.serviceName != spans.head.serviceName => {
          completedSpanPair = Option(SpanPairBuilder.createSpanPair(parentSpan, spans.head))
        }
        case _ =>
      }
    }

    if (completedSpanPair.isDefined) {
      completedSpanPairs += completedSpanPair.get

      completedSpanPair.get.getBackingSpans.foreach(ls => {
        spanMap.remove(ls.spanId)
        parentSpanMap.remove(ls.spanId)
      })
    }
  }

  @VisibleForTesting
  def getPunctuator(context: ProcessorContext): Punctuator = {
    (timestamp: Long) => {
      //add gauge
      aggregateHistogram.update(spanCount)

      //we process only until cutoff time and leave the rest in place and see
      //if they get their matching span pair in the next punctuation
      val cutOffTime = timestamp - (accumulatorInterval * 0.5).asInstanceOf[Long]

      LOGGER.debug(s"Punctuate called with $timestamp. CutOff is $cutOffTime. Map sizes are ${spanMap.values.flatten[LightSpan].size} & ${parentSpanMap.size}")

      //dequeue weighableSpans and add to map of [spanId, SpanPair]
      //if a span is already there, then merge it
      spanMap.foreach {
        case (spanId, spans) => processSpan(spanId, spans)
      }

      //iterate map values and forward all complete spans. If the incomplete one is within
      //the last few TimeUnits, we will retain it by enqueuing again to see if there is a matching
      //span in the next batch. If the incomplete one is over the time limit, we will discard them
      var count = 0
      completedSpanPairs.foreach(spanPair => {
        forward(context, spanPair)
        count += 1
        spanMap = spanMap.filter {
          case (_, ls) => ls.exists(sp => sp.isLaterThan(cutOffTime))
        }
        parentSpanMap = parentSpanMap.filter {
          case (_, ls) => ls.isLaterThan(cutOffTime)
        }
      })

      // commit the current processing progress
      context.commit()
    }
  }

  @VisibleForTesting
  def spanCount: Int = spanMap.values.flatten[LightSpan].size
}
