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
package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.www.haystack.commons.entities._
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType
import org.slf4j.LoggerFactory

/**
  * An instance of SpanPair can contain data from both server and client spans.
  * SpanPair is considered "complete" if it has data fields from both server and client span of the same SpanId
  */
class SpanPair() {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanPair])

  private var clientSpan: LightSpan = _
  private var serverSpan: LightSpan = _

  /**
    * Returns true of the current instance has data for both server and client spans
    * and their services are different
    *
    * @return true or false
    */
  def isComplete: Boolean = {
    clientSpan != null && serverSpan != null && clientSpan.serviceName != serverSpan.serviceName
  }

  /**
    * Returns the backing LightSpan objects
    *
    * @return list of LightSpan objects or an empty list
    */
  def getBackingSpans: List[LightSpan] = {
    List(clientSpan, serverSpan).filter(w => w != null)
  }

  /**
    * Merges the given spans into the current instance of the SpanPair using spanType.
    * Also, merge them if parent-child relationship is there between given spans
    * to produce {@link #getGraphEdge} and {@link #getLatency} data
    *
    * @param spanOne lightSpan to be merged with the current SpanPair
    * @param spanTwo lightSpan to be merged with the current SpanPair
    */
  def merge(spanOne: LightSpan, spanTwo: LightSpan): Unit = {
    if (spanOne.spanId.equalsIgnoreCase(spanTwo.parentSpanId)) {
      setSpans(LightSpanBuilder.updateSpanType(spanOne, SpanType.CLIENT), LightSpanBuilder.updateSpanType(spanTwo, SpanType.SERVER))
    } else if (spanOne.parentSpanId.equalsIgnoreCase(spanTwo.spanId)) {
      setSpans(LightSpanBuilder.updateSpanType(spanOne, SpanType.SERVER), LightSpanBuilder.updateSpanType(spanTwo, SpanType.CLIENT))
    } else {
      setSpans(spanOne, spanTwo)
    }

    LOGGER.debug("created a span pair: client: {}, server: {}", List(clientSpan, serverSpan):_*)
  }

  /**
    * set clientSpan or serverSpan depending upon the value of spanType in LightSpan
    *
    * @param spanOne span which needs to be set to clientSpan or serverSpan
    * @param spanTwo span which needs to be set to clientSpan or serverSpan
    */
  private def setSpans(spanOne: LightSpan, spanTwo: LightSpan) = {
    Seq(spanOne, spanTwo).foreach(span =>
      span.spanType match {
        case SpanType.CLIENT =>
          this.clientSpan = span
        case SpanType.SERVER =>
          this.serverSpan = span
        case SpanType.OTHER =>
      }
    )
  }

  /**
    * Returns an instance of GraphEdge if the current SpanPair is complete. A GraphEdge
    * contains the client span's ServiceName, it's OperationName and the corresponding server
    * span's ServiceName. These three data points acts as the two nodes and edge of a graph relationship
    *
    * @return an instance of GraphEdge or None if the current SpanPair is inComplete
    */
  def getGraphEdge: Option[GraphEdge] = {
    if (isComplete) {
      val clientVertex = GraphVertex(clientSpan.serviceName, clientSpan.tags)
      val serverVertex = GraphVertex(serverSpan.serviceName, serverSpan.tags)
      Some(GraphEdge(clientVertex, serverVertex, clientSpan.operationName, clientSpan.time))
    } else {
      None
    }
  }

  /**
    * Returns an instance of MetricPoint that measures the latency of the current Span. Latency of the current
    * Span is computed as client span's duration minus it's corresponding server span's duration. MetricPoint instance
    * returned will be of type Gauge tagged with the current (client span's) service name and operation name.
    *
    * @return an instance of MetricPoint or None if the current spanPair instance is incomplete
    */
  def getLatency: Option[MetricPoint] = {
    if (isComplete) {
      val tags = Map(
        TagKeys.SERVICE_NAME_KEY -> clientSpan.serviceName,
        TagKeys.OPERATION_NAME_KEY -> clientSpan.operationName
      )

      Some(MetricPoint("latency", MetricType.Gauge, tags,
        clientSpan.duration - serverSpan.duration, clientSpan.time / 1000))
    } else {
      None
    }
  }

  def getId: String = s"($clientSpan.spanId)"

  override def toString = s"SpanPair($isComplete, $clientSpan, $serverSpan)"
}

object SpanPairBuilder {
  def createSpanPair(spanOne: LightSpan, spanTwo: LightSpan): SpanPair = {
    require(spanOne != null)
    require(spanTwo != null)

    val newSpanPair = new SpanPair
    newSpanPair.merge(spanOne, spanTwo)
    newSpanPair
  }
}