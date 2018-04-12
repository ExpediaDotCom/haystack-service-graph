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

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import com.expedia.www.haystack.service.graph.node.finder.utils.{Flag, SpanType, SpanUtils}
import org.slf4j.LoggerFactory

/**
  * An instance of LightSpan can contain data from both server and client spans.
  * LightSpan is considered "complete" if it has data fields from both server and client span of the same SpanId
  * @param spanId Unique identifier of a Span
  */
class LightSpan(val spanId: String) {

  private val LOGGER = LoggerFactory.getLogger(classOf[LightSpan])

  require(spanId != null)

  private var clientSpan : WeighableSpan = _
  private var serverSpan : WeighableSpan = _
  private var flag = Flag(0)

  /**
    * Returns true of the current instance has data from both server and client spans
    * of the same SpanId
    * @return true or false
    */
  def isComplete: Boolean = flag.equals(Flag(3))

  /**
    * Returns the backing WeighableSpan objects
    * @return list of WeighableSpan objects or an empty list
    */
  def getBackingSpans : List[WeighableSpan] = {
    List(clientSpan, serverSpan).filter(w => w != null)
  }

  /**
    * Merges the given span into the current instance of the LightSpan. If the spanId of
    * the given span matches the spanId of the LightSpan, then it is held by the LighSpan
    * to produce {@link #getGraphEdge} and {@link #getLatency} data
    * @param weighableSpan WeighableSpan to be merged with the current LightSpan
    */
  def merge(weighableSpan: WeighableSpan): Unit = {
    if (weighableSpan.spanId.equals(spanId)) {
      LOGGER.debug(s"received a matching span of type ${weighableSpan.spanType}")
      weighableSpan.spanType match {
        case SpanType.CLIENT =>
          this.clientSpan = weighableSpan
          flag = flag | Flag(1)
        case SpanType.SERVER =>
          this.serverSpan = weighableSpan
          flag = flag | Flag(2)
      }
    }
  }

  /**
    * Returns an instance of GraphEdge if the current LightSpan is complete. A GraphEdge
    * contains the client span's ServiceName, it's OperationName and the corresponding server
    * span's ServiceName. These three data points acts as the two nodes and edge of a graph relationship
    * @return an instance of GraphEdge or None if the current LightSpan is inComplete
    */
  def getGraphEdge: Option[GraphEdge] = {
    if (isComplete) {
      Some(GraphEdge(clientSpan.serviceName, serverSpan.serviceName, clientSpan.operationName))
    } else {
      None
    }
  }

  /**
    * Returns an instance of MetricPoint that measures the latency of the current Span. Latency of the current
    * Span is computed as client span's duration minus it's corresponding server span's duration. MetricPoint instance
    * returned will be of type Gauge tagged with the current (client span's) service name and operation name.
    * @return an instance of MetricPoint or None if the current spanLite instance is incomplete
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

  override def toString = s"LightSpan($flag, $spanId, $isComplete)"
}
