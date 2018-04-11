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
  * A 'light' version of Span with fewer fields. An instance of SpanLite can contain data
  * from both server and client spans. SpanLite is considered "complete" if it has data fields
  * from both server and client span of the same SpanId
  * @param spanId Unique identifier of a Span
  */
class SpanLite(val spanId: String) {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanLite])

  require(spanId != null)

  private var sourceServiceName: String = _
  private var operationName: String = _
  private var clientSend: Long = _
  private var clientDuration: Long = _
  private var serverDuration: Long = _
  private var destinationServiceName: String = _
  private var flag = Flag(0)

  /**
    * Returns true of the current instance has data from both server and client spans
    * of the same SpanId
    * @return true or false
    */
  def isComplete: Boolean = flag.equals(Flag(3))

  /**
    * Merges the given span into the current instance of the SpanLite. If the spanId of
    * the given span matches the spanId of the SpanLite, certain fields from the given span are
    * read and held in the current SpanLite by mutating it's private fields.
    * @param span Span to be merged with the current SpanLite
    * @param spanType type of the Span provided
    * @return true if a merge is performed or false
    */
  def merge(span: Span, spanType: SpanType): Boolean = {
    if (span.getSpanId.equals(spanId)) {
      LOGGER.debug(s"received a matching span of type $spanType")
      spanType match {
        case SpanType.CLIENT =>
          sourceServiceName = span.getServiceName
          operationName = span.getOperationName
          clientSend = SpanUtils.getEventTimestamp(span, SpanUtils.CLIENT_SEND_EVENT).getOrElse(0)
          clientDuration = span.getDuration
          flag = flag | Flag(1)
          true
        case SpanType.SERVER =>
          destinationServiceName = span.getServiceName
          serverDuration = span.getDuration
          flag = flag | Flag(2)
          true
        case _ => false
      }
    }
    else {
      LOGGER.debug(s"received a span with spanId ${span.getSpanId} to be merged with $spanId - Ignored")
      false
    }
  }

  /**
    * Returns an instance of GraphEdge if the current SpanLite is complete. A GraphEdge
    * contains the client span's ServiceName, it's OperationName and the corresponding server
    * span's ServiceName. These three data points acts as the two nodes and edge of a graph relationship
    * @return an instance of GraphEdge or None if the current SpanLite is inComplete
    */
  def getGraphEdge: Option[GraphEdge] = {
    if (isComplete) {
      Some(GraphEdge(sourceServiceName, destinationServiceName, operationName))
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
      Some(MetricPoint("latency", MetricType.Gauge, getTags, clientDuration - serverDuration, clientSend))
    } else {
      None
    }
  }

  private def getTags: Map[String, String] = {
    Map(
      TagKeys.SERVICE_NAME_KEY -> sourceServiceName,
      TagKeys.OPERATION_NAME_KEY -> operationName
    )
  }


  override def toString = s"SpanLite($flag, $spanId, $isComplete)"
}
