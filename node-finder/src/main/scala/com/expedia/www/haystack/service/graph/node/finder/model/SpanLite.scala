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

class SpanLite(val spanId: String) {

  private val LOGGER = LoggerFactory.getLogger(classOf[SpanLite])

  require(spanId != null)

  private var sourceServiceName: String = _
  private var operationName: String = _
  private var clientSend: Long = _
  private var clientReceive: Long = _
  private var clientDuration: Long = _
  private var serverReceive: Long = _
  private var serverSend: Long = _
  private var serverDuration: Long = _
  private var destinationServiceName: String = _
  private var flag = Flag(0)

  def isComplete: Boolean = flag.equals(Flag(3))

  def merge(span: Span, spanType: SpanType): Boolean = {
    if (span.getSpanId.equals(spanId)) {
      LOGGER.debug(s"received a matching span of type $spanType")
      spanType match {
        case SpanType.CLIENT =>
          sourceServiceName = span.getServiceName
          operationName = span.getOperationName
          clientSend = SpanUtils.getEventTimestamp(span, SpanUtils.CLIENT_SEND_EVENT).getOrElse(0)
          clientReceive = SpanUtils.getEventTimestamp(span, SpanUtils.CLIENT_RECV_EVENT).getOrElse(0)
          clientDuration = span.getDuration
          flag = flag | Flag(1)
          true
        case SpanType.SERVER =>
          destinationServiceName = span.getServiceName
          serverReceive = SpanUtils.getEventTimestamp(span, SpanUtils.SERVER_RECV_EVENT).getOrElse(0)
          serverSend = SpanUtils.getEventTimestamp(span, SpanUtils.SERVER_SEND_EVENT).getOrElse(0)
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

  def getGraphEdge: Option[GraphEdge] = {
    if (isComplete)
      Some(GraphEdge(sourceServiceName, destinationServiceName, operationName))
    else
      None
  }

  def getLatency: Option[MetricPoint] = {
    if (isComplete)
      Some(MetricPoint("latency", MetricType.Gauge, getTags, clientDuration - serverDuration, clientSend))
    else
      None
  }

  private def getTags: Map[String, String] = {
    Map(
      TagKeys.SERVICE_NAME_KEY -> sourceServiceName,
      TagKeys.OPERATION_NAME_KEY -> operationName
    )
  }
}
