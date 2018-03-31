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
import com.expedia.www.haystack.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import com.expedia.www.haystack.service.graph.node.finder.utils.{Flag, SpanType, SpanUtils}

class SpanLite(val spanId: String) {

  require(spanId != null)

  private var sourceServiceName: String = _
  private var operationName: String = _
  private var clientSend: Long = _
  private var clientReceive: Long = _
  private var serverReceive: Long = _
  private var serverSend: Long = _
  private var destinationServiceName: String = _
  private var flag = Flag(0)

  def isComplete: Boolean = flag.equals(Flag(3))

  def merge(span: Span, spanType: SpanType): Unit = {
    spanType match {
      case SpanType.CLIENT =>
        sourceServiceName = span.getServiceName
        operationName = span.getOperationName
        clientSend = SpanUtils.getEventTimestamp(span, SpanUtils.CLIENT_SEND_EVENT)
        clientReceive = SpanUtils.getEventTimestamp(span, SpanUtils.CLIENT_RECV_EVENT)
        flag = flag | Flag(1)
      case SpanType.SERVER =>
        destinationServiceName = span.getServiceName
        serverReceive = SpanUtils.getEventTimestamp(span, SpanUtils.SERVER_RECV_EVENT)
        serverSend = SpanUtils.getEventTimestamp(span, SpanUtils.SERVER_SEND_EVENT)
        flag = flag | Flag(2)
    }
  }

  def getGraphEdge: Option[GraphEdge] = {
    if (isComplete)
      Some(GraphEdge(sourceServiceName, destinationServiceName, operationName))
    else
      None
  }

  def getLatency: Option[List[MetricPoint]] = {
    if (isComplete)
      Some(List[MetricPoint](
        MetricPoint(getMetricKey, MetricType.Gauge, Map.empty, serverReceive - clientSend, clientSend),
        MetricPoint(getMetricKey, MetricType.Gauge, Map.empty, clientReceive - serverSend, serverSend)
      ))
    else
      None
  }

  private def getMetricKey : String = {
    s"$sourceServiceName.$operationName.latency"
  }
}
