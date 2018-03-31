package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.entities.MetricPoint
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
      Some(List[MetricPoint]())
    else
      None
  }
}
