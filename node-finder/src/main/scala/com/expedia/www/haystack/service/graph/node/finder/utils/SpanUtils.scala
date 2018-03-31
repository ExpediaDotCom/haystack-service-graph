package com.expedia.www.haystack.service.graph.node.finder.utils

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

object SpanUtils {

  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"

  private val SPAN_MARKERS = Map(
    CLIENT_SEND_EVENT -> Flag(1), CLIENT_RECV_EVENT -> Flag(2),
    SERVER_SEND_EVENT -> Flag(4), SERVER_RECV_EVENT -> Flag(8))

  private val SPAN_TYPE_MAP = Map(Flag(3) -> SpanType.CLIENT, Flag(12) -> SpanType.SERVER)

  def getSpanType(span: Span): SpanType = {
    var flag = Flag(0)
    span.getLogsList.forEach(log => {
      log.getFieldsList.foreach(tag => {
        if (tag.getKey.equalsIgnoreCase("event") && StringUtils.isNotEmpty(tag.getVStr)) {
          flag = flag | SPAN_MARKERS.getOrElse(tag.getVStr.toLowerCase, Flag(0))
        }
      })
    })
    SPAN_TYPE_MAP.getOrElse(flag, SpanType.OTHER)
  }

  def getEventTimestamp(span: Span, event: String): Long =
    span.getLogsList.find(log => {
      log.getFieldsList.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") && StringUtils.isNotEmpty(tag.getVStr) &&
          tag.getVStr.equalsIgnoreCase(event)
      })
    }) match {
      case Some(log) => log.getTimestamp
      case _ => 0
    }
}

object SpanType extends Enumeration {
  type SpanType = Value
  val SERVER, CLIENT, OTHER = Value
}

case class Flag(value: Int) {
  def | (that: Flag): Flag = Flag(this.value | that.value)

  override def equals(obj: scala.Any): Boolean = {
    obj.asInstanceOf[Flag].value == value
  }
}


