package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import org.apache.commons.lang3.StringUtils

/**
  * Light weight representation of a Span with minimal information required
  *
  * @param spanId        Unique identity of the Span
  * @param parentSpanId  spanId of its Parent span
  * @param time          Timestamp associated with a Span in MilliSeconds (i.e., StartTime)
  * @param serviceName   Service name of the span
  * @param operationName Operation name of the span
  * @param duration      duration of the Span
  * @param spanType      type of the span
  */
case class LightSpan(spanId: String,
                     parentSpanId: String,
                     time: Long,
                     serviceName: String,
                     operationName: String,
                     duration: Long,
                     spanType: SpanType,
                     tags: Map[String, String]) {
  require(StringUtils.isNotBlank(spanId))
  require(time > 0)
  require(StringUtils.isNotBlank(serviceName))
  require(StringUtils.isNoneBlank(operationName))
  require(spanType != null)

  /**
    * check whether this light span is later than the given cutOffTime
    * @param cutOffTime time to be compared
    * @return true if this span is later than the given cutOffTime time else false
    */
  def isLaterThan(cutOffTime: Long): Boolean = (time - cutOffTime) > 0

  override def equals(obj: scala.Any): Boolean = {
    val that = obj.asInstanceOf[LightSpan]
    (this.spanId.equals(that.spanId)
    && this.parentSpanId.equals(that.parentSpanId)
    && this.serviceName.equalsIgnoreCase(that.serviceName))
  }
}

object LightSpanBuilder {
  def updateSpanType(span: LightSpan, spanType: SpanType): LightSpan = {
    LightSpan(span.spanId,
      span.parentSpanId,
      span.time,
      span.serviceName,
      span.operationName,
      span.duration,
      spanType,
      span.tags)
  }
}