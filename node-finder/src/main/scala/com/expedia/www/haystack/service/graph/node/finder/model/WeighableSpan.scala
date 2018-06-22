package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import org.apache.commons.lang3.StringUtils

/**
  * Light weight representation of a Span with minimal information with timestamp of
  * the span element as its weight
  *
  * @param spanId        Unique identity of the Span
  * @param time          Timestamp associated with a Span in MilliSeconds (i.e., StartTime)
  * @param serviceName   Service name of the span
  * @param operationName Operation name of the span
  * @param duration      duration of the Span
  * @param spanType      type of the span
  */
case class WeighableSpan(spanId: String,
                         time: Long,
                         serviceName: String,
                         operationName: String,
                         duration: Long,
                         spanType: SpanType, tags: Map[String, String]) extends Weighable {
  require(StringUtils.isNotBlank(spanId))
  require(time > 0)
  require(StringUtils.isNotBlank(serviceName))
  require(StringUtils.isNoneBlank(operationName))
  require(spanType != null)

  override def weight(): Long = time

  def isLaterThan(cutOffTime : Long): Boolean = (time - cutOffTime) > 0
}
