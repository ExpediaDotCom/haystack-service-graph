package com.expedia.www.haystack

import java.util.UUID

import com.expedia.open.tracing.{Log, Span, Tag}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanUtils
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

trait UnitTestSpec extends FunSpec with GivenWhenThen with Matchers with EasyMockSugar {
  def newSpan(serviceName: String, operation: String, duration: Long, client: Boolean, server: Boolean) : Span = {
    newSpan(UUID.randomUUID().toString, serviceName, operation, duration, client, server)
  }

  def newSpan(spanId: String, serviceName: String, operation: String, duration: Long, client: Boolean, server: Boolean) : Span = {
    val ts = System.currentTimeMillis() - (10 * 1000)
    newSpan(spanId, ts, serviceName, operation, duration, client, server)
  }

  def newSpan(spanId: String, ts: Long, serviceName: String, operation: String, duration: Long, client: Boolean, server: Boolean) : Span = {
    val spanBuilder = Span.newBuilder()
    spanBuilder.setTraceId(UUID.randomUUID().toString)
    spanBuilder.setSpanId(spanId)
    spanBuilder.setServiceName(serviceName)
    spanBuilder.setOperationName(operation)
    spanBuilder.setStartTime(ts)
    spanBuilder.setDuration(duration)

    val logBuilder = Log.newBuilder()
    if (client) {
      logBuilder.setTimestamp(ts)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.CLIENT_SEND_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      logBuilder.clear()
      logBuilder.setTimestamp(ts + duration)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.CLIENT_RECV_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
    }

    if (server) {
      logBuilder.setTimestamp(ts)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.SERVER_RECV_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      logBuilder.clear()
      logBuilder.setTimestamp(ts + duration)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.SERVER_SEND_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
    }

    spanBuilder.build()
  }
}
