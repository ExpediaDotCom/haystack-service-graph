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
package com.expedia.www.haystack

import java.util.UUID

import com.expedia.open.tracing.Tag.TagType
import com.expedia.open.tracing.{Log, Span, Tag}
import com.expedia.www.haystack.service.graph.node.finder.model.{SpanPair, WeighableSpan}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType.SpanType
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

trait TestSpec extends FunSpec with GivenWhenThen with Matchers with EasyMockSugar {

  def newWeighableSpan(span: Span, spanType: SpanType, tags: Map[String, String] = Map()) : WeighableSpan = {
    WeighableSpan(span.getSpanId, span.getStartTime / 1000, span.getServiceName, span.getOperationName, span
      .getDuration, spanType, tags)
  }

  def newSpan(serviceName: String, operation: String, duration: Long, client: Boolean, server: Boolean): (Span, SpanType) = {
    newSpan(UUID.randomUUID().toString, serviceName, operation, duration, client, server)
  }

  def newSpan(spanId: String, serviceName: String, operation: String, duration: Long, client: Boolean, server: Boolean): (Span, SpanType) = {
    val ts = System.currentTimeMillis() - (10 * 1000)
    newSpan(spanId, ts, serviceName, operation, duration, client, server)
  }

  def newSpan(spanId: String, ts: Long, serviceName: String, operation: String, duration: Long, client: Boolean,
              server: Boolean, tags: Map[String, String] = Map()): (Span, SpanType) = {
    val spanBuilder = Span.newBuilder()
    spanBuilder.setTraceId(UUID.randomUUID().toString)
    spanBuilder.setSpanId(spanId)
    spanBuilder.setServiceName(serviceName)
    spanBuilder.setOperationName(operation)
    spanBuilder.setStartTime(ts * 1000)  //microseconds
    spanBuilder.setDuration(duration)
    var spanType = SpanType.OTHER

    val logBuilder = Log.newBuilder()
    if (client) {
      logBuilder.setTimestamp(ts)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.CLIENT_SEND_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      logBuilder.clear()
      logBuilder.setTimestamp(ts + duration)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.CLIENT_RECV_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      spanType = SpanType.CLIENT
    }

    if (server) {
      logBuilder.setTimestamp(ts)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.SERVER_RECV_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      logBuilder.clear()
      logBuilder.setTimestamp(ts + duration)
      logBuilder.addFields(Tag.newBuilder().setKey("event").setVStr(SpanUtils.SERVER_SEND_EVENT).build())
      spanBuilder.addLogs(logBuilder.build())
      spanType = SpanType.SERVER
    }
    if (tags.nonEmpty) {
      val tagBuilder = Tag.newBuilder();
      tags.foreach(tag => {
        tagBuilder.setKey(tag._1).setVStr(tag._2).setType(TagType.STRING)
        spanBuilder.addTags(tagBuilder.build())
        tagBuilder.clear()
      })
    }

    (spanBuilder.build(), spanType)
  }

  def produceSimpleSpan(offset: Long, callback: (Span) => Unit): Unit =
    callback(newSpan(UUID.randomUUID().toString,
      System.currentTimeMillis() - offset,
      "foo-service", "bar", 1500, client = false, server = false)._1)

  def produceClientSpan(offset: Long, callback: (Span) => Unit): Unit =
    callback(newSpan(UUID.randomUUID().toString,
      System.currentTimeMillis() - offset,
      "foo-service", "bar", 1500, client = true, server = false)._1)

  def produceServerSpan(offset: Long, callback: (Span) => Unit): Unit =
    callback(newSpan(UUID.randomUUID().toString,
      System.currentTimeMillis() - offset,
      "baz-service", "bar", 500, client = false, server = true)._1)

  def produceClientAndServerSpans(offset: Long, callback: (Span) => Unit): Unit = {
    val clientSend = System.currentTimeMillis() - offset
    val serverReceive = clientSend + 500
    val spanId = UUID.randomUUID().toString
    val source = "foo-service"
    val op = "bar"
    val dest = "baz-service"
    val (clientSpan, _) = newSpan(spanId, clientSend, source, op, 1500, client = true, server = false)
    val (serverSpan, _)  = newSpan(spanId, serverReceive, dest, op, 500, client = false, server = true)
    callback(clientSpan)
    callback(serverSpan)
  }

  def writeSpans(count: Int,
                 startOffset: Long,
                 producer: (Long, (Span) => Unit) => Unit,
                 consumer: (Span) => Unit): Unit = {
    require(count >= 1)
    var i = count
    while (i >= 1) {
      producer(i * startOffset, consumer)
      i -= 1
    }
  }

  def inCompleteSpanPair(): SpanPair = {
    val spanId = UUID.randomUUID().toString
    val spanPair = new SpanPair(spanId)
    val (span, spanType) = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
    spanPair.merge(newWeighableSpan(span, spanType))
    spanPair
  }

  def validSpanPair(tags: Map[String, String] = Map()): SpanPair = {
    val clientSend = System.currentTimeMillis()
    val serverReceive = clientSend + 500
    val spanId = UUID.randomUUID().toString
    val spanPair = new SpanPair(spanId)
    val (clientSpan, clientSpanType) = newSpan(spanId, clientSend, "foo-service", "bar", 1500, client = true, server
      = false, tags)
    val (serverSpan, serverSpanType) = newSpan(spanId, serverReceive, "baz-service", "bar", 500, client = false,
      server = true, tags)
    spanPair.merge(newWeighableSpan(clientSpan, clientSpanType, tags))
    spanPair.merge(newWeighableSpan(serverSpan, serverSpanType, tags))
    spanPair
  }
}
