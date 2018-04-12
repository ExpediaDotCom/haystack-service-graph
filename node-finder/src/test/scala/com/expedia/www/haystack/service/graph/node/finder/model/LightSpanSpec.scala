package com.expedia.www.haystack.service.graph.node.finder.model

import java.util.UUID

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities.{MetricPoint, MetricType, TagKeys}

class LightSpanSpec extends TestSpec {
  describe("an incomplete LightSpan") {
    it("should return no graph edge") {
      Given("an incomplete spanlite")
      val spanLite = new LightSpan(UUID.randomUUID().toString)
      When("get graphEdge is invoked")
      val graphEdge = spanLite.getGraphEdge
      Then("it should return None")
      graphEdge should be(None)
    }
    it("should return no metric points") {
      Given("an incomplete spanlite")
      val spanLite = new LightSpan(UUID.randomUUID().toString)
      When("get latency is invoked")
      val latency = spanLite.getLatency
      Then("it should return None")
      latency should be(None)
    }
    it("should still be incomplete when a span is merged with incorrect spanlite (non matching spanId)") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new LightSpan(spanId)
      val (span, spanType) = newSpan("foo-service", "bar", 1000, client = true, server = false)
      val weighableSpan = newWeighableSpan(span, spanType)
      When("a CLIENT span is merged")
      spanLite.merge(weighableSpan)
      Then("it should not be merged")
      spanLite.isComplete should be(false)
    }
    it("should still be incomplete when only CLIENT or SERVER span is merged with spanlite") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new LightSpan(spanId)
      val (span, spanType) = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      val weighableSpan = newWeighableSpan(span, spanType)
      When("a CLIENT span is merged")
      spanLite.merge(weighableSpan)
      Then("it should be merged yet stay incomplete")
      spanLite.isComplete should be(false)
    }
    it("should be complete when CLIENT and SERVER spans are merged with spanlite") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new LightSpan(spanId)
      val (clientSpan, clientSpanType) = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      val (serverSpan, serverSpanType) = newSpan(spanId, "foo-service", "bar", 1000, client = false, server = true)
      When("a CLIENT span is merged")
      spanLite.merge(newWeighableSpan(clientSpan, clientSpanType))
      spanLite.merge(newWeighableSpan(serverSpan, serverSpanType))
      Then("it should be merged and turn complete")
      spanLite.isComplete should be(true)
    }
  }
  describe("a complete span") {
    it("should return a valid graphEdge") {
      Given("a complete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new LightSpan(spanId)
      val (clientSpan, clientSpanType) = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      val (serverSpan, serverSpanType) = newSpan(spanId, "baz-service", "bar", 1000, client = false, server = true)
      spanLite.merge(newWeighableSpan(clientSpan, clientSpanType))
      spanLite.merge(newWeighableSpan(serverSpan, serverSpanType))
      When("get graphEdge is called")
      val graphEdge = spanLite.getGraphEdge
      Then("it should return a valid graphEdge")
      spanLite.isComplete should be(true)
      graphEdge.get should be(GraphEdge("foo-service", "baz-service", "bar"))
    }
    it("should return valid metricPoints") {
      Given("a complete spanlite")
      val clientSend = System.currentTimeMillis()
      val serverReceive = clientSend + 500
      val spanId = UUID.randomUUID().toString
      val spanLite = new LightSpan(spanId)
      val (clientSpan, clientSpanType) = newSpan(spanId, clientSend, "foo-service", "bar", 1500, client = true, server = false)
      val (serverSpan, serverSpanType) = newSpan(spanId, serverReceive, "baz-service", "bar", 500, client = false, server = true)
      spanLite.merge(newWeighableSpan(clientSpan, clientSpanType))
      spanLite.merge(newWeighableSpan(serverSpan, serverSpanType))
      When("get Latency is called")
      val metricPoint = spanLite.getLatency.get
      Then("it should return a valid latency pairs")
      spanLite.isComplete should be(true)
      metricPoint should be(MetricPoint("latency", MetricType.Gauge, Map(TagKeys.SERVICE_NAME_KEY -> "foo-service", TagKeys.OPERATION_NAME_KEY -> "bar"), 1000, clientSend / 1000))
    }
  }

}
