package com.expedia.www.haystack.service.graph.node.finder.model

import java.util.UUID

import com.expedia.www.haystack.UnitTestSpec
import com.expedia.www.haystack.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType

class SpanLiteSpec extends UnitTestSpec {
  describe("an incomplete SpanLite") {
    it("should return no graph edge") {
      Given("an incomplete spanlite")
      val spanLite = new SpanLite(UUID.randomUUID().toString)
      When("get graphEdge is invoked")
      val graphEdge = spanLite.getGraphEdge
      Then("it should return None")
      graphEdge should be (None)
    }
    it("should return no metric points") {
      Given("an incomplete spanlite")
      val spanLite = new SpanLite(UUID.randomUUID().toString)
      When("get latency is invoked")
      val latency = spanLite.getLatency
      Then("it should return None")
      latency should be (None)
    }
    it("should still be incomplete when a span is merged with incorrect spanlite (non matching spanId)") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new SpanLite(spanId)
      val span = newSpan("foo-service", "bar", 1000, client = true, server = false)
      When("a CLIENT span is merged")
      val merged = spanLite.merge(span, SpanType.CLIENT)
      Then("it should not be merged")
      merged should be (false)
      spanLite.isComplete should be (false)
    }
    it("should still be incomplete when only CLIENT or SERVER span is merged with spanlite") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new SpanLite(spanId)
      val span = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      When("a CLIENT span is merged")
      val merged = spanLite.merge(span, SpanType.CLIENT)
      Then("it should be merged yet stay incomplete")
      merged should be (true)
      spanLite.isComplete should be (false)
    }
    it("should be complete when CLIENT and SERVER spans are merged with spanlite") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new SpanLite(spanId)
      val clientSpan = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      val serverSpan = newSpan(spanId, "foo-service", "bar", 1000, client = false, server = true)
      When("a CLIENT span is merged")
      val merged = spanLite.merge(clientSpan, SpanType.CLIENT)
      val merged2 = spanLite.merge(serverSpan, SpanType.SERVER)
      Then("it should be merged and turn complete")
      merged should be (true)
      spanLite.isComplete should be (true)
    }
  }
  describe("a complete span") {
    it("should return a valid graphEdge") {
      Given("a complete spanlite")
      val spanId = UUID.randomUUID().toString
      val spanLite = new SpanLite(spanId)
      val clientSpan = newSpan(spanId, "foo-service", "bar", 1000, client = true, server = false)
      val serverSpan = newSpan(spanId, "baz-service", "bar", 1000, client = false, server = true)
      spanLite.merge(clientSpan, SpanType.CLIENT)
      spanLite.merge(serverSpan, SpanType.SERVER)
      When("get graphEdge is called")
      val graphEdge = spanLite.getGraphEdge
      Then("it should return a valid graphEdge")
      spanLite.isComplete should be (true)
      graphEdge.get should be (GraphEdge("foo-service", "baz-service", "bar"))
    }
    it("should return valid metricPoints") {
      Given("a complete spanlite")
      val clientSend = System.currentTimeMillis()
      val serverReceive = clientSend + 500
      val spanId = UUID.randomUUID().toString
      val spanLite = new SpanLite(spanId)
      val clientSpan = newSpan(spanId, clientSend, "foo-service", "bar", 1500, client = true, server = false)
      val serverSpan = newSpan(spanId, serverReceive, "baz-service", "bar", 500, client = false, server = true)
      spanLite.merge(clientSpan, SpanType.CLIENT)
      spanLite.merge(serverSpan, SpanType.SERVER)
      When("get Latency is called")
      val metricPoint = spanLite.getLatency.get
      Then("it should return a valid latency pairs")
      spanLite.isComplete should be (true)
      metricPoint should be (MetricPoint("foo-service.bar.latency", MetricType.Gauge, Map.empty, 1000, clientSend))
    }
  }

}
