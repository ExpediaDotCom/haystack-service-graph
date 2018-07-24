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

import java.util.UUID

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities._
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanType

class SpanPairSpec extends TestSpec {
  describe("an incomplete SpanPair") {
    it("should return no graph edge") {
      Given("an incomplete spanlite")
      val spanPair = new SpanPair(UUID.randomUUID().toString, randomLightSpan())

      When("get graphEdge is invoked")
      val graphEdge = spanPair.getGraphEdge

      Then("it should return None")
      graphEdge should be(None)
    }
    it("should return no metric points") {
      Given("an incomplete spanlite")
      val spanPair = new SpanPair(UUID.randomUUID().toString, randomLightSpan())

      When("get latency is invoked")
      val latency = spanPair.getLatency

      Then("it should return None")
      latency should be(None)
    }

    it("should be incomplete when only CLIENT or only SERVER spans are merged") {
      Given("an incomplete spanlite")
      val spanId = UUID.randomUUID().toString
      val parentSpanId = UUID.randomUUID().toString
      val clientLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "foo-service", "bar", 1000, SpanType.CLIENT)
      val anotherClientLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "foo-service", "bar", 1000, SpanType.CLIENT)

      When("a CLIENT span is merged")
      val spanPair = new SpanPair(spanId, clientLightSpan)
      spanPair.mergeUsingSpanType(anotherClientLightSpan)

      Then("it should be merged yet stay incomplete")
      spanPair.isComplete should be(false)
    }

    it("should be complete when CLIENT and SERVER spans are merged with span pair") {
      Given("an incomplete span pair")
      val spanId = UUID.randomUUID().toString
      val parentSpanId = UUID.randomUUID().toString
      val clientLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "foo-service", "bar", 1000, SpanType.CLIENT)
      val serverLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "foo-service", "bar", 1000, SpanType.SERVER)
      val spanPair = new SpanPair(spanId, clientLightSpan)

      When("a SERVER span is merged")
      spanPair.mergeUsingSpanType(serverLightSpan)

      Then("it should be merged and turn complete")
      spanPair.isComplete should be(true)
    }
  }

  describe("a complete span") {
    it("should return a valid graphEdge") {
      Given("a complete spanlite")
      val spanId = UUID.randomUUID().toString
      val parentSpanId = UUID.randomUUID().toString
      val clientLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "foo-service", "bar", 1000, SpanType.CLIENT)
      val serverLightSpan = newLightSpan(spanId, parentSpanId, System.currentTimeMillis(), "baz-service", "bar", 1000, SpanType.SERVER)

      val spanPair = new SpanPair(spanId, clientLightSpan)
      spanPair.mergeUsingSpanType(serverLightSpan)

      When("get graphEdge is called")
      val graphEdge = spanPair.getGraphEdge

      Then("it should return a valid graphEdge")
      spanPair.isComplete should be(true)
      graphEdge.get should be(GraphEdge(GraphVertex("foo-service"), GraphVertex("baz-service"), "bar"))
    }

    it("should return valid metricPoints") {
      Given("a complete span pair")
      val clientSend = System.currentTimeMillis()
      val serverReceive = clientSend + 500
      val spanId = UUID.randomUUID().toString
      val parentSpanId = UUID.randomUUID().toString
      val clientLightSpan = newLightSpan(spanId, parentSpanId, clientSend, "foo-service", "bar", 1500, SpanType.CLIENT)
      val serverLightSpan = newLightSpan(spanId, parentSpanId, serverReceive, "foo-service", "bar", 500, SpanType.SERVER)
      val spanPair = new SpanPair(spanId, clientLightSpan)

      spanPair.mergeUsingSpanType(serverLightSpan)

      When("get Latency is called")
      val metricPoint = spanPair.getLatency.get

      Then("it should return a valid latency pairs")
      spanPair.isComplete should be(true)
      metricPoint should be(MetricPoint("latency", MetricType.Gauge, Map(TagKeys.SERVICE_NAME_KEY -> "foo-service", TagKeys.OPERATION_NAME_KEY -> "bar"), 1000, clientSend / 1000))
    }
  }

}
