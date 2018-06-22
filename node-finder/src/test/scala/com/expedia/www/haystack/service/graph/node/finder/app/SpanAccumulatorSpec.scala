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
package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities.TagKeys
import com.expedia.www.haystack.commons.graph.GraphEdgeTagCollector
import com.expedia.www.haystack.service.graph.node.finder.model.SpanPair
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}
import org.easymock.EasyMock._

class SpanAccumulatorSpec extends TestSpec {
  describe("a span accumulator") {
    it("should schedule Punctuator on init") {
      Given("a processor context")
      val context = mock[ProcessorContext]
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable])
          .once()
      }
      replay(context)
      val accumulator = new SpanAccumulator(1000, new GraphEdgeTagCollector())
      When("accumulator is initialized")
      accumulator.init(context)
      Then("it should schedule punctuation")
      verify(context)
    }

    it("should collect all Client or Server Spans provided for processing") {
      Given("an accumulator")
      val accumulator = new SpanAccumulator(1000, new GraphEdgeTagCollector())
      When("10 server, 10 client and 10 other spans are processed")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan)
      producers.foreach(producer => writeSpans(10, 1000, producer, (span) => accumulator.process(span.getSpanId, span)))
      Then("accumulator should hold only the 10 client and 10 server spans")
      accumulator.spanCount should be (20)
    }

    it("should emit SpanPair instances only for pairs of server and client spans") {
      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(10)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("50 spans are written to it, with 10 client, 10 server, 10 other and 10 pairs of server and client")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan, produceClientAndServerSpans)
      producers.foreach(producer => writeSpans(10, 1000, producer, (span) => accumulator.process(span.getSpanId, span)))
      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())
      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be (0)
    }
  }
  describe("span accumulator supplier") {
    it("should supply a valid accumulator") {
      Given("a supplier instance")
      val supplier = new SpanAccumulatorSupplier(1000, new GraphEdgeTagCollector())
      When("an accumulator instance is request")
      val producer = supplier.get()
      Then("should yield a valid producer")
      producer should not be null
    }
  }
}
