package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.UnitTestSpec
import com.expedia.www.haystack.service.graph.node.finder.model.SpanLite
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}
import org.easymock.EasyMock._

class SpanAggregatorSpec extends UnitTestSpec {
  describe("a span aggregator") {
    it("should schedule Punctuator on init") {
      Given("a processor context")
      val context = mock[ProcessorContext]
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable])
          .once()
      }
      replay(context)
      val aggregator = new SpanAggregator(1000)
      When("aggregator is initialized")
      aggregator.init(context)
      Then("it should schedule punctuation")
      verify(context)
    }

    it("should collect all Client or Server Spans provided for processing") {
      Given("an aggregator")
      val aggregator = new SpanAggregator(1000)
      When("10 server, 10 client and 10 other spans are processed")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan)
      producers.foreach(producer => writeSpans(10, 1000, producer, (span) => aggregator.process(span.getSpanId, span)))
      Then("aggregator should hold only the 10 client and 10 server spans")
      aggregator.spanCount should be (20)
    }

    it("should emit SpanLite instances only for pairs of server and client spans") {
      Given("an aggregator and initialized with a processor context")
      val aggregator = new SpanAggregator(1000)
      val context = mock[ProcessorContext]
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanLite])).times(10)
        context.commit().once()
      }
      replay(context)
      aggregator.init(context)
      And("50 spans are written to it, with 10 client, 10 server, 10 other and 10 pairs of server and client")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan, produceClientAndServerSpans)
      producers.foreach(producer => writeSpans(10, 1000, producer, (span) => aggregator.process(span.getSpanId, span)))
      When("punctuate is called")
      aggregator.punctuate(System.currentTimeMillis())
      Then("it should produce 10 SpanLite instances as expected")
      verify(context)
      And("the aggregator's collection should be empty")
      aggregator.spanCount should be (0)
    }
  }
}
