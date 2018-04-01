package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.UnitTestSpec
import com.expedia.www.haystack.commons.entities.MetricPoint
import org.apache.kafka.streams.processor.ProcessorContext
import org.easymock.EasyMock._

class LatencyProducerSpec extends UnitTestSpec {
  describe("latency producer") {
    it("should produce two latency metrics for complete SpanLite") {
      Given("a valid SpanLite instance")
      val spanLite = validSpanLite()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SpanLite")
      expecting {
        context.forward(anyString(), isA(classOf[MetricPoint])).times(2)
        context.commit().once()
      }
      replay(context)
      latencyProducer.init(context)
      latencyProducer.process(spanLite.spanId, spanLite)
      Then("it should produce two metric points in the context")
      verify(context)
    }
    it("should produce no metrics for incomplete SpanLite") {
      Given("an incomplete SpanLite instance")
      val spanLite = inCompleteSpanLite()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SpanLite")
      expecting {
        context.commit().once()
      }
      replay(context)
      latencyProducer.init(context)
      latencyProducer.process(spanLite.spanId, spanLite)
      Then("it should produce no metric points in the context")
      verify(context)
    }
  }
}
