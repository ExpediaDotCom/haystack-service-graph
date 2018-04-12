package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities.MetricPoint
import org.apache.kafka.streams.processor.ProcessorContext
import org.easymock.EasyMock._

class LatencyProducerSpec extends TestSpec {
  describe("latency producer") {
    it("should produce latency metric for complete SpanPair") {
      Given("a valid SpanPair instance")
      val spanPair = validSpanPair()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SpanPair")
      expecting {
        context.forward(anyString(), isA(classOf[MetricPoint])).once()
        context.commit().once()
      }
      replay(context)
      latencyProducer.init(context)
      latencyProducer.process(spanPair.spanId, spanPair)
      Then("it should produce a metric point in the context")
      verify(context)
    }
    it("should produce no metrics for incomplete SpanPair") {
      Given("an incomplete SpanPair instance")
      val spanPair = inCompleteSpanPair()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SpanPair")
      expecting {
        context.commit().once()
      }
      replay(context)
      latencyProducer.init(context)
      latencyProducer.process(spanPair.spanId, spanPair)
      Then("it should produce no metric points in the context")
      verify(context)
    }
  }
  describe("latency producer supplier") {
    it("should supply a valid producer") {
      Given("a supplier instance")
      val supplier = new LatencyProducerSupplier
      When("a producer is request")
      val producer = supplier.get()
      Then("should yield a valid producer")
      producer should not be null
    }
  }
}
