package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities.MetricPoint
import org.apache.kafka.streams.processor.ProcessorContext
import org.easymock.EasyMock._

class LatencyProducerSpec extends TestSpec {
  describe("latency producer") {
    it("should produce latency metric for complete SlimSpan") {
      Given("a valid SlimSpan instance")
      val spanLite = validSlimSpan()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SlimSpan")
      expecting {
        context.forward(anyString(), isA(classOf[MetricPoint])).once()
        context.commit().once()
      }
      replay(context)
      latencyProducer.init(context)
      latencyProducer.process(spanLite.spanId, spanLite)
      Then("it should produce a metric point in the context")
      verify(context)
    }
    it("should produce no metrics for incomplete SlimSpan") {
      Given("an incomplete SlimSpan instance")
      val spanLite = inCompleteSlimSpan()
      val context = mock[ProcessorContext]
      val latencyProducer = new LatencyProducer
      When("process is invoked with a complete SlimSpan")
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
