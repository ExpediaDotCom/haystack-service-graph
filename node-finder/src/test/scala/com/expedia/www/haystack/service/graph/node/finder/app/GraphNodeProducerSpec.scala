package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.TestSpec
import org.apache.kafka.streams.processor.ProcessorContext
import org.easymock.EasyMock._

class GraphNodeProducerSpec extends TestSpec {
  describe("producing graph nodes") {
    it("should emit a valid graph node for a give complete SlimSpan") {
      Given("a valid SlimSpan instance")
      val spanLite = validSlimSpan()
      val context = mock[ProcessorContext]
      val graphNodeProducer = new GraphNodeProducer
      val captured = newCapture[String]()
      When("process is called on GraphNodeProducer with it")
      expecting {
        context.forward(anyString(), capture[String](captured)).once()
        context.commit().once()
      }
      replay(context)
      graphNodeProducer.init(context)
      graphNodeProducer.process(spanLite.spanId, spanLite)
      val json = captured.getValue
      Then("it should produce a valid GraphNode object")
      verify(context)
      json should be ("{\"source\":\"foo-service\",\"destination\":\"baz-service\",\"operation\":\"bar\"}")
    }
    it("should emit no graph nodes for incomplete SpanLit") {
      Given("an incomplete SlimSpan instance")
      val spanLite = inCompleteSlimSpan()
      val context = mock[ProcessorContext]
      val graphNodeProducer = new GraphNodeProducer
      When("process is called on GraphNodeProducer with it")
      expecting {
        context.commit().once()
      }
      replay(context)
      graphNodeProducer.init(context)
      graphNodeProducer.process(spanLite.spanId, spanLite)
      Then("it should produce no graph node in the context")
      verify(context)
    }
  }
  describe("graph node producer supplier") {
    it("should supply a valid producer") {
      Given("a supplier instance")
      val supplier = new GraphNodeProducerSupplier
      When("a producer is request")
      val producer = supplier.get()
      Then("should yield a valid producer")
      producer should not be null
    }
  }
}
