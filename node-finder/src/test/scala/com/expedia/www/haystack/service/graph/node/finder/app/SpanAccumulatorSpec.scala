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
import com.expedia.www.haystack.commons.graph.GraphEdgeTagCollector
import com.expedia.www.haystack.service.graph.node.finder.model.SpanPair
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.Stores
import org.easymock.EasyMock._

class SpanAccumulatorSpec extends TestSpec {
  private val storeName = "my-store"

  describe("a span accumulator") {
    it("should schedule Punctuator on init") {
      Given("a processor context")
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable])
          .once()
        context.getStateStore(storeName).andReturn(stateStore)
      }
      replay(context)
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      When("accumulator is initialized")
      accumulator.init(context)
      Then("it should schedule punctuation")
      verify(context)
    }

    it("should collect all Client or Server Spans provided for processing") {
      Given("an accumulator")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      When("10 server, 10 client and 10 other spans are processed")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan)
      producers.foreach(producer => writeSpans(10, 1000, producer, (span) => accumulator.process(span.getSpanId, span)))
      Then("accumulator should hold only the 10 client and 10 server spans")
      accumulator.spanCount should be(30)
    }

    it("should emit SpanPair instances only for pairs of server and client spans") {
      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(10)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("50 spans are written to it, with 10 client, 10 server, 10 other and 10 pairs of server and client")
      val producers = List[(Long, (Span) => Unit) => Unit](produceSimpleSpan,
        produceServerSpan, produceClientSpan, produceClientAndServerSpans)
      producers.foreach(producer => writeSpans(10, 2500, producer, (span) => accumulator.process(span.getSpanId, span)))
      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }
  }

  describe("create span pair using ids") {
    it("should emit SpanPair instances for parent-child relation using ids") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(4)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 5 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1"),
        newSpan("I3", "I1", "svc1"),
        newSpan("I4", "I3", "svc2"),
        newSpan("I5", "I4", "svc2"),
        newSpan("I6", "I5", "svc3"),
        newSpan("I7", "I6", "svc3"),
        newSpan("I8", "I7", "svc4"),
        newSpan("I9", "I8", "svc4"),
        newSpan("I10", "I9", "svc5"),
        newSpan("I11", "I10", "svc5")
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }

    it("should emit SpanPair instances for parent-child relation using ids with server spans") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(2)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 5 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1", "oper1", 1000, false, true), // server
        newSpan("I4", "I1", "svc2", "oper1", 1000, false, true), // server
        newSpan("I5", "I4", "svc2", "oper1", 1000, true, false),
        newSpan("I6", "I5", "svc3", "oper1", 1000, false, true), // server
        newSpan("I8", "I6", "svc4", "oper1", 1000, false, true), // server
        newSpan("I9", "I8", "svc4", "oper1", 1000, true, false),
        newSpan("I10", "I9", "svc5", "oper1", 1000, true, false),
        newSpan("I11", "I10", "svc6", "oper1", 1000, false, true)
      )
      spanList.foreach(span => accumulator.process(span._1.getSpanId, span._1))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }

    it("should emit SpanPair instances for parent-child relation using ids even if spanId and parentId is same") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(3)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 4 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1"),
        newSpan("I11", "I1", "svc4"), // child of svc1
        newSpan("I12", "I1", "svc2"), // child of svc1
        newSpan("I13", "I1", "svc3") // child of svc1
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }

    it("should emit SpanPair instances for parent-child relation using ids with (I5, I4) and (I6, I5) in reverse order") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(4)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 5 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1"),
        newSpan("I3", "I1", "svc1"),
        newSpan("I4", "I3", "svc2"),
        newSpan("I6", "I5", "svc3"), // child comes first
        newSpan("I5", "I4", "svc2"), // then comes the parent
        newSpan("I7", "I6", "svc3"),
        newSpan("I8", "I7", "svc4"),
        newSpan("I9", "I8", "svc4"),
        newSpan("I10", "I9", "svc5"),
        newSpan("I11", "I10", "svc5")
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }

    it("should emit SpanPair instances for fork relation using ids for svc4 -> svc5 & svc4 -> svc6") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(5)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 6 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1"),
        newSpan("I3", "I1", "svc1"),
        newSpan("I4", "I3", "svc2"),
        newSpan("I6", "I5", "svc3"),
        newSpan("I5", "I4", "svc2"),
        newSpan("I7", "I6", "svc3"),

        newSpan("I8", "I7", "svc4"),
        newSpan("I9", "I8", "svc4"),
        newSpan("I10", "I8", "svc4"),

        //downstream of svc4
        newSpan("I11", "I9", "svc5"),
        newSpan("I12", "I11", "svc5"),

        //downstream of svc4
        newSpan("I13", "I10", "svc6"),
        newSpan("I14", "I13", "svc6")
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }

    it("should emit valid SpanPair instances for parent-child relation ignoring duplicate spans") {

      Given("an accumulator and initialized with a processor context")
      val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
      val context = mock[ProcessorContext]
      val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
      expecting {
        context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
          .andReturn(mock[Cancellable]).once()
        context.forward(anyString(), isA(classOf[SpanPair])).times(1)
        context.getStateStore(storeName).andReturn(stateStore)
        context.commit().once()
      }
      replay(context)
      accumulator.init(context)
      And("spans from 5 services")
      val spanList = List(
        newSpan("I1", "I2", "svc1"),
        newSpan("I1", "I2", "svc1"), //duplicate server span
        newSpan("I3", "I1", "svc2"),
        newSpan("I4", "I3", "svc2")
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 1 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
    }
  }

  it("should emit valid SpanPair instances in mixed merge mode where we receive spans in zipkin and jaeger style") {

    Given("an accumulator and initialized with a processor context")
    val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
    val context = mock[ProcessorContext]
    val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
    expecting {
      context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
        .andReturn(mock[Cancellable]).once()
      context.forward(anyString(), isA(classOf[SpanPair])).times(3)
      context.getStateStore(storeName).andReturn(stateStore)
      context.commit().once()
    }
    replay(context)
    accumulator.init(context)
    And("spans from 5 services")
    val spanList = List(
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I1", "I2", "svc2"),
      newClientSpan("I2", "I1", "svc2"),
      newServerSpan("I3", "I2", "svc3"),
      newClientSpan("I4", "I3", "svc3"),
      newServerSpan("I4", "I3", "svc4")
    )
    spanList.foreach(span => accumulator.process(span.getSpanId, span))

    When("punctuate is called")
    accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

    Then("it should produce 3 SpanPair instances as expected")
    verify(context)
    And("the accumulator's collection should be empty")
    accumulator.spanCount should be(0)
  }


  it("should respect the singular(zipkin) span merge style once set") {

    Given("an accumulator and initialized with a processor context")
    val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
    val context = mock[ProcessorContext]
    val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
    expecting {
      context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
        .andReturn(mock[Cancellable]).once()
      context.forward(anyString(), isA(classOf[SpanPair])).times(3)
      context.getStateStore(storeName).andReturn(stateStore)
      context.commit().once()
    }
    replay(context)
    accumulator.init(context)
    And("spans from 5 services")
    val spanList = List(
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I1", "I2", "svc2"),
      newClientSpan("I3", "I1", "svc2"),
      newServerSpan("I3", "I1", "svc3"),

      newClientSpan("T1", "T2", "svc1"),
      newServerSpan("T3", "T1", "svc3"),
      newClientSpan("T3", "T1", "svc2")
    )
    spanList.foreach(span => accumulator.process(span.getSpanId, span))

    When("punctuate is called")
    accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

    Then("it should produce 3 SpanPair instances as expected")
    verify(context)
    And("the accumulator's collection should be empty")
    accumulator.spanCount should be(0)
  }


  it("should emit valid SpanPair instances for only zipkin styled spans") {

    Given("an accumulator and initialized with a processor context")
    val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
    val context = mock[ProcessorContext]
    val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
    expecting {
      context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
        .andReturn(mock[Cancellable]).once()
      context.forward(anyString(), isA(classOf[SpanPair])).times(3)
      context.getStateStore(storeName).andReturn(stateStore)
      context.commit().once()
    }
    replay(context)
    accumulator.init(context)
    And("spans from 5 services")
    val spanList = List(
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I1", "I2", "svc2"),
      newClientSpan("I2", "I1", "svc2"),
      newServerSpan("I2", "I1", "svc3"),
      newClientSpan("I3", "I2", "svc3"),
      newServerSpan("I3", "I2", "svc4")
    )
    spanList.foreach(span => accumulator.process(span.getSpanId, span))

    When("punctuate is called")
    accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

    Then("it should produce 4 SpanPair instances as expected")
    verify(context)
    And("the accumulator's collection should be empty")
    accumulator.spanCount should be(0)
  }


  describe("span accumulator supplier") {
    it("should supply a valid accumulator") {
      Given("a supplier instance")
      val supplier = new SpanAccumulatorSupplier(storeName, 1000, new GraphEdgeTagCollector())
      When("an accumulator instance is request")
      val producer = supplier.get()
      Then("should yield a valid producer")
      producer should not be null
    }
  }
}
