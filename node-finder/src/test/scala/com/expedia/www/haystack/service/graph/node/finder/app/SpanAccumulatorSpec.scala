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

import java.util

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.graph.GraphEdgeTagCollector
import com.expedia.www.haystack.service.graph.node.finder.model.{ServiceNodeMetadata, SpanPair}
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanMergeStyle
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock}

import scala.collection.JavaConverters._

class SpanAccumulatorSpec extends TestSpec {
  private val storeName = "my-store"

  describe("a span accumulator") {
    it("should schedule Punctuator on init") {
      Given("a processor context")
      val (context, _, _, _) = mockContext(0)
      When("accumulator is initialized")
      createAccumulator(context)
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
      val (context, _, _, _) = mockContext(10)
      val accumulator = createAccumulator(context)
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
      val (context, kvStore, _, _) = mockContext(4)
      val accumulator = createAccumulator(context)

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
      kvStore.get("svc1") shouldBe null
      2 until 6 foreach(id => {
        kvStore.get(s"svc$id").mergeStyle shouldBe SpanMergeStyle.DUAL
      })
    }

    it("should emit SpanPair instances for parent-child relation using ids with server spans") {
      Given("an accumulator and initialized with a processor context")
      val (context, kvStore, _, _) = mockContext(2)
      val accumulator = createAccumulator(context)

      And("spans from 5 services")
      val spanList = List(
        newServerSpan("I1", "I2", "svc1"),
        newServerSpan("I4", "I1", "svc2"),
        newClientSpan("I5", "I4", "svc2"),
        newServerSpan("I6", "I5", "svc3"),
        newServerSpan("I8", "I6", "svc4"),
        newClientSpan("I9", "I8", "svc4"),
        newClientSpan("I10", "I9", "svc5"),
        newServerSpan("I11", "I10", "svc6")
      )
      spanList.foreach(span => accumulator.process(span.getSpanId, span))

      When("punctuate is called")
      accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

      Then("it should produce 10 SpanPair instances as expected")
      verify(context)
      And("the accumulator's collection should be empty")
      accumulator.spanCount should be(0)
      Set(1, 2, 4, 5) foreach { id =>
        kvStore.get(s"svc$id") shouldBe null
      }
      Set(3, 6) foreach { id =>
        kvStore.get(s"svc$id").mergeStyle shouldBe SpanMergeStyle.DUAL
      }
    }

    it("should emit SpanPair instances for parent-child relation using ids with (I5, I4) and (I6, I5) in reverse order") {
      Given("an accumulator and initialized with a processor context")
      val (context, _, _, _) = mockContext(4)
      val accumulator = createAccumulator(context)
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
      val (context, _, _, _) = mockContext(5)
      val accumulator = createAccumulator(context)
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
      val (context, _, _, _) = mockContext(1)
      val accumulator = createAccumulator(context)
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

  it("should emit valid SpanPair instances in mixed merge mode where we receive spans in Singular(sharable) and Dual(non-sharable) style") {
    Given("an accumulator and initialized with a processor context")
    val (context, kvStore, forwardedKeys, forwardedSpanPairs) = mockContext(3)
    val accumulator = createAccumulator(context)
    And("spans from 4 services")
    val spanList = List(
      // sharable client-server span
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I1", "I2", "svc2"),

      // non-sharable client-server span
      newClientSpan("I2", "I1", "svc2"),
      newServerSpan("I3", "I2", "svc3"),

      // sharable client-server span
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
    kvStore.get("svc1") shouldBe null
    kvStore.get("svc2").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    kvStore.get("svc3").mergeStyle shouldBe SpanMergeStyle.DUAL
    kvStore.get("svc4").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    extractClientServerSvcNames(forwardedSpanPairs) should contain allOf ("svc1->svc2", "svc2->svc3", "svc3->svc4")
    forwardedKeys.getValues.asScala.toSet should contain allOf ("I1", "I2", "I4")
  }


  it("should respect the singular(sharable) span merge style once set even later if it receives dual(non-sharable) span mode") {
    Given("an accumulator and initialized with a processor context")
    val (context, kvStore, forwardedKeys, forwardedSpanPairs) = mockContext(3)
    val accumulator = createAccumulator(context)
    And("spans from 3 services")
    val spanList = List(
      // sharable client-server span
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I1", "I2", "svc2"),

      // sharable client-server span
      newClientSpan("I3", "I1", "svc2"),
      newServerSpan("I3", "I1", "svc3"),

      // one non-sharable client-server span between svc1 and svc3
      // one sharable client-server span between svc2 and svc3
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

    kvStore.get("svc1") shouldBe null
    kvStore.get("svc2").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    kvStore.get("svc3").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    extractClientServerSvcNames(forwardedSpanPairs) should contain allOf ("svc1->svc2", "svc2->svc3")
    forwardedKeys.getValues.asScala.toSet should contain allOf ("I1", "I3", "T3")
  }

  it("should auto-correct from dual to Singular merge style mode and never go back") {
    Given("an accumulator and initialized with a processor context")
    val (context, kvStore, forwardedKeys, forwardedSpanPairs) = mockContext(3)
    val accumulator = createAccumulator(context)
    And("spans from 5 services")
    val spanList = List(
      // non-sharable client-server span between svc1 and svc3
      newClientSpan("I1", "I2", "svc1"),
      newServerSpan("I3", "I1", "svc3"),

      newServerSpan("I1", "I2", "svc2"),
      newClientSpan("I3", "I1", "svc2"),

      newClientSpan("T1", "T2", "svc1"),
      newServerSpan("T3", "T1", "svc3")
    )
    spanList.foreach(span => accumulator.process(span.getSpanId, span))

    When("punctuate is called")
    accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

    Then("it should produce 3 SpanPair instances as expected")
    verify(context)
    And("the accumulator's collection should be empty")
    accumulator.spanCount should be(0)
    kvStore.get("svc1") shouldBe null
    kvStore.get("svc2").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    kvStore.get("svc3").mergeStyle shouldBe SpanMergeStyle.SINGULAR
    extractClientServerSvcNames(forwardedSpanPairs) should contain allOf ("svc1->svc2", "svc1->svc3", "svc2->svc3")
    forwardedKeys.getValues.asScala.toSet should contain allOf ("I1", "I3")
  }

  it("should emit valid SpanPair instances for only singular(sharable) styled spans") {
    Given("an accumulator and initialized with a processor context")
    val (context, _, forwardedKeys, forwardedSpanPairs) = mockContext(3)
    val accumulator = createAccumulator(context)
    And("spans from 4 services")
    val spanList = List(
      newServerSpan("I1", "I2", "svc2"),
      newServerSpan("I2", "I1", "svc3"),
      newServerSpan("I3", "I2", "svc4"),
      newClientSpan("I1", "I2", "svc1"),
      newClientSpan("I2", "I1", "svc2"),
      newClientSpan("I3", "I2", "svc3")
    )
    spanList.foreach(span => accumulator.process(span.getSpanId, span))

    When("punctuate is called")
    accumulator.getPunctuator(context).punctuate(System.currentTimeMillis())

    Then("it should produce 3 SpanPair instances as expected")
    verify(context)
    And("the accumulator's collection should be empty")
    accumulator.spanCount should be(0)
    extractClientServerSvcNames(forwardedSpanPairs) should contain allOf ("svc1->svc2", "svc2->svc3", "svc3->svc4")
    forwardedKeys.getValues.asScala.toSet should contain allOf ("I1", "I2", "I3")
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

  private def mockContext(expectedForwardCalls: Int): (ProcessorContext, KeyValueStore[String, ServiceNodeMetadata], Capture[String], Capture[SpanPair]) = {
    val context = mock[ProcessorContext]
    val stateStore = Stores.inMemoryKeyValueStore(storeName).get()
    val captureForwardedKeys = EasyMock.newCapture[String](CaptureType.ALL)
    val captureForwardedSpanPairs = EasyMock.newCapture[SpanPair](CaptureType.ALL)

    expecting {
      context.schedule(anyLong(), isA(classOf[PunctuationType]), isA(classOf[Punctuator]))
        .andReturn(mock[Cancellable]).once()

      if (expectedForwardCalls > 0) {
        context
          .forward(EasyMock.capture(captureForwardedKeys), EasyMock.capture(captureForwardedSpanPairs))
          .times(expectedForwardCalls)
        context.commit().once()
      }
      context.getStateStore(storeName).andReturn(stateStore)
    }
    replay(context)
    (context, stateStore.asInstanceOf[KeyValueStore[String, ServiceNodeMetadata]], captureForwardedKeys, captureForwardedSpanPairs)
  }

  private def createAccumulator(context: ProcessorContext): SpanAccumulator = {
    val accumulator = new SpanAccumulator(storeName, 1000, new GraphEdgeTagCollector())
    accumulator.init(context)
    accumulator
  }

  private def extractClientServerSvcNames(spanPairs: Capture[SpanPair]): Set[String] = {
    spanPairs.getValues.asScala.map(p => p.getClientSpan.serviceName + "->" + p.getServerSpan.serviceName).toSet
  }
}
