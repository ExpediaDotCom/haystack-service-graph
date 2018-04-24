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
package com.expedia.www.haystack.service.graph.graph.builder.app

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.service.graph.graph.builder.{ServiceGraph, TestSpec}
import com.expedia.www.haystack.service.graph.graph.builder.model.GraphEdge
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}
import org.easymock.EasyMock._

class ServiceGraphAccumulatorSpec extends TestSpec {
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
      val accumulator = new ServiceGraphAccumulator(1000)
      When("accumulator is initialized")
      accumulator.init(context)
      Then("it should schedule punctuation")
      verify(context)
    }

    it("should collect all graph edges") {
      Given("an accumulator")
      val accumulator = new ServiceGraphAccumulator(1000)

      When("2 edges are processed")
      accumulator.process("testA", GraphEdge("a", "x", "op"))
      accumulator.process("testB", GraphEdge("b", "y", "op"))

      Then("accumulator should contain 2 edges")
      accumulator.serviceGraphSize should be(2)
    }
  }
  describe("span accumulator supplier") {
    it("should supply a valid accumulator") {
      Given("a supplier instance")
      val supplier = new ServiceGraphAccumulatorSupplier(1000)
      When("an accumulator instance is request")
      val producer = supplier.get()
      Then("should yield a valid producer")
      producer should not be null
    }
  }
}
