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

import com.expedia.www.haystack.TestSpec

class GraphEdgeSpec extends TestSpec {
  describe("graphEdge object serialization") {
    it("should produce an expected json") {
      Given("a graph edge instance")
      val graphEdge = GraphEdge("source-service", "destination-service", "operation-name")
      When("toJson is invoked")
      val json = graphEdge.toJson
      Then("it should produce a simple json string")
      json should be ("{\"source\":\"source-service\",\"destination\":\"destination-service\",\"operation\":\"operation-name\"}")
    }
  }
}
