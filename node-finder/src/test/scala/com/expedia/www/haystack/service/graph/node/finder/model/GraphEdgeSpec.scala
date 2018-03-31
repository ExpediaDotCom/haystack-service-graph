package com.expedia.www.haystack.service.graph.node.finder.model

import com.expedia.www.haystack.UnitTestSpec

class GraphEdgeSpec extends UnitTestSpec {
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
