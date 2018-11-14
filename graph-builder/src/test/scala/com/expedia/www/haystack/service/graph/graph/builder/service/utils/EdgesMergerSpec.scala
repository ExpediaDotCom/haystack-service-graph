package com.expedia.www.haystack.service.graph.graph.builder.service.utils

import java.lang.Math.{max, min}

import com.expedia.www.haystack.service.graph.graph.builder.model.{ServiceEdgeStats, ServiceGraphEdge, ServiceGraphVertex}
import org.scalatest.{FunSpec, Matchers}

class EdgesMergerSpec extends FunSpec with Matchers {

  describe("EdgesMerger.getMergedServiceEdges()") {
    val vertexA: ServiceGraphVertex = ServiceGraphVertex("serviceGraphVertexA", Map.empty)
    val vertexB: ServiceGraphVertex = ServiceGraphVertex("serviceGraphVertexB", Map.empty)
    val vertexC: ServiceGraphVertex = ServiceGraphVertex("serviceGraphVertexC", Map.empty)
    val stats1 = ServiceEdgeStats(1, 10, 100)
    val stats3 = ServiceEdgeStats(3, 30, 300)
    val stats5 = ServiceEdgeStats(5, 50, 500)
    val stats7 = ServiceEdgeStats(7, 70, 700)
    val edgeAB1 = ServiceGraphEdge(vertexA, vertexB, stats1, 1000, 10000)
    val edgeAB3 = ServiceGraphEdge(vertexA, vertexB, stats3, 3000, 30000)
    val edgeAC5 = ServiceGraphEdge(vertexA, vertexC, stats5, 5000, 50000)
    val edgeBC7 = ServiceGraphEdge(vertexB, vertexC, stats7, 7000, 70000)
    it("should create two edges when source matches but destination does not") {
      val mergedEdges = EdgesMerger.getMergedServiceEdges(Seq(edgeAB1, edgeAC5))
      mergedEdges.size should equal(2)
      mergedEdges should contain(edgeAB1)
      mergedEdges should contain(edgeAC5)
    }
    it("should create two edges when destination matches but source does not") {
      val mergedEdges = EdgesMerger.getMergedServiceEdges(Seq(edgeAC5, edgeBC7))
      mergedEdges.size should equal(2)
      mergedEdges should contain(edgeAC5)
      mergedEdges should contain(edgeBC7)
    }
    it("should merge two edges when source and destination match") {
      val mergedEdges = EdgesMerger.getMergedServiceEdges(Seq(edgeAB1, edgeAB3))
      mergedEdges.size should equal(1)
      val mergedEdge: ServiceGraphEdge = mergedEdges.head
      mergedEdge.source should equal(vertexA)
      mergedEdge.destination should equal(vertexB)
      mergedEdge.effectiveFrom should equal(min(edgeAB1.effectiveFrom, edgeAB3.effectiveFrom))
      mergedEdge.effectiveTo should equal(max(edgeAB1.effectiveTo, edgeAB3.effectiveTo))
      mergedEdge.stats.count should equal(edgeAB1.stats.count + edgeAB3.stats.count)
      mergedEdge.stats.lastSeen should equal(max(edgeAB1.stats.lastSeen, edgeAB3.stats.lastSeen))
      mergedEdge.stats.errorCount should equal(edgeAB1.stats.errorCount + edgeAB3.stats.errorCount)
    }
  }
}
