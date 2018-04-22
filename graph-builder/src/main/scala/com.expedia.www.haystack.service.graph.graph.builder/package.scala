package com.expedia.www.haystack.service.graph.graph

import java.util

import com.expedia.www.haystack.service.graph.graph.builder.model.GraphEdge

package object builder {
  type ServiceGraph = util.HashSet[GraphEdge]
}
