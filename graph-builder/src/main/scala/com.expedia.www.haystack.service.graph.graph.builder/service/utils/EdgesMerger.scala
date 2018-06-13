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
package com.expedia.www.haystack.service.graph.graph.builder.service.utils

import com.expedia.www.haystack.service.graph.graph.builder.model.{EdgeStats, OperationGraphEdge, ServiceGraphEdge}

object EdgesMerger {
  def getMergedServiceEdges(serviceGraphEdges: List[ServiceGraphEdge]): List[ServiceGraphEdge] = {
    // group by source and destination service
    val groupedEdges = serviceGraphEdges.groupBy((edge) => ServicePair(edge.source, edge.destination))

    // go through edges grouped by source and destination
    // add counts for all edges in group to get total count for a source destination pair
    // get latest last seen for all edges in group to lastseen for a source destination pair
    groupedEdges.map(
      (group) => group._2
        .reduce((e1, e2) => ServiceGraphEdge(group._1.source, group._1.destination,
          EdgeStats(e1.stats.count + e2.stats.count, Math.max(e1.stats.lastSeen, e2.stats.lastSeen)))))
      .toList
  }

  def getMergedOperationEdge(operationGraphEdges: List[OperationGraphEdge]): List[OperationGraphEdge] = {
    // group by source and destination service
    val groupedEdges = operationGraphEdges.groupBy((edge) => OperationPair(edge.source, edge.destination, edge.operation))

    // go through edges grouped by source and destination
    // add counts for all edges in group to get total count for a source destination pair
    // get latest last seen for all edges in group to lastseen for a source destination pair
    groupedEdges.map(
      (group) => group._2
        .reduce((e1, e2) => OperationGraphEdge(group._1.source, group._1.destination, group._1.operation,
          EdgeStats(e1.stats.count + e2.stats.count, Math.max(e1.stats.lastSeen, e2.stats.lastSeen)))))
      .toList
  }

  private case class ServicePair(source: String, destination: String)

  private case class OperationPair(source: String, destination: String, operation: String)
}