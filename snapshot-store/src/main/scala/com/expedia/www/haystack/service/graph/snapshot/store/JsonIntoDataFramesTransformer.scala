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
package com.expedia.www.haystack.service.graph.snapshot.store

import com.expedia.www.haystack.service.graph.snapshot.store.Constants._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

class JsonIntoDataFramesTransformer {
  private val SourceId = SourceKey + IdKey.capitalize
  private val DestinationId = DestinationKey + IdKey.capitalize
  private val StatsCount = StatsKey + CountKey.capitalize
  private val StatsLastSeen = StatsKey + LastSeenKey.capitalize
  private val StatsErrorCount = StatsKey + ErrorCountKey.capitalize

  def parseJson(jsonInput: String): NodesAndEdges = {
    val jValue = parse(jsonInput, useBigDecimalForDouble = false, useBigIntForLong = true)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val map = jValue.extract[Map[String, Any]]
    val edgesList = map(EdgesKey).asInstanceOf[List[Any]]
    val nodeToIdMap = getNodeToIdMap(edgesList)
    val nodes = createNodesCsvList(nodeToIdMap)
    val edges = createEdgesCsvList(edgesList, nodeToIdMap)
    NodesAndEdges(nodes, edges)
  }

  private val EdgesFormatString = "%s,%s,%s,%s,%s,%s,%s,%s\n"
  private def createEdgesCsvList(edgesList: List[Any], nodeToIdMap: mutable.Map[Node, Int]): String = {
    val stringBuilder = new mutable.StringBuilder(EdgesFormatString.format(
      IdKey, SourceId, DestinationId, StatsCount, StatsLastSeen, StatsErrorCount, EffectiveFromKey, EffectiveToKey))
    var id = 1
    for {
      edge <- edgesList
    } yield {
      val edgeAsMap = edge.asInstanceOf[Map[String, Any]]
      val statsAsMap = edgeAsMap(StatsKey).asInstanceOf[Map[String, Any]]
      val str = EdgesFormatString.format(id,
        nodeToIdMap(findNode(SourceKey, edgeAsMap)),
        nodeToIdMap(findNode(DestinationKey, edgeAsMap)),
        statsAsMap(CountKey).asInstanceOf[BigInt],
        statsAsMap(LastSeenKey).asInstanceOf[BigInt],
        statsAsMap(ErrorCountKey).asInstanceOf[BigInt],
        edgeAsMap(EffectiveFromKey).asInstanceOf[BigInt],
        edgeAsMap(EffectiveToKey).asInstanceOf[BigInt])
      stringBuilder.append(str)
      id = id + 1
    }
    stringBuilder.toString
  }

  private val NodesFormatString = "%s,%s,%s,%s\n"
  private def createNodesCsvList(nodeToIdMap: mutable.Map[Node, Int]): String = {
    val stringBuilder = new mutable.StringBuilder(NodesFormatString.format(
      IdKey, NameKey, InfrastructureProviderKey, TierKey))
    nodeToIdMap.foreach {
      case (node, id) =>
        val name = surroundWithQuotesIfNecessary(node.name)
        val infrastructureProvider = surroundWithQuotesIfNecessary(node.infrastructureProvider.getOrElse(""))
        val tier = surroundWithQuotesIfNecessary(node.tier.getOrElse(""))
        val str = NodesFormatString.format(id, name, infrastructureProvider, tier)
        stringBuilder.append(str)
    }
    stringBuilder.toString
  }

  // See http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm#FileFormat
  private def surroundWithQuotesIfNecessary(string: String): String = {
    val stringWithEscapedQuotes = string.replaceAll("\"", "\"\"")
    var stringToReturn = stringWithEscapedQuotes
    if(string.startsWith(" ") || string.endsWith(" ") || string.contains(",")) {
      stringToReturn = "\"" + string + "\""
    }
    stringToReturn
  }

  private def getNodeToIdMap(edgesList: List[Any]) = {
    val sourceNodes = findNodesOfType(edgesList, SourceKey)
    val destinationNodes = findNodesOfType(edgesList, DestinationKey)
    val nodes = (sourceNodes ::: destinationNodes).distinct
    val nodeToIdMap = mutable.Map[Node, Int]()
    var nodeId = 1
    for (node <- nodes) {
      nodeToIdMap(node) = nodeId
      nodeId = nodeId + 1
    }
    nodeToIdMap
  }

  private def findNodesOfType(edgesList: List[Any], nodeType: String) = {
    val nodes = for {
      edge <- edgesList
    } yield {
      val edgeMap = edge.asInstanceOf[Map[String, Any]]
      val sourceNode: Node = findNode(nodeType, edgeMap)
      sourceNode
    }
    nodes
  }

  private def findNode(nodeType: String, edgeMap: Map[String, Any]): Node = {
    val sourceMap = edgeMap(nodeType).asInstanceOf[Map[String, Any]]
    val sourceName = sourceMap(NameKey).asInstanceOf[String]
    val tags = sourceMap.getOrElse(TagsKey, Map.empty[String, String]).asInstanceOf[Map[String, String]]
    val xHaystackInfrastructureProvider = Option(tags.getOrElse(InfrastructureProviderKey, null))
    val tier = Option(tags.getOrElse(TierKey, null))
    Node(sourceName, xHaystackInfrastructureProvider, tier)
  }
}
