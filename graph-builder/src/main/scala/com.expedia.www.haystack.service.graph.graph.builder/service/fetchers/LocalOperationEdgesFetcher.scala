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
package com.expedia.www.haystack.service.graph.graph.builder.service.fetchers

import com.expedia.www.haystack.commons.entities.GraphEdge
import com.expedia.www.haystack.service.graph.graph.builder.model.{EdgeStats, OperationGraphEdge, ServiceGraphEdge}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue}

import scala.collection.JavaConverters._

class LocalOperationEdgesFetcher(streams: KafkaStreams, storeName: String) {
  private lazy val store: ReadOnlyKeyValueStore[GraphEdge, EdgeStats] =
    streams.store(storeName, QueryableStoreTypes.keyValueStore[GraphEdge, EdgeStats]())

  def fetchEdges(): List[OperationGraphEdge] = {
    val operationGraphEdgesIterator =
      for (kv: KeyValue[GraphEdge, EdgeStats] <- store.all().asScala)
        yield OperationGraphEdge(kv.key.source, kv.key.destination, kv.key.operation, kv.value)

    operationGraphEdgesIterator.toList
  }
}
