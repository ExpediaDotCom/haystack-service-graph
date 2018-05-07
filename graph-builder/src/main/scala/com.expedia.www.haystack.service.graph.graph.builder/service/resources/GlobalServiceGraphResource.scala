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
package com.expedia.www.haystack.service.graph.graph.builder.service.resources

import java.util

import com.expedia.www.haystack.service.graph.graph.builder.config.entities.ServiceConfiguration
import com.expedia.www.haystack.service.graph.graph.builder.model.{ServiceGraph, ServiceGraphEdge}
import com.expedia.www.haystack.service.graph.graph.builder.service.fetchers.{LocalEdgesFetcher, RemoteEdgesFetcher}
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class GlobalServiceGraphResource(streams: KafkaStreams,
                                 storeName: String,
                                 serviceConfig: ServiceConfiguration,
                                 localEdgesFetcher: LocalEdgesFetcher,
                                 remoteEdgesFetcher: RemoteEdgesFetcher)
  extends Resource("servicegraph") {
  private val LOGGER = LoggerFactory.getLogger(classOf[LocalServiceGraphResource])
  private val globalEdgeCount = metricRegistry.histogram("servicegraph.global.edges")

  protected override def get(): ServiceGraph = {
    val edgesList: util.ArrayList[ServiceGraphEdge] = new util.ArrayList[ServiceGraphEdge]()

    // get list of all hosts containing service-graph store
    // fetch local service graphs from all hosts
    // and merge local graphs to create global graph
    streams
      .allMetadataForStore(storeName)
      .forEach(host => {

        val edges =
          if (host.host() == serviceConfig.host) {
            val localEdges = localEdgesFetcher.fetchEdges()
            LOGGER.info(s"graph from local returned ${localEdges.size} edges")
            localEdges
          }
          else {
            val remoteEdges = remoteEdgesFetcher.fetchEdges(serviceConfig.host, serviceConfig.http.port)
            LOGGER.info(s"graph from ${host.host()} returned ${remoteEdges.size} edges")
            remoteEdges
          }

        edgesList.addAll(edges.asJava)
      })

    globalEdgeCount.update(edgesList.size())
    ServiceGraph(edgesList)
  }
}
