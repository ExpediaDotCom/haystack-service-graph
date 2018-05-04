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

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.expedia.www.haystack.commons.entities.GraphEdge
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.graph.builder.model.{EdgeStats, ServiceGraph, ServiceGraphEdge}
import com.google.gson.Gson
import org.apache.http.entity.ContentType
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class LocalServiceGraphResource(streams: KafkaStreams, storeName: String) extends HttpServlet with MetricsSupport  {

  private val LOGGER = LoggerFactory.getLogger(classOf[LocalServiceGraphResource])
  private val edgeCount = metricRegistry.histogram("servicegraph.local.edges")

  private lazy val store: ReadOnlyKeyValueStore[GraphEdge, EdgeStats] =
    streams.store(storeName, QueryableStoreTypes.keyValueStore[GraphEdge, EdgeStats]())

  // TODO add counters, improve logging and error handling for servlet
  protected override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setContentType(ContentType.APPLICATION_JSON.getMimeType)
    response.setStatus(HttpServletResponse.SC_OK)

    val serviceGraphJson = new Gson().toJson(fetchServiceGraphFromLocal())

    response.getWriter.print(serviceGraphJson)
    response.getWriter.flush()

    LOGGER.info("accesslog: /servicegraph/local completed successfully")
  }

  private def fetchServiceGraphFromLocal() = {
    val serviceGraphEdgesIterator =
      for (kv: KeyValue[GraphEdge, EdgeStats] <- store.all().asScala)
        yield ServiceGraphEdge(kv.key.source, kv.key.destination, kv.key.operation, kv.value)
    val serviceGraphEdge = serviceGraphEdgesIterator.toList.asJava
    edgeCount.update(serviceGraphEdge.size())
    ServiceGraph(serviceGraphEdge)
  }
}
