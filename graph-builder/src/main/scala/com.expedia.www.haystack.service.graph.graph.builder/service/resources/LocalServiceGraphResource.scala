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
import com.expedia.www.haystack.service.graph.graph.builder.model.{EdgeStats, ServiceGraph, ServiceGraphEdge}
import com.google.gson.Gson
import org.apache.http.entity.ContentType
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue}

import scala.collection.JavaConverters._

class LocalServiceGraphResource(streams: KafkaStreams, storeName: String) extends HttpServlet {
  lazy val store: ReadOnlyKeyValueStore[GraphEdge, EdgeStats] =
    streams.store(storeName, QueryableStoreTypes.keyValueStore[GraphEdge, EdgeStats]())

  // TODO add counters and logging for servlet
  protected override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setContentType(ContentType.APPLICATION_JSON.getMimeType)
    response.setStatus(HttpServletResponse.SC_OK)

    // TODO add counters for number of edges being returned
    val serviceGraphJson = new Gson().toJson(fetchServiceGraphFromLocal())

    response.getWriter.println(serviceGraphJson)
  }

  private def fetchServiceGraphFromLocal() = {
    val serviceGraphEdges = for (kv: KeyValue[GraphEdge, EdgeStats] <- store.all().asScala) yield ServiceGraphEdge(kv.key, kv.value)
    ServiceGraph(serviceGraphEdges.toList.asJava)
  }
}
