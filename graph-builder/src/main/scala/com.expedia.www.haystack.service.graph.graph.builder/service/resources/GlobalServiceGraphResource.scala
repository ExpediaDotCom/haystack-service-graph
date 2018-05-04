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
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.graph.builder.model.{ServiceGraph, ServiceGraphEdge}
import com.google.gson.Gson
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory

class GlobalServiceGraphResource(streams: KafkaStreams, storeName: String) extends HttpServlet with MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(classOf[LocalServiceGraphResource])
  private val edgeCount = metricRegistry.histogram("servicegraph.global.edges")

  // TODO add counters, improve logging and error handling for servlet
  protected override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setContentType(ContentType.APPLICATION_JSON.getMimeType)
    response.setStatus(HttpServletResponse.SC_OK)

    response.getWriter.print(new Gson().toJson(fetchEdgesFromAllHosts()))
    response.getWriter.flush()

    LOGGER.info("accesslog: /servicegraph/global completed successfully")
  }

  private def fetchEdgesFromAllHosts() = {
    val edgesList: util.ArrayList[ServiceGraphEdge] = new util.ArrayList[ServiceGraphEdge]()

    // get list of all hosts containing service-graph store
    // fetch local service graphs from all hosts
    // and merge local graphs to create global graph
    streams
      .allMetadataForStore(storeName)
      .forEach(host => {
        LOGGER.info(s"request graph from :$host")

        // TODO handle exception cases
        // TODO add configs for http client
        // TODO trigger requests to all hosts in parallel and merge the futures
        val edgeJson = Request
          .Get(s"http://${host.host()}:${host.port()}/servicegraph/local")
          .execute()
          .returnContent()
          .asString()

        edgesList.addAll(new Gson().fromJson(edgeJson, classOf[ServiceGraph]).edges)
      })

    edgeCount.update(edgesList.size())
    ServiceGraph(edgesList)
  }
}
