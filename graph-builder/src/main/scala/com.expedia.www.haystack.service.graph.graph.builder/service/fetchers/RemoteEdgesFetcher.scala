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

import com.expedia.www.haystack.service.graph.graph.builder.config.entities.ServiceClientConfiguration
import com.expedia.www.haystack.service.graph.graph.builder.model.{ServiceGraph, ServiceGraphEdge}
import com.google.gson.Gson
import org.apache.http.client.fluent.Request
import org.apache.http.client.utils.URIBuilder

import scala.collection.JavaConverters._

class RemoteEdgesFetcher(clientConfig: ServiceClientConfiguration) {
  def fetchEdges(host: String, port: Int): List[ServiceGraphEdge] = {
    val request = new URIBuilder()
      .setScheme("http")
      .setHost(host)
      .setPort(port)
      .build()

    val edgeJson = Request.Get(request)
      .connectTimeout(clientConfig.connectionTimeout)
      .socketTimeout(clientConfig.socketTimeout)
      .execute()
      .returnContent()
      .asString()

    new Gson()
      .fromJson(edgeJson, classOf[ServiceGraph])
      .edges
      .asScala
      .toList
  }
}
