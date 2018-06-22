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
package com.expedia.www.haystack.service.graph.graph.builder.model

import java.util

import com.google.gson.Gson
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class EdgeStatsSerde extends Serde[EdgeStats] {
  override def deserializer(): Deserializer[EdgeStats] = new EdgeStatsDeserializer

  override def serializer(): Serializer[EdgeStats] = new EdgeStatsSerializer

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()
}

class EdgeStatsSerializer extends Serializer[EdgeStats] {
  override def serialize(topic: String, graphEdgeSet: EdgeStats): Array[Byte] = {
    new Gson().toJson(graphEdgeSet).getBytes
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()
}

class EdgeStatsDeserializer extends Deserializer[EdgeStats] {
  override def deserialize(topic: String, data: Array[Byte]): EdgeStats = {
    if(data == null) EdgeStats(0, 0, 0)
    else new Gson().fromJson(new String(data), classOf[EdgeStats])
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()
}
