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

package com.expedia.www.haystack.service.graph.node.finder.model

import java.util

import com.expedia.www.haystack.service.graph.node.finder.app.SpanAccumulator
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanMergeStyle
import com.expedia.www.haystack.service.graph.node.finder.utils.SpanMergeStyle.SpanMergeStyle
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}

case class ServiceNodeMetadata(mergeStyle: SpanMergeStyle)

class ServiceNodeMetadataSerde extends Serde[ServiceNodeMetadata] {
  implicit val formats: Formats = DefaultFormats + new org.json4s.ext.EnumNameSerializer(SpanMergeStyle)

  override def deserializer(): Deserializer[ServiceNodeMetadata] = {
    new Deserializer[ServiceNodeMetadata] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      override def deserialize(key: String, payload: Array[Byte]): ServiceNodeMetadata = {
        if (payload == null) {
          null
        } else {
          Serialization.read[ServiceNodeMetadata](new String(payload))
        }
      }
    }
  }

  override def serializer(): Serializer[ServiceNodeMetadata] = {
    new Serializer[ServiceNodeMetadata] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(key: String, data: ServiceNodeMetadata): Array[Byte] = {
        Serialization.write(data).getBytes("utf-8")
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()
}

object MetadataStoreBuilder {
  def storeBuilder(config: KafkaConfiguration): StoreBuilder[KeyValueStore[String, ServiceNodeMetadata]] = {
    val builder = Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore(config.metadataConfig.topic),
      Serdes.String(),
      new ServiceNodeMetadataSerde())
    .withCachingEnabled()

    if (config.metadataConfig.logEnabled) builder else builder.withLoggingDisabled()
  }
}
