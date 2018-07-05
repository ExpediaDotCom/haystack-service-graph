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
package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.entities.GraphEdge
import com.expedia.www.haystack.commons.entities.encoders.PeriodReplacementEncoder
import com.expedia.www.haystack.commons.kstreams.SpanTimestampExtractor
import com.expedia.www.haystack.commons.kstreams.serde.SpanDeserializer
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricPointSerializer
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import org.apache.kafka.common.serialization.{Serializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.easymock.EasyMock._

class StreamsSpec extends TestSpec {
  describe("configuring a topology should") {
    it("should add a source, three processors and two sinks with expected arguments") {
      Given("a configuration object of type KafkaConfiguration")
      val streamsConfig = mock[StreamsConfig]
      val kafkaConfig = KafkaConfiguration(streamsConfig,
        "metrics", new PeriodReplacementEncoder(), "service-call",
        "proto-spans", Topology.AutoOffsetReset.LATEST,
        new SpanTimestampExtractor, 10000, 10000, List("tier"))
      val streams = new Streams(kafkaConfig)
      val topology = mock[Topology]
      When("initialize is invoked with a topology")
      expecting {
        topology.addSource(isA(classOf[Topology.AutoOffsetReset]), anyString(),
          isA(classOf[SpanTimestampExtractor]), isA(classOf[StringDeserializer]),
          isA(classOf[SpanDeserializer]), anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[SpanAccumulatorSupplier]),
          anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[GraphNodeProducerSupplier]),
          anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[LatencyProducerSupplier]),
          anyString()).andReturn(topology).once()
        topology.addSink(anyString(), anyString(), isA(classOf[StringSerializer]),
          isA(classOf[MetricPointSerializer]), anyString()).andReturn(topology).once()
        topology.addSink(anyString(), anyString(), isA(classOf[Serializer[GraphEdge]]),
          isA(classOf[Serializer[GraphEdge]]), anyString()).andReturn(topology).once()
      }
      replay(topology)
      streams.initialize(topology)
      Then("it is configured as expected")
      verify(topology)
    }
  }

}
