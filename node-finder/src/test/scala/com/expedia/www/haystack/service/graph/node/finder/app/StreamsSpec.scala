package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.TestSpec
import com.expedia.www.haystack.commons.kstreams.SpanTimestampExtractor
import com.expedia.www.haystack.commons.kstreams.serde.SpanDeserializer
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricPointSerializer
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.easymock.EasyMock._

class StreamsSpec extends TestSpec {
  describe("configuring a topology should") {
    it("should add a source, three processors and two sinks with expected arguments") {
      Given("a configuration object of type KafkaConfiguration")
      val streamsConfig = mock[StreamsConfig]
      val kafkaConfig = KafkaConfiguration(streamsConfig,
        "metrics", "service-call",
        "proto-spans", Topology.AutoOffsetReset.LATEST,
        new SpanTimestampExtractor, 10000, 10000)
      val streams = new Streams(kafkaConfig)
      val topology = mock[Topology]
      When("addSteps is invoked with a topology")
      expecting {
        topology.addSource(isA(classOf[Topology.AutoOffsetReset]), anyString(),
          isA(classOf[SpanTimestampExtractor]), isA(classOf[StringDeserializer]),
          isA(classOf[SpanDeserializer]), anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[SpanAggregatorSupplier]),
          anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[GraphNodeProducerSupplier]),
          anyString()).andReturn(topology).once()
        topology.addProcessor(anyString(), isA(classOf[LatencyProducerSupplier]),
          anyString()).andReturn(topology).once()
        topology.addSink(anyString(), anyString(), isA(classOf[StringSerializer]),
          isA(classOf[MetricPointSerializer]), anyString()).andReturn(topology).once()
        topology.addSink(anyString(), anyString(), isA(classOf[StringSerializer]),
          isA(classOf[StringSerializer]), anyString()).andReturn(topology).once()
      }
      replay(topology)
      streams.addSteps(topology)
      Then("it is configured as expected")
      verify(topology)
    }
  }

}
