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

import java.util.function.Supplier

import com.expedia.www.haystack.commons.kstreams.serde.SpanSerde
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricPointSerializer
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.Topology

class Streams(kafkaConfiguration: KafkaConfiguration) extends Supplier[Topology] {

  val PROTO_SPANS = "proto-spans"
  val SPAN_AGGREGATOR = "span-aggregator"
  val LATENCY_PRODUCER = "latency-producer"
  val GRAPH_NODE_PRODUCER = "nodes-n-edges-producer"
  val METRIC_SINK = "metric-sink"
  val GRAPH_NODE_SINK = "graph-nodes-sink"

  /**
    * This provides a topology that is shown in the flow chart below
    *
    *                     +---------------+
    *                     |               |
    *                     |  proto-spans  |
    *                     |               |
    *                     +-------+-------+
    *                             |
    *                             |
    *                             |
    *                   +---------v----------+
    *                   |                    |
    *              +----+   span-aggregator  +----+
    *              |    |                    |    |
    *              |    +--------------------+    |
    *              |                              |
    *              |                              |
    *              |                              |
    *    +---------v----------+         +---------v----------------+
    *    |                    |         |                          |
    *    |  latency-producer  |         |  nodes-n-edges-producer  |
    *    |                    |         |                          |
    *    +---------+----------+         +---------+----------------+
    *              |                              |
    *              |                              |
    *     +--------v-------+                +-----v-------------+
    *     |                |                |                   |
    *     |   metric-sink  |                |  graph-nodes-sink |
    *     |                |                |                   |
    *     +----------------+                +-------------------+
    *
    *    Source:
    *
    *         proto-spans  :   Reads a topic of span serialized in protobuf
    *
    *    Processors:
    *
    *         span-aggregator         :  Aggregates incoming spans for specified time to find matching client-server spans
    *         latency-producer        :  From the span pairs produced by span-aggregator, this processor computes and emits network latency
    *         nodes-n-edges-producer  :  From the span pairs produced by span-aggregator, this processor produces a simple graph relationship
    *                                    between the services in the forrm of  service --(operation)--> service
    *    Sinks:
    *
    *         metric-sink       :  Output of latency-producer (MetricPoint) is serialized using MessagePack and sent to a kafka topic
    *         graph-nodes-sink  :  Output of nodes-n-edges-producer is serialized a json string and sent to a kafka topic
    *
    * @return
    */
  override def get(): Topology = addSteps(new Topology)

  @VisibleForTesting
  def addSteps(topology: Topology) : Topology = {
    //add source
    addSource(PROTO_SPANS, topology)

    //add span aggregator. This step will aggregate spans
    //by message id. This will emit spans with client-server
    //relationship after specified number of seconds
    addAggregator(SPAN_AGGREGATOR, topology, PROTO_SPANS)

    //add latency producer. This is downstream of aggregator
    //this will parse a span with client-server relationship and
    //emit a metric point on the latency for that service-operation pair
    addLatencyProducer(LATENCY_PRODUCER, topology, SPAN_AGGREGATOR)

    //add graph node producer. This is downstream of aggregator
    //for each client-server span emitted by the aggregator, this will
    //produce a service - operation - service data point for building
    //the edges between the nodes in a graph
    addGraphNodeProducer(GRAPH_NODE_PRODUCER, topology, SPAN_AGGREGATOR)

    //add sink for latency producer
    addMetricSink(METRIC_SINK, kafkaConfiguration.metricsTopic, topology, LATENCY_PRODUCER)

    //add sink for graph node producer
    addGraphNodeSink(GRAPH_NODE_SINK, kafkaConfiguration.serviceCallTopic, topology, GRAPH_NODE_PRODUCER)

    //return the topology built
    topology
  }

  private def addSource(stepName: String, topology: Topology) : Unit = {
    //add a source
    topology.addSource(
      kafkaConfiguration.autoOffsetReset,
      stepName,
      kafkaConfiguration.timestampExtractor,
      new StringDeserializer,
      (new SpanSerde).deserializer(),
      kafkaConfiguration.protoSpanTopic)
  }

  private def addAggregator(aggregatorName: String, topology: Topology, sourceName: String) : Unit = {
    topology.addProcessor(
      aggregatorName,
      new SpanAggregatorSupplier(kafkaConfiguration.aggregatorInterval),
      sourceName
    )
  }

  private def addLatencyProducer(latencyProducerName: String, topology: Topology, aggregatorName: String) : Unit = {
    topology.addProcessor(
      latencyProducerName,
      new LatencyProducerSupplier(),
      aggregatorName
    )
  }

  private def addGraphNodeProducer(graphNodeProducerName: String, topology: Topology, aggregatorName: String) = {
    topology.addProcessor(
      graphNodeProducerName,
      new GraphNodeProducerSupplier(),
      aggregatorName
    )
  }

  private def addMetricSink(metricSinkName: String, metricsTopic: String, topology: Topology,
                            latencyProducerName: String) : Unit = {
    topology.addSink(
      metricSinkName,
      metricsTopic,
      new StringSerializer,
      new MetricPointSerializer,
      latencyProducerName
    )
  }

  private def addGraphNodeSink(graphNodeSinkName: String, serviceCallTopic: String, topology: Topology,
                               graphNodeProducerName: String) :Unit = {
    topology.addSink(
      graphNodeSinkName,
      serviceCallTopic,
      new StringSerializer,
      new StringSerializer,
      graphNodeProducerName
    )
  }

}
