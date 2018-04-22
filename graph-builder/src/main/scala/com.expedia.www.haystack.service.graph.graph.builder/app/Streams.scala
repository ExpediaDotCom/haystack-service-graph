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
package com.expedia.www.haystack.service.graph.graph.builder.app

import java.util.function.Supplier

import com.expedia.www.haystack.service.graph.graph.builder.config.KafkaConfiguration
import com.expedia.www.haystack.service.graph.graph.builder.model.{GraphEdgeDeserializer, ServiceGraphSerializer}
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.Topology

class Streams(kafkaConfiguration: KafkaConfiguration) extends Supplier[Topology] {
  val GRAPH_NODES = "graph-nodes"
  val SERVICE_GRAPH_ACCUMULATOR = "service-graph-accumulator"
  val KAFKA_SINK = "kafka-sink"
  val CASSANDRA_SINK = "cassandra-sink"

  override def get(): Topology = initialize(new Topology)

  @VisibleForTesting
  def initialize(topology: Topology): Topology = {
    // add source
    addSource(GRAPH_NODES, topology)

    // add service graph accumulator
    // this step will buffer an in memory graph of services
    // will be emitted out periodically
    addAccumulator(SERVICE_GRAPH_ACCUMULATOR, topology, GRAPH_NODES)

    //add sink for graph node producer
    addServiceGraphKafkaSink(KAFKA_SINK, kafkaConfiguration.kafkaSinkTopic, topology, SERVICE_GRAPH_ACCUMULATOR)

    // TODO add Cassandra sink
    // addServiceGraphCassandraSink(CASSANDRA_SINK, kafkaConfiguration.kafkaSinkTopic, topology, SERVICE_GRAPH_ACCUMULATOR)

    //return the topology built
    topology
  }

  private def addSource(stepName: String, topology: Topology) : Unit = {
    topology.addSource(
      kafkaConfiguration.autoOffsetReset,
      stepName,
      kafkaConfiguration.timestampExtractor,
      new StringDeserializer,
      new GraphEdgeDeserializer,
      kafkaConfiguration.consumerTopic)
  }

  private def addAccumulator(accumulatorName: String, topology: Topology, sourceName: String) : Unit = {
    topology.addProcessor(
      accumulatorName,
      new ServiceGraphAccumulatorSupplier(kafkaConfiguration.accumulatorInterval),
      sourceName
    )
  }

  private def addServiceGraphKafkaSink(graphNodeSinkName: String, serviceCallTopic: String, topology: Topology, graphNodeProducerName: String) :Unit = {
    topology.addSink(
      graphNodeSinkName,
      serviceCallTopic,
      new StringSerializer,
      new ServiceGraphSerializer,
      graphNodeProducerName
    )
  }
}
