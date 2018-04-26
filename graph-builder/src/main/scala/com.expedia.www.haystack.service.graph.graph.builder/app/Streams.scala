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

import com.expedia.www.haystack.service.graph.graph.builder.config.{CassandraConfiguration, KafkaConfiguration}
import com.expedia.www.haystack.service.graph.graph.builder.model.GraphEdgeDeserializer
import com.expedia.www.haystack.service.graph.graph.builder.writer.CassandraWriter
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.Topology

class Streams(kafkaConfiguration: KafkaConfiguration, cassandraConfiguration: CassandraConfiguration) extends Supplier[Topology] {
  val GRAPH_NODES = "graph-nodes"
  val SERVICE_GRAPH_ACCUMULATOR = "service-graph-accumulator"
  val CASSANDRA_SINK = "cassandra-sink"

  implicit private val executor = scala.concurrent.ExecutionContext.global
  val writer = new CassandraWriter(cassandraConfiguration)

  override def get(): Topology = initialize(new Topology)

  @VisibleForTesting
  def initialize(topology: Topology): Topology = {
    // add source
    addSource(GRAPH_NODES, topology)

    // add service graph accumulator
    // this step will buffer an in memory graph of services
    // will be emitted out periodically
    addAccumulator(SERVICE_GRAPH_ACCUMULATOR, topology, GRAPH_NODES)

    // cassandra sink for graph node producer
    addServiceGraphCassandraSink(CASSANDRA_SINK,  topology, SERVICE_GRAPH_ACCUMULATOR)

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

  private def addServiceGraphCassandraSink(graphNodeSinkName: String, topology: Topology, graphNodeProducerName: String) :Unit = {
    topology.addProcessor(
      graphNodeSinkName,
      new ServiceGraphSinkSupplier(writer),
      graphNodeProducerName
    )
  }
}
