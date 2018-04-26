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

import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.graph.builder.model.{GraphEdge, ServiceGraph}
import com.netflix.servo.util.VisibleForTesting
import org.apache.kafka.streams.processor._
import org.slf4j.LoggerFactory

class ServiceGraphAccumulatorSupplier(accumulatorInterval: Int) extends ProcessorSupplier[String, GraphEdge] {
  override def get(): Processor[String, GraphEdge] = new ServiceGraphAccumulator(accumulatorInterval)
}

object ServiceGraphAccumulator extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(classOf[ServiceGraphAccumulator])
  private val processMeter = metricRegistry.meter("service.graph.accumulator.processed")
  private val aggregateHistogram = metricRegistry.histogram("service.graph.accumulator.edges")
}

class ServiceGraphAccumulator(accumulatorInterval: Int) extends Processor[String, GraphEdge] {
  import ServiceGraphAccumulator._

  private var accumulator: ServiceGraph = new ServiceGraph

  override def init(context: ProcessorContext): Unit = {
    context.schedule(accumulatorInterval, PunctuationType.STREAM_TIME, getPunctuator(context))
    LOGGER.info(s"${this.getClass.getSimpleName} initialized")
  }

  override def process(key: String, edge: GraphEdge): Unit = {
    processMeter.mark()
    accumulator.add(edge)
  }

  @VisibleForTesting
  def getPunctuator(context: ProcessorContext): Punctuator = {
    (timestamp: Long) => {
      aggregateHistogram.update(accumulator.size())
      context.forward(s"service-graph-${context.taskId}-$timestamp", accumulator)
      accumulator = new ServiceGraph
      context.commit()
    }
  }

  override def punctuate(timestamp: Long): Unit = {}

  override def close(): Unit = {}

  @VisibleForTesting
  def serviceGraphSize: Int = accumulator.size()
}
