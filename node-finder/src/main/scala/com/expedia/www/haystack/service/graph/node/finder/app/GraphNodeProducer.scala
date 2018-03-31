package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.www.haystack.service.graph.node.finder.model.SpanLite
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}

class GraphNodeProducerSupplier extends ProcessorSupplier[String, SpanLite] {
  override def get(): Processor[String, SpanLite] = new GraphNodeProducer
}

class GraphNodeProducer extends Processor[String, SpanLite] {
  override def init(context: ProcessorContext): Unit = ???

  override def process(key: String, value: SpanLite): Unit = ???

  override def punctuate(timestamp: Long): Unit = ???

  override def close(): Unit = ???
}
