package com.expedia.www.haystack.service.graph.node.finder.app

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.service.graph.node.finder.config.KafkaConfiguration
import com.expedia.www.haystack.service.graph.node.finder.model.SpanLite
import com.expedia.www.haystack.service.graph.node.finder.utils.{SpanType, SpanUtils}
import org.apache.kafka.streams.processor._

class SpanAggregatorSupplier(kafkaConfig: KafkaConfiguration) extends ProcessorSupplier[String, Span] {
  override def get() : Processor[String, Span] = new SpanAggregator(kafkaConfig)
}

class SpanAggregator(kafkaConfig: KafkaConfiguration) extends Processor[String, Span] with Punctuator {

  private var context: ProcessorContext = _
  private var map : Map[String, SpanLite] = Map.empty

  override def init(context: ProcessorContext): Unit =  {
    this.context = context
    this.context.schedule(kafkaConfig.aggregatorInterval, PunctuationType.STREAM_TIME, this)
  }

  override def process(key: String, span: Span): Unit = {
    val spanType = SpanUtils.getSpanType(span)
    if (spanType != SpanType.OTHER) {
      val spanLite = map.getOrElse(span.getSpanId, {
        val s = new SpanLite(span.getSpanId)
        map = map.updated(span.getSpanId, s)
        s
      })
      spanLite.merge(span, spanType)
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    //time to emit. Swap the map here
    val mapToEmit = map
    map = Map.empty

    //iterate and forward
    mapToEmit.values.filter(s => s.isComplete).foreach(s => context.forward(s.spanId, s))

    // commit the current processing progress
    context.commit()
  }

  override def close(): Unit = {}
}
