package com.expedia.www.haystack.service.graph.node.finder.config

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.TimestampExtractor

case class KafkaConfiguration(streamsConfig: StreamsConfig,
                              metricsTopic: String,
                              serviceCallTopic: String,
                              protoSpanTopic: String,
                              autoOffsetReset: AutoOffsetReset,
                              timestampExtractor: TimestampExtractor,
                              aggregatorInterval: Int,
                              closeTimeoutInMs: Long)
