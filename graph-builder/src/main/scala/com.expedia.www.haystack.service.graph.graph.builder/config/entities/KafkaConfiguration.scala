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
package com.expedia.www.haystack.service.graph.graph.builder.config.entities

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology.AutoOffsetReset

/**
  * Case class holding required configuration for the node finder kstreams app
  * @param streamsConfig valid instance of StreamsConfig
  * @param consumerTopic topic name for incoming graph edges topic
  * @param producerTopic topic name for materialized ktable changelogs
  * @param autoOffsetReset Offset type for the kstreams app to start with
  * @param closeTimeoutInMs time for closing a kafka topic
  */
case class KafkaConfiguration(streamsConfig: StreamsConfig,
                              consumerTopic: String,
                              producerTopic: String,
                              autoOffsetReset: AutoOffsetReset,
                              closeTimeoutInMs: Long,
                              aggregationWindowSec: Int,
                              aggregationRetentionDays: Int
                             ) {
  require(streamsConfig != null)
  require(StringUtils.isNotBlank(consumerTopic))
  require(StringUtils.isNotBlank(producerTopic))
  require(autoOffsetReset != null)
  require(closeTimeoutInMs > 0)
}
