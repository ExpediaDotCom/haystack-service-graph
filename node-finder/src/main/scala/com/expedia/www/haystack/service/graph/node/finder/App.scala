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
package com.expedia.www.haystack.service.graph.node.finder

import com.expedia.www.haystack.commons.health.{HealthStatusController, UpdateHealthStatusFile}
import com.expedia.www.haystack.commons.kstreams.app.{Main, StateChangeListener, StreamsFactory, StreamsRunner}
import com.expedia.www.haystack.service.graph.node.finder.app.Streams
import com.expedia.www.haystack.service.graph.node.finder.config.AppConfiguration

object App extends Main {
  def createStreamsRunner() : StreamsRunner = {
    val appConfiguration = new AppConfiguration()

    val healthStatusController = new HealthStatusController
    healthStatusController.addListener(new UpdateHealthStatusFile(appConfiguration.healthStatusFilePath))

    val streamsFactory = new StreamsFactory(new Streams(appConfiguration.kafkaConfig),
      appConfiguration.kafkaConfig.streamsConfig,
      Some(appConfiguration.kafkaConfig.protoSpanTopic))

    new StreamsRunner(streamsFactory, new StateChangeListener(healthStatusController))
  }
}
