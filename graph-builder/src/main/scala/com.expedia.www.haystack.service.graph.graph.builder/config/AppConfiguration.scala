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
package com.expedia.www.haystack.service.graph.graph.builder.config

import java.util.Properties

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.{TimestampExtractor, WallclockTimestampExtractor}

import scala.collection.JavaConverters._

/**
  * This class reads the configuration from the given resource name using {@link ConfigurationLoader ConfigurationLoader}
  *
  * @param resourceName name of the resource file to load
  */
class AppConfiguration(resourceName: String) {

  require(StringUtils.isNotBlank(resourceName))

  private val config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName = this.resourceName)

  /**
    * default constructor. Loads config from resource name to "app.conf"
    */
  def this() = this("app.conf")

  /**
    * Location of the health status file
    */
  val healthStatusFilePath: String = config.getString("health.status.path")

  /**
    * Instance of {@link KafkaConfiguration KafkaConfiguration} to be used by the kstreams application
    */
  lazy val kafkaConfig: KafkaConfiguration = {

    // verify if the applicationId and bootstrap server config are non empty
    def verifyRequiredProps(props: Properties): Unit = {
      require(StringUtils.isNotBlank(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)))
      require(StringUtils.isNotBlank(props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)))
    }

    def addProps(config: Config, props: Properties, prefix: (String) => String = identity): Unit = {
      config.entrySet().asScala.foreach(kv => {
        val propKeyName = prefix(kv.getKey)
        props.setProperty(propKeyName, kv.getValue.unwrapped().toString)
      })
    }

    val kafka = config.getConfig("kafka")
    val streamsConfig = kafka.getConfig("streams")
    val consumerConfig = kafka.getConfig("consumer")

    val props = new Properties

    // add stream specific properties
    addProps(streamsConfig, props)

    // validate props
    verifyRequiredProps(props)

    val timestampExtractor =  Option(props.getProperty("timestamp.extractor")) match {
      case Some(timeStampExtractorClass) =>
        Class.forName(timeStampExtractorClass).newInstance().asInstanceOf[TimestampExtractor]
      case None =>
        new WallclockTimestampExtractor
    }

    //set timestamp extractor
    props.setProperty("timestamp.extractor", timestampExtractor.getClass.getName)

    KafkaConfiguration(new StreamsConfig(props),
      consumerConfig.getString("topic"),
      if (streamsConfig.hasPath("auto.offset.reset")) {
        AutoOffsetReset.valueOf(streamsConfig.getString("auto.offset.reset").toUpperCase)
      }
      else {
        AutoOffsetReset.LATEST
      },
      timestampExtractor,
      kafka.getInt("accumulator.interval"),
      kafka.getLong("close.timeout.ms")
    )
  }

  /**
    *
    * cassandra configuration object
    */
  lazy val cassandraConfig: CassandraConfiguration = {
    val cs = config.getConfig("cassandra")

    val socketConfig = cs.getConfig("connections")

    val socket = SocketConfiguration(
      socketConfig.getInt("max.per.host"),
      socketConfig.getBoolean("keep.alive"),
      socketConfig.getInt("conn.timeout.ms"),
      socketConfig.getInt("read.timeout.ms"))

    val keyspaceConfig = cs.getConfig("keyspace")

    val autoCreateSchemaField = "auto.create.schema"
    val autoCreateSchema = if (keyspaceConfig.hasPath(autoCreateSchemaField)
      && StringUtils.isNotEmpty(keyspaceConfig.getString(autoCreateSchemaField))) {
      Some(keyspaceConfig.getString(autoCreateSchemaField))
    } else {
      None
    }

    CassandraConfiguration(
      if (cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
      keyspaceConfig.getString("name"),
      keyspaceConfig.getString("table.name"),
      autoCreateSchema,
      socket)
  }
}
