package com.expedia.www.haystack.service.graph.graph.builder.writer
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DefaultRetryPolicy, LatencyAwarePolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.service.graph.graph.builder.config.CassandraConfiguration
import com.expedia.www.haystack.service.graph.graph.builder.model.ServiceGraph
import com.google.gson.Gson
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

class CassandraWriter(config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor) extends ServiceGraphWriter with MetricsSupport{
  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraWriter])
  private val writeTimer = metricRegistry.timer("cassandra.write.time")
  private val writeFailures = metricRegistry.meter("cassandra.write.failure")

  private val ID_COLUMN_NAME = "id"
  private val TIMESTAMP_COLUMN_NAME = "ts"
  private val SERVICE_GRAPH_COLUMN_NAME = "servicegraph"

  private val (cluster, session, preparedInsertStatement) = {
    // setup load balancing policy
    val tokenAwarePolicy = new TokenAwarePolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build())

    // create cluster
    val cluster = Cluster.builder()
      .withClusterName("cassandra-cluster")
      .addContactPoints(config.endpoints: _*)
      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
      .withAuthProvider(AuthProvider.NONE)
      .withSocketOptions(new SocketOptions()
        .setKeepAlive(config.socket.keepAlive)
        .setConnectTimeoutMillis(config.socket.connectionTimeoutMillis)
        .setReadTimeoutMillis(config.socket.readTimeoutMills))
      .withLoadBalancingPolicy(tokenAwarePolicy)
      .withPoolingOptions(new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, config.socket.maxConnectionPerHost))
      .build()

    // create session
    val newSession = cluster.connect()

    // ensure table exists
    CassandraTableSchema.ensureExists(config.keyspace, config.tableName, config.autoCreateSchema, newSession)

    // use keyspace
    newSession.execute("USE " + config.keyspace)

    // prepare bound statetment
    import QueryBuilder.{bindMarker, ttl}
    val statement = QueryBuilder
      .insertInto(config.tableName)
      .value(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))
      .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
      .value(SERVICE_GRAPH_COLUMN_NAME, bindMarker(SERVICE_GRAPH_COLUMN_NAME))
    val preparedInsertStatement = newSession.prepare(statement)

    (cluster, newSession, preparedInsertStatement)
  }

  override def write(taskId: String, serviceGraph: ServiceGraph): Unit = {
    try {
      val timer = writeTimer.time()

      val statement: Statement = new BoundStatement(preparedInsertStatement)
        .setString(ID_COLUMN_NAME, "servicegraph")
        .setTimestamp(TIMESTAMP_COLUMN_NAME, new Date())
        .setBytes(SERVICE_GRAPH_COLUMN_NAME, ByteBuffer.wrap(new Gson().toJson(serviceGraph).getBytes()))

      val asyncResult = session.executeAsync(statement)
      asyncResult.addListener(new CassandraWriteResultsListener(asyncResult, timer), dispatcher)

    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to write to cassandra with exception", ex)
        writeFailures.mark()
    }
  }

  /**
    * close the session and client
    */
  override def close(): Unit = {
    Try(session.close())
    Try(cluster.close())
  }
}
