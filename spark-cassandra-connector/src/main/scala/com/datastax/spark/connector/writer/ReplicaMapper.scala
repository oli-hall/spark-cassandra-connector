package com.datastax.spark.connector.writer


import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.{CountingIterator, Logging}
import org.apache.spark.TaskContext

import scala.collection._
import scala.collection.JavaConversions._

/**
 * Converts Rows into Tokens
 * @param connector
 * @param tableDef
 * @param rowWriter
 * @tparam T
 */
class ReplicaMapper[T] private(
                                 connector: CassandraConnector,
                                 tableDef: TableDef,
                                 rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  val columns = columnNames.map(tableDef.columnByName)
  implicit val protocolVersion = connector.withClusterDo {
    _.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }


  private def quote(name: String): String =
    "\"" + name + "\""

  /**
   * This query is only used to build a prepared statement so we can more easily extract
   * partition tokens from tables
   */
  private lazy val querySelectUsingOnlyParititonKeys: String = {
    val partitionKeys = columns.filter(_.isPartitionKeyColumn)
    def quotedColumnNames(columns: Seq[ColumnDef]) = partitionKeys.map(_.columnName).map(quote)
    val whereClause = quotedColumnNames(partitionKeys).map(c => s"$c = :$c").mkString(" AND ")
    s"SELECT * FROM ${quote(keyspaceName)}.${quote(tableName)} WHERE $whereClause"
  }

  private def prepareDummyStatement(session: Session): PreparedStatement = {
    try {
      session.prepare(querySelectUsingOnlyParititonKeys)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $querySelectUsingOnlyParititonKeys: " + t.getMessage, t)
    }
  }


  def mapReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] = {
    connector.withClusterDo{ cluster =>
    connector.withSessionDo { session =>
      val stmt = prepareDummyStatement(session)
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
      val batchStmtBuilder = new BatchStatementBuilder(Type.UNLOGGED, rowWriter, stmt, protocolVersion, routingKeyGenerator, ConsistencyLevel.LOCAL_ONE)
      data.map { row =>
        val hosts = cluster.getMetadata.getReplicas(keyspaceName, routingKeyGenerator.apply(batchStmtBuilder.bind(row))).map(_.getAddress).toSet[InetAddress]
        (hosts , row)
        }
      }
    }
  }
}

object ReplicaMapper {

  def apply[T: RowWriterFactory](
                                  connector: CassandraConnector,
                                  keyspaceName: String,
                                  tableName: String): ReplicaMapper[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = tableDef.partitionKey.map(_.columnName).toSeq
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef,
      selectedColumns,
      checkColumns = CheckLevel.CheckPartitionOnly)
    new ReplicaMapper[T](connector, tableDef, rowWriter)
  }

}
