package com.datastax.spark.connector.writer


import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.core._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.Logging

import scala.collection.JavaConversions._
import scala.collection._

class ReplicaMapper[T] private(
                                 connector: CassandraConnector,
                                 tableDef: TableDef,
                                 rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  implicit val protocolVersion = connector.withClusterDo {
    _.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }


  private def quote(name: String): String =
    "\"" + name + "\""

  /**
   * This query is only used to build a prepared statement so we can more easily extract
   * partition tokens from tables. We prepare a statement of the form SELECT * FROM keyspace.table
   * where x= .... This statement is never executed.
   */
  private lazy val querySelectUsingOnlyParititonKeys: String = {
    val partitionKeys = tableDef.partitionKey
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

  /**
   * Pairs each piece of data with the Cassandra Replicas which that data would be found on
   * @param data A source of data which can be bound to a statement by BatchStatementBuilder
   * @return an Iterator over the same data keyed by the replica's ip addresses
   */
  def keyByReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] = {
    connector.withClusterDo{ cluster =>
    connector.withSessionDo { session =>
      val stmt = prepareDummyStatement(session)
      val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
      val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion)
      data.map { row =>
        val hosts = cluster.getMetadata
          .getReplicas(keyspaceName, routingKeyGenerator.apply(boundStmtBuilder.bind(row)))
          .map(_.getAddress)
          .toSet[InetAddress]
        (hosts , row)
        }
      }
    }
  }
}

/**
 * Helper methods for mapping a set of data to their relative locations in a Cassandra Cluster.
 */
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
