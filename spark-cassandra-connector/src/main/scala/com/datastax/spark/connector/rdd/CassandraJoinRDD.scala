package com.datastax.spark.connector.rdd

import com.datastax.driver.core.{PreparedStatement, Session}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{ReplicaPartition, ReplicaPartitioner}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

// O[ld] Is the type of the RDD we are Mapping From, N[ew] the type were are mapping too Old
class CassandraJoinRDD[O, N] private[connector](prev: RDD[O],
                                      keyspaceName: String,
                                      tableName: String,
                                      connector: CassandraConnector,
                                      columns: ColumnSelector = AllColumns,
                                      where: CqlWhereClause = CqlWhereClause.empty,
                                      readConf: ReadConf = ReadConf())
                                      (implicit oldTag: ClassTag[O], newTag: ClassTag[N],
                                       @transient rwf: RowWriterFactory[O], @transient rrf: RowReaderFactory[N])
  extends CassandraRDD[N](prev.sparkContext, connector, keyspaceName, tableName, columns, where, readConf, prev.dependencies) {

  //Make sure copy operations make new CJRDDs and not CRDDs
  override def copy(columnNames: ColumnSelector = columnNames,
                   where: CqlWhereClause = where,
                   readConf: ReadConf = readConf, connector: CassandraConnector = connector): CassandraJoinRDD[O, N] =
    new CassandraJoinRDD[O, N](prev, keyspaceName, tableName, connector, columnNames, where, readConf)

  private val converter = ReplicaMapper[O](connector, keyspaceName, tableName)

  //We need to make sure we get selectedColumnNames before serialization so that our RowReader is
  //built
  private val singleKeyCqlQuery: (String) = {
    require(tableDef.partitionKey.map(_.columnName).exists(
      partitionKey => where.predicates.exists(_.contains(partitionKey))) == false,
      "Partition keys are not allowed in the .where() clause of a Cassandra Join")
    logDebug("Generating Single Key Query Prepared Statement String")
    val columns = selectedColumnNames.map(_.cql).mkString(", ")
    val partitionWhere = tableDef.partitionKey.map(_.columnName).map(name => s"${quote(name)} = :$name")
    val filter = (where.predicates ++ partitionWhere).mkString(" AND ")
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    val query = s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter"
    logDebug(query)
    query
  }

  /**
   * When computing a CassandraPartitionKeyRDD the data is selected via single CQL statements
   * from the specified C* Keyspace and Table. This will be preformed on whatever data is
   * avaliable in the previous RDD in the chain.
   * @param split
   * @param context
   * @return
   */
  override def compute(split: Partition, context: TaskContext): Iterator[N] = {
    connector.withSessionDo { session =>
      logDebug(s"Query::: $singleKeyCqlQuery")
      val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
      fetchIterator(session, stmt, prev.iterator(split, context))
      }
    }

  def fetchIterator(session:Session, stmt: PreparedStatement, lastIt:Iterator[O]): Iterator[N] = {
    val columnNamesArray = selectedColumnNames.map(_.selectedAs).toArray
    converter.bindStatements(lastIt, stmt).flatMap { request => //flatMap Because we may get multiple results for a single query
      implicit val pv = protocolVersion(session)
      val rs = session.execute(request)
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val result = iterator.map(rowTransformer.read(_, columnNamesArray))
      result
    }
  }

  @transient override val partitioner: Option[Partitioner] = prev.partitioner

  /**
   * If this RDD was partitioned using the ReplicaPartitioner then that means we can get preffered locations
   * for each partition, otherwise we will rely on the previous RDD's partitioning.
   * @return
   */
  override def getPartitions: Array[Partition] = {
    partitioner match {
      case Some(rp:ReplicaPartitioner) => prev.partitions.map(partition => rp.getEndpointParititon(partition))
      case _ => prev.partitions
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split match {
      case epp: ReplicaPartition =>
        epp.endpoints.map(_.getHostAddress).toSeq // We were previously partitioned using the ReplicaPartitioner
      case other: Partition => prev.preferredLocations(split) //Fall back to last RDD's preferred spot
    }
  }



}