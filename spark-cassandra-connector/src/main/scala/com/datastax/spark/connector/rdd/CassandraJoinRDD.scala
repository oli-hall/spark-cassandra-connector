package com.datastax.spark.connector.rdd

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{ReplicaPartition, ReplicaPartitioner}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

// O[ld] Is the type of the left Side RDD, N[ew] the type of the right hand side Results
class CassandraJoinRDD[O, N] private[connector](prev: RDD[O],
                                                keyspaceName: String,
                                                tableName: String,
                                                connector: CassandraConnector,
                                                columns: ColumnSelector = AllColumns,
                                                where: CqlWhereClause = CqlWhereClause.empty,
                                                readConf: ReadConf = ReadConf())
                                               (implicit oldTag: ClassTag[O], newTag: ClassTag[N],
                                                @transient rwf: RowWriterFactory[O], @transient rrf: RowReaderFactory[N])
  extends BaseCassandraRDD[N, (O, N)](prev.sparkContext, connector, keyspaceName, tableName, columns, where, readConf, prev.dependencies) {

  //Make sure copy operations make new CJRDDs and not CRDDs
  override protected def copy(columnNames: ColumnSelector = columnNames,
                    where: CqlWhereClause = where,
                    readConf: ReadConf = readConf, connector: CassandraConnector = connector) =
    new CassandraJoinRDD[O, N](prev, keyspaceName, tableName, connector, columnNames, where, readConf).asInstanceOf[this.type]

  /** Todo, allow for more complicated joining returns the names of columns to be joined on in the table. */
  lazy val joinColumnNames: Seq[NamedColumnRef] = {
    tableDef.partitionKey.map(col => col.columnName: NamedColumnRef).toSeq
  }

  val rowWriter = implicitly[RowWriterFactory[O]].rowWriter(
    tableDef,
    joinColumnNames.map(_.columnName),
    checkColumns = CheckLevel.CheckPartitionOnly)

  //We need to make sure we get selectedColumnNames before serialization so that our RowReader is
  //built
  private val singleKeyCqlQuery: (String) = {
    require(tableDef.partitionKey.map(_.columnName).exists(
      partitionKey => where.predicates.exists(_.contains(partitionKey))) == false,
      "Partition keys are not allowed in the .where() clause of a Cassandra Join")
    logDebug("Generating Single Key Query Prepared Statement String")
    val columns = selectedColumnNames.map(_.cql).mkString(", ")
    val joinWhere = joinColumnNames.map(_.columnName).map(name => s"${quote(name)} = :$name")
    val filter = (where.predicates ++ joinWhere).mkString(" AND ")
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
  override def compute(split: Partition, context: TaskContext): Iterator[(O, N)] = {
    connector.withSessionDo { session =>
      logDebug(s"Query::: $singleKeyCqlQuery")
      implicit val pv = protocolVersion(session)
      val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
      val bsb = new BoundStatementBuilder[O](rowWriter, stmt, pv)
      fetchIterator(session, bsb, prev.iterator(split, context))
    }
  }

  def fetchIterator(session: Session, bsb: BoundStatementBuilder[O], lastIt: Iterator[O]): Iterator[(O, N)] = {
    val columnNamesArray = selectedColumnNames.map(_.selectedAs).toArray
    implicit val pv = protocolVersion(session)
    lastIt.map(leftSide => (leftSide, bsb.bind(leftSide))).flatMap { case (leftSide, boundStmt) =>
      val rs = session.execute(boundStmt)
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val result = iterator.map(rightSide => (leftSide, rowTransformer.read(rightSide, columnNamesArray)))
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
      case Some(rp: ReplicaPartitioner) => prev.partitions.map(partition => rp.getEndpointParititon(partition))
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