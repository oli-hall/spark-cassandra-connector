package com.datastax.spark.connector.rdd

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.metrics.InputMetricsUpdater
import com.datastax.spark.connector.rdd.partitioner.{ReplicaPartition, ReplicaPartitioner}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

// O[ld] Is the type of the left Side RDD, N[ew] the type of the right hand side Results
/**
 * An RDD that will do a selecting join between `prev:RDD` and the specified Cassandra Table
 * This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDD's that have been partitioned with the [[ReplicaPartitioner]]
 * @param prev
 * @param keyspaceName
 * @param tableName
 * @param connector
 * @param columns
 * @param joinColumns
 * @param where
 * @param readConf
 * @param oldTag
 * @param newTag
 * @param rwf
 * @param rrf
 * @tparam O
 * @tparam N
 */
class CassandraJoinRDD[O, N] private[connector](prev: RDD[O],
                                                keyspaceName: String,
                                                tableName: String,
                                                connector: CassandraConnector,
                                                columns: ColumnSelector = AllColumns,
                                                joinColumns: ColumnSelector = PartitionKeyColumns,
                                                where: CqlWhereClause = CqlWhereClause.empty,
                                                readConf: ReadConf = ReadConf())
                                               (implicit oldTag: ClassTag[O], newTag: ClassTag[N],
                                                @transient rwf: RowWriterFactory[O], @transient rrf: RowReaderFactory[N])
  extends BaseCassandraRDD[N, (O, N)](prev.sparkContext, connector, keyspaceName, tableName, columns, where, readConf, prev.dependencies) {

  //Make sure copy operations make new CJRDDs and not CRDDs
  override protected def copy(columnNames: ColumnSelector = columnNames,
                              where: CqlWhereClause = where,
                              readConf: ReadConf = readConf, connector: CassandraConnector = connector) =
    new CassandraJoinRDD[O, N](prev, keyspaceName, tableName, connector, columnNames, joinColumns, where, readConf).asInstanceOf[this.type]

  lazy val joinColumnNames: Seq[NamedColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException("Unable to join against all columns in a Cassandra Table. Only primary key columns allowed")
    case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: NamedColumnRef).toSeq
    case SomeColumns(cs@_*) => {
      checkColumnsExistence(cs) /* andThen checkValidJoin(cs)*/
    }
  }

  val rowWriter = implicitly[RowWriterFactory[O]].rowWriter(
    tableDef,
    joinColumnNames.map(_.columnName),
    checkColumns = CheckLevel.CheckPartitionOnly)

  def on(joinColumns: ColumnSelector): CassandraJoinRDD[O, N] = {
    new CassandraJoinRDD[O, N](prev, keyspaceName, tableName, connector, columnNames, joinColumns, where, readConf).asInstanceOf[this.type]
  }

  //We need to make sure we get selectedColumnNames before serialization so that our RowReader is
  //built
  private val singleKeyCqlQuery: (String) = {
    require(tableDef.partitionKey.map(_.columnName).exists(
      partitionKey => where.predicates.exists(_.contains(partitionKey))) == false,
      "Partition keys are not allowed in the .where() clause of a Cassandra Join")
    logDebug("Generating Single Key Query Prepared Statement String")
    logDebug(s"SelectedColumns : $selectedColumnNames -- JoinColumnNames : $joinColumnNames")
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
    logDebug(s"Query::: $singleKeyCqlQuery")
    val session = connector.openSession()
    implicit val pv = protocolVersion(session)
    val stmt = session.prepare(singleKeyCqlQuery).setConsistencyLevel(consistencyLevel)
    val bsb = new BoundStatementBuilder[O](rowWriter, stmt, pv)
    val metricsUpdater = InputMetricsUpdater(context, 20)
    val rowIterator = fetchIterator(session, bsb, prev.iterator(split, context))
    val countingIterator = new CountingIterator(rowIterator)
    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName for partition ${split.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

  private def fetchIterator(session: Session, bsb: BoundStatementBuilder[O], lastIt: Iterator[O]): Iterator[(O, N)] = {
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