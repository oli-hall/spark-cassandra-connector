package com.datastax.spark.connector.rdd

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.metrics.InputMetricsUpdater
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

// O[ld] Is the type of the left Side RDD, N[ew] the type of the right hand side Results
/**
 * An RDD that will do a selecting join between `prev:RDD` and the specified Cassandra Table
 * This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDD's that have been partitioned with the [[ReplicaPartitioner]]
 */
class CassandraJoinRDD[O, N] private[connector](prev: RDD[O],
                                                val keyspaceName: String,
                                                val tableName: String,
                                                val connector: CassandraConnector,
                                                val columnNames: ColumnSelector = AllColumns,
                                                val joinColumns: ColumnSelector = PartitionKeyColumns,
                                                val where: CqlWhereClause = CqlWhereClause.empty,
                                                val readConf: ReadConf = ReadConf())
                                               (implicit oldTag: ClassTag[O], val rct: ClassTag[N],
                                                @transient val rwf: RowWriterFactory[O], @transient val rtf: RowReaderFactory[N])
  extends CassandraRDD[(O, N)](prev.sparkContext, prev.dependencies) with CassandraReader[N] {

  //Make sure copy operations make new CJRDDs and not CRDDs
  override protected def copy(columnNames: ColumnSelector = columnNames,
                              where: CqlWhereClause = where,
                              readConf: ReadConf = readConf, connector: CassandraConnector = connector) =
    new CassandraJoinRDD[O, N](prev, keyspaceName, tableName, connector, columnNames, joinColumns, where, readConf).asInstanceOf[this.type]

  lazy val joinColumnNames: Seq[NamedColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException("Unable to join against all columns in a Cassandra Table. Only primary key columns allowed")
    case PartitionKeyColumns => tableDef.partitionKey.map(col => col.columnName: NamedColumnRef).toSeq
    case SomeColumns(cs@_*) => {
      checkColumnsExistence(cs)
      checkValidJoin(cs)
    }
  }

  protected def checkValidJoin(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet
    val clusteringColumnsNames = tableDef.clusteringColumns.map(_.columnName).toSet

    def checkSingleColumn(column: NamedColumnRef) = {
      if (regularColumnNames.contains(column.columnName))
        throw new IllegalArgumentException(s"Can't pushdown join on column $column because it is not part of the PRIMARY KEY")
      column
    }

    //Make sure we have all of the clustering indexes between the 0th position and the max requested in the join
    val chosenClusteringColumns = tableDef.clusteringColumns
      .filter(tableCol => columns.exists(joinCol => tableCol.columnName == joinCol.columnName))
    if (chosenClusteringColumns.size != 0) {
      val chosenClusteringIndexes = chosenClusteringColumns.flatMap(col => col.componentIndex)
      val maxIndex = chosenClusteringIndexes.max
      val requiredIndexes = 0 to maxIndex
      val missingIndexes = requiredIndexes.toSet -- chosenClusteringIndexes.toSet
      if (missingIndexes.size != 0) {
        val maxCol = tableDef.clusteringColumns
          .filter(cc => cc.componentIndex.get == maxIndex)
          .map(_.columnName).mkString(", ")
        val otherCols = tableDef.clusteringColumns
          .filter(cc => missingIndexes.contains(cc.componentIndex.get))
          .map(_.columnName).mkString(", ")
        throw new IllegalArgumentException(s"Can't pushdown join on column ${maxCol} without also specifying [ ${otherCols} ]")
      }
    }

    //Partition Keys checked when creating RowWriter

    columns.map(checkSingleColumn)
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

  override protected def getPartitions: Array[Partition] = prev.partitions
}