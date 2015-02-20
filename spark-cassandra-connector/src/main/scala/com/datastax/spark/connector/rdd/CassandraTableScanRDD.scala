package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.metrics.InputMetricsUpdater
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CassandraRDDPartitioner, CqlTokenRange}
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.types.{ColumnType, TypeConverter}
import com.datastax.spark.connector.util.CountingIterator
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.ClassTag


/** RDD representing a Table Scan of A Cassandra table.
  * This class is the main entry point for analyzing data in Cassandra database with Spark.
  * Obtain objects of this class by calling [[com.datastax.spark.connector.SparkContextFunctions#cassandraTablecassandraTable]].
  *
  * Configuration properties should be passed in the `SparkConf` configuration of `SparkContext`.
  * `CassandraRDD` needs to open connection to Cassandra, therefore it requires appropriate connection property values
  * to be present in `SparkConf`. For the list of required and available properties, see
  * [[com.datastax.spark.connector.cql.CassandraConnector CassandraConnector]].
  *
  * `CassandraRDD` divides the dataset into smaller partitions, processed locally on every cluster node.
  * A data partition consists of one or more contiguous token ranges.
  * To reduce the number of roundtrips to Cassandra, every partition is fetched in batches. The following
  * properties control the number of partitions and the fetch size:
  *
  * - spark.cassandra.input.split.size:        approx number of Cassandra partitions in a Spark partition, default 100000
  * - spark.cassandra.input.page.row.size:     number of CQL rows fetched per roundtrip, default 1000
  *
  * A `CassandraRDD` object gets serialized and sent to every Spark executor.
  *
  * By default, reads are performed at ConsistencyLevel.LOCAL_ONE in order to leverage data-locality and minimize network traffic.
  * This read consistency level is controlled by the following property:
  *
  * - spark.cassandra.input.consistency.level: consistency level for RDD reads, string matching the ConsistencyLevel enum name.
  *
  * If a Cassandra node fails or gets overloaded during read, queries are retried to a different node.
  */
class CassandraTableScanRDD[R] private[connector](
                                                   @transient sc: SparkContext,
                                                   val connector: CassandraConnector,
                                                   val keyspaceName: String,
                                                   val tableName: String,
                                                   val columnNames: ColumnSelector = AllColumns,
                                                   val where: CqlWhereClause = CqlWhereClause.empty,
                                                   val readConf: ReadConf = ReadConf(),
                                                   dependecyList: Seq[Dependency[_]] = Seq.empty)(
                                                   implicit
                                                   val rct: ClassTag[R], @transient val rtf: RowReaderFactory[R])
  extends CassandraRDD[R](sc, Seq.empty) with CassandraReader[R] {

  override protected def copy(columnNames: ColumnSelector = columnNames,
                              where: CqlWhereClause = where,
                              readConf: ReadConf = readConf, connector: CassandraConnector = connector) = {
    require(sc != null,
      "RDD transformation requires a non-null SparkContext. Unfortunately SparkContext in this CassandraRDD is null. " +
        "This can happen after CassandraRDD has been deserialized. SparkContext is not Serializable, therefore it deserializes to null." +
        "RDD transformations are not allowed inside lambdas used in other RDD transformations.")
    new CassandraTableScanRDD[R](sc, connector, keyspaceName, tableName, columnNames, where, readConf).asInstanceOf[this.type]
  }

  /** Maps each row into object of a different type using provided function taking column value(s) as argument(s).
    * Can be used to convert each row to a tuple or a case class object:
    * {{{
    * sc.cassandraTable("ks", "table").select("column1").as((s: String) => s)                 // yields CassandraRDD[String]
    * sc.cassandraTable("ks", "table").select("column1", "column2").as((_: String, _: Long))  // yields CassandraRDD[(String, Long)]
    *
    * case class MyRow(key: String, value: Long)
    * sc.cassandraTable("ks", "table").select("column1", "column2").as(MyRow)                 // yields CassandraRDD[MyRow]
    * }}} */
  def as[B: ClassTag, A0: TypeConverter](f: A0 => B): CassandraTableScanRDD[B] = {
    implicit val ft = new FunctionBasedRowReader1(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowReader2(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowReader3(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter,
  A3: TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowReader4(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowReader5(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowReader6(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowReader7(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter,
  A7: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowReader8(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter,
  A8: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowReader9(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter,
  A8: TypeConverter, A9: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowReader10(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9: TypeConverter, A10: TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowReader11(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B: ClassTag, A0: TypeConverter, A1: TypeConverter, A2: TypeConverter, A3: TypeConverter,
  A4: TypeConverter, A5: TypeConverter, A6: TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9: TypeConverter, A10: TypeConverter, A11: TypeConverter](
                                                              f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowReader12(f)
    new CassandraTableScanRDD[B](sc, connector, keyspaceName, tableName, columnNames, where, readConf)
  }


  override def getPartitions: Array[Partition] = {
    verify // let's fail fast
    val tf = TokenFactory.forCassandraPartitioner(cassandraPartitionerClassName)
    val partitions = new CassandraRDDPartitioner(connector, tableDef, splitSize)(tf).partitions(where)
    logDebug(s"Created total ${partitions.size} partitions for $keyspaceName.$tableName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override def getPreferredLocations(split: Partition) =
    split.asInstanceOf[CassandraPartition]
      .endpoints.map(_.getHostName).toSeq

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnNames.map(_.cql).mkString(", ")
    val filter = (range.cql +: where.predicates).filter(_.nonEmpty).mkString(" AND ") + " ALLOW FILTERING"
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    (s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter", range.values ++ where.values)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    try {
      implicit val pv = protocolVersion(session)
      val stmt = session.prepare(cql)
      stmt.setConsistencyLevel(consistencyLevel)
      val converters = stmt.getVariables
        .map(v => ColumnType.converterToCassandra(v.getType))
        .toArray
      val convertedValues =
        for ((value, converter) <- values zip converters)
        yield converter.convert(value)
      val bstm = stmt.bind(convertedValues: _*)
      bstm.setFetchSize(fetchSize)
      bstm
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def fetchTokenRange(session: Session, range: CqlTokenRange, inputMetricsUpdater: InputMetricsUpdater): Iterator[R] = {
    val (cql, values) = tokenRangeToCqlQuery(range)
    logDebug(s"Fetching data for range ${range.cql} with $cql with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)
    val columnNamesArray = selectedColumnNames.map(_.selectedAs).toArray

    try {
      implicit val pv = protocolVersion(session)
      val tc = inputMetricsUpdater.resultSetFetchTimer.map(_.time())
      val rs = session.execute(stmt)
      tc.map(_.stop())
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val iteratorWithMetrics = iterator.map(inputMetricsUpdater.updateMetrics)
      val result = iteratorWithMetrics.map(rowTransformer.read(_, columnNamesArray))
      logDebug(s"Row iterator for range ${range.cql} obtained successfully.")
      result
    } catch {
      case t: Throwable =>
        throw new IOException(s"Exception during execution of $cql: ${t.getMessage}", t)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val session = connector.openSession()
    val partition = split.asInstanceOf[CassandraPartition]
    val tokenRanges = partition.tokenRanges
    val metricsUpdater = InputMetricsUpdater(context, 20)

    // Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
    // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
    // than all of the rows returned by the previous query have been consumed
    val rowIterator = tokenRanges.iterator.flatMap(
      fetchTokenRange(session, _, metricsUpdater))
    val countingIterator = new CountingIterator(rowIterator)

    context.addTaskCompletionListener { (context) =>
      val duration = metricsUpdater.finish() / 1000000000d
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName for partition ${partition.index} in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }
}

object CassandraTableScanRDD {
  def apply[T](sc: SparkContext, keyspaceName: String, tableName: String)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T]): CassandraTableScanRDD[T] =
    new CassandraTableScanRDD[T](
      sc, CassandraConnector(sc.getConf), keyspaceName, tableName, AllColumns, CqlWhereClause.empty)

  def apply[K, V](sc: SparkContext, keyspaceName: String, tableName: String)
                 (implicit keyCT: ClassTag[K], valueCT: ClassTag[V], rrf: RowReaderFactory[(K, V)]): CassandraTableScanRDD[(K, V)] =
    new CassandraTableScanRDD[(K, V)](
      sc, CassandraConnector(sc.getConf), keyspaceName, tableName, AllColumns, CqlWhereClause.empty)
}
