package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.driver.core.{ProtocolVersion, Session}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.reader._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Dependency, SparkContext}

import scala.language.existentials
import scala.reflect.ClassTag


/** RDD representing a Cassandra table.
  * This class is the main entry point for analyzing data in Cassandra database with Spark.
  * Obtain objects of this class by calling [[com.datastax.spark.connector.SparkContextFunctions# c a s s a n d r a T a b l e c a s s a n d r a T a b l e]].
  *
  * Configuration properties should be passed in the `SparkConf` configuration of `SparkContext`.
  * `CassandraRDD` needs to open connection to Cassandra, therefore it requires appropriate connection property values
  * to be present in `SparkConf`. For the list of required and available properties, see
  * [[com.datastax.spark.connector.cql.CassandraConnector C a s s a n d r a C o n n e c t o r]].
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
abstract class BaseCassandraRDD[R, P] private[connector](
                                                          @transient sc: SparkContext,
                                                          val connector: CassandraConnector,
                                                          val keyspaceName: String,
                                                          val tableName: String,
                                                          val columnNames: ColumnSelector = AllColumns,
                                                          val where: CqlWhereClause = CqlWhereClause.empty,
                                                          val readConf: ReadConf = ReadConf(),
                                                          val dependecyList: Seq[Dependency[_]] = Seq.empty)(
                                                          implicit
                                                          ct: ClassTag[R], rddt: ClassTag[P],
                                                          @transient rtf: RowReaderFactory[R])
  extends RDD[P](sc, dependecyList) with Logging {

  protected def fetchSize = readConf.fetchSize

  protected def splitSize = readConf.splitSize

  protected def consistencyLevel = readConf.consistencyLevel

  protected def copy(columnNames: ColumnSelector = columnNames,
                     where: CqlWhereClause = where,
                     readConf: ReadConf = readConf, connector: CassandraConnector = connector): this.type

  /** Returns a copy of this Cassandra RDD with specified connector */
  def withConnector(connector: CassandraConnector) =
    copy(connector = connector)

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column. */
  def where(cql: String, values: Any*): this.type = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Allows to set custom read configuration, e.g. consistency level or fetch size. */
  def withReadConf(readConf: ReadConf) = {
    copy(readConf = readConf)
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(columns: Seq[NamedColumnRef], availableColumns: Seq[NamedColumnRef]) {
    val availableColumnsSet = availableColumns.collect {
      case ColumnName(columnName) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case ColumnName(columnName) if !availableColumnsSet.contains(columnName) => columnName
    }

    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }

  /** Filters currently selected set of columns with a new set of columns */
  private def narrowColumnSelection(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] = {
    columnNames match {
      case SomeColumns(cs@_*) =>
        checkColumnsAvailable(columns, cs)
      case AllColumns =>
      // we do not check for column existence yet as it would require fetching schema and a call to C*
      // columns existence will be checked by C* once the RDD gets computed.
    }
    columns
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.
    *
    * The selected columns are [[NamedColumnRef]] instances. This type allows to specify columns for
    * straightforward retrieval and to read TTL or write time of regular columns as well. Implicit
    * conversions included in [[com.datastax.spark.connector]] package make it possible to provide
    * just column names (which is also backward compatible) and optional add `.ttl` or `.writeTime`
    * suffix in order to create an appropriate [[NamedColumnRef]] instance.
    */
  def select(columns: NamedColumnRef*): this.type = {
    copy(columnNames = SomeColumns(narrowColumnSelection(columns): _*))
  }

  lazy val tableDef = {
    Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  protected lazy val rowTransformer = implicitly[RowReaderFactory[R]].rowReader(tableDef)

  protected def checkColumnsExistence(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet

    def checkSingleColumn(column: NamedColumnRef) = {
      if (!allColumnNames.contains(column.columnName))
        throw new IOException(s"Column $column not found in table $keyspaceName.$tableName")

      column match {
        case ColumnName(_) =>

        case TTL(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")

        case WriteTime(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
      }

      column
    }

    columns.map(checkSingleColumn)
  }


  /** Returns the names of columns to be selected from the table. */
  lazy val selectedColumnNames: Seq[NamedColumnRef] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => tableDef.allColumns.map(col => col.columnName: NamedColumnRef).toSeq
        case SomeColumns(cs@_*) => checkColumnsExistence(cs)
      }

    (rowTransformer.columnNames, rowTransformer.requiredColumns) match {
      case (Some(cs), None) => providedColumnNames.filter(columnName => cs.toSet(columnName.selectedAs))
      case (_, _) => providedColumnNames
    }
  }


  /** Checks for existence of keyspace, table, columns and whether the number of selected columns corresponds to
    * the number of the columns expected by the target type constructor.
    * If successful, does nothing, otherwise throws appropriate `IOException` or `AssertionError`. */
  lazy val verify = {
    val targetType = implicitly[ClassTag[R]]

    tableDef.allColumns // will throw IOException if table does not exist

    rowTransformer.columnNames match {
      case Some(names) =>
        val missingColumns = names.toSet -- selectedColumnNames.map(_.selectedAs).toSet
        assert(missingColumns.isEmpty, s"Missing columns needed by $targetType: ${missingColumns.mkString(", ")}")
      case None =>
    }

    rowTransformer.requiredColumns match {
      case Some(count) =>
        assert(selectedColumnNames.size >= count,
          s"Not enough columns selected for the target row type $targetType: ${selectedColumnNames.size} < $count")
      case None =>
    }
  }

  protected lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  protected def quote(name: String) = "\"" + name + "\""

  def protocolVersion(session: Session): ProtocolVersion = {
    session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }

}


