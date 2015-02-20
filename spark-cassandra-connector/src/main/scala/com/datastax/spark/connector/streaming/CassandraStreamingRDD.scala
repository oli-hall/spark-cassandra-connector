package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.rdd.{CassandraTableScanRDD, CqlWhereClause, ReadConf}
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[com.datastax.spark.connector.rdd.CassandraTableScanRDD]]*/
class CassandraStreamingRDD[R] private[connector] (
    sctx: StreamingContext,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    columns: ColumnSelector = AllColumns,
    where: CqlWhereClause = CqlWhereClause.empty,
    readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R],
    @transient override val rtf: RowReaderFactory[R])
  extends CassandraTableScanRDD[R](sctx.sparkContext, connector, keyspace, table, columns, where, readConf)
