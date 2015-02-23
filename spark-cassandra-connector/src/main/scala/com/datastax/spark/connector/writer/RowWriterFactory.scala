package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

/** Creates instances of [[RowWriter]] objects for the given row type `T`.
  * `RowWriterFactory` is the trait you need to implement if you want to support row representations
  * which cannot be simply mapped by a [[com.datastax.spark.connector.mapper.ColumnMapper C o l u m n M a p p e r]]. */
trait RowWriterFactory[T] {

  /** Creates a new `RowWriter` instance.
    * @param table target table the user wants to write into
    * @param columnNames columns selected by the user; the user might wish to write only a subset of columns */
  def rowWriter(table: TableDef, columnNames: Seq[String]): RowWriter[T]
}

/** Provides a low-priority implicit `RowWriterFactory` able to write objects of any class for which
  * a [[com.datastax.spark.connector.mapper.ColumnMapper C o l u m n M a p p e r]] is defined. */
trait LowPriorityRowWriterFactoryImplicits {
  implicit def defaultRowWriterFactory[T: ColumnMapper]: RowWriterFactory[T] = DefaultRowWriter.factory
}

/** Provides an implicit `RowWriterFactory` for saving [[com.datastax.spark.connector.CassandraRow C a s s a n d r a R o w]] objects. */
object RowWriterFactory extends LowPriorityRowWriterFactoryImplicits {
  implicit val genericRowWriterFactory = CassandraRowWriter.Factory
}