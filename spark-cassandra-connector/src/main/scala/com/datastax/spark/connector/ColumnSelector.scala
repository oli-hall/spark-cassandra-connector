package com.datastax.spark.connector

import scala.language.implicitConversions

sealed trait ColumnSelector {
  def toAliasMap: Map[String, String]
}

case object AllColumns extends ColumnSelector {
  override def toAliasMap: Map[String, String] = Map.empty
}

case class SomeColumns(columns: NamedColumnRef*) extends ColumnSelector {
  override def toAliasMap: Map[String, String] = columns.map {
    case ref => (ref.alias.getOrElse(ref.selectedAs), ref.selectedAs)
  }.toMap
}

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: NamedColumnRef): _*)
}


