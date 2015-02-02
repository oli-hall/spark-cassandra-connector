package com.datastax.spark.connector.rdd

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{FlatSpec, Matchers}


case class KVRow (key: Int)

class RDDSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate{

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(cassandraHost))
  implicit val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val keyspace = "rdd_test"
  val tableName = "key_value"
  val otherTable = "other_table"
  val bigTableRowCount = 100000
  val keys = Seq(1,2,3)

  conn.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$tableName (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    for (value <- keys) {
      session.execute(s"INSERT INTO $keyspace.$tableName (key, group, value) VALUES ($value, ${value * 100}, '${value.toString}')")
    }

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$otherTable (key INT, group BIGINT,  PRIMARY KEY (key))")
    for (value <- keys) {
      session.execute(s"INSERT INTO $keyspace.$otherTable (key, group) VALUES ($value, ${value * 100})")
    }
  }


  "A Tuple RDD specifying partition keys" should "be retreivable from Cassandra" in {

    val someCass = sc.parallelize(1 to 3).map( Tuple1(_)).fetchFromCassandra(keyspace, tableName)
    println("Dependencies on someCass")
    println(someCass.toDebugString)
    val result = someCass.collect
    result should have length  3
    result.foreach(println)
  }

  "A case-class RDD specifying partition keys" should "be retrievable from Cassandra" in {
    val someCass = sc.parallelize(1 to 3).map( x => new KVRow(x)).fetchFromCassandra(keyspace, tableName)
    println("Dependencies on someCass")
    println(someCass.toDebugString)
    val result = someCass.collect
    result should have length  3
    result.foreach(println)

  }

  "A Tuple RDD specifying partitioning keys and clustering keys " should "be retrievable from Cassandra" in {
    val someCass = sc.parallelize(1 to 3).map( x=> (x,x*100:Long)).fetchFromCassandra(keyspace, tableName)
    println("Dependencies on someCass")
    println(someCass.toDebugString)
    val result = someCass.collect
    result should have length  3
    result.foreach(println)
  }

  "A CassandraRDD " should "be retreivable from Cassandra" in {
    val someCass = sc.cassandraTable(keyspace,otherTable).fetchFromCassandra(keyspace, tableName)
    println("Dependencies on someCass")
    println(someCass.toDebugString)
    val result = someCass.collect
    result should have length  3
    result foreach println
  }

  it should " be retreivable without repartitioning" in {
    val someCass = sc.cassandraTable(keyspace,otherTable).fetchFromCassandra(keyspace, tableName, false)
    println("Dependencies on someCass")
    println(someCass.toDebugString)
    someCass.toDebugString should not contain "ShuffledRDD"
    val result = someCass.collect
    result should have length  3
    result foreach println

  }

  "A fetched CassandraRDD " should " support select clauses " in {
    val someCass = sc.cassandraTable(keyspace,otherTable).fetchFromCassandra(keyspace, tableName).select("value")
    val results = someCass.collect.flatMap(_.getString(0)).sorted
    results should be (Array('1','2','3'))
  }

  "A fetched CassandraRDD " should " support where clauses" in {
    val someCass = sc.parallelize(1 to 3).map( x => new KVRow(x)).fetchFromCassandra(keyspace, tableName).where("group >= 200")
    val results = someCass.collect
    results should have length 2
    results foreach println
  }


}
