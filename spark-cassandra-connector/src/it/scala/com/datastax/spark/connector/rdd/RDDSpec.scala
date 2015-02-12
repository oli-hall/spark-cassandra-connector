package com.datastax.spark.connector.rdd

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{FlatSpec, Matchers}


case class KVRow(key: Int)

case class FullRow(key: Int, group: Long, value: String)

class RDDSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(cassandraHost))
  implicit val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val keyspace = "rdd_test"
  val tableName = "key_value"
  val otherTable = "other_table"
  val keys = 0 to 200
  val total = 0 to 10000

  conn.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$tableName (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    for (value <- total) {
      session.execute(s"INSERT INTO $keyspace.$tableName (key, group, value) VALUES ($value, ${value * 100}, '${value.toString}')")
    }

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$otherTable (key INT, group BIGINT,  PRIMARY KEY (key))")
    for (value <- keys) {
      session.execute(s"INSERT INTO $keyspace.$otherTable (key, group) VALUES ($value, ${value * 100})")
    }
  }

  def checkLeftSide[T, S](leftSideSource: Array[T], result: Array[(T, S)]) = {
    markup("Checking LeftSide")
    val leftSideResults = result.map(_._1)
    for (element <- leftSideSource) {
      leftSideResults should contain(element)
    }
  }

  def checkArrayCassandraRow[T](result: Array[(T, CassandraRow)]) = {
    markup("Checking RightSide Join Results")
    result.length should be(keys.length)
    for (key <- keys) {
      val sorted_result = result.map(_._2).sortBy(_.getInt(0))
      sorted_result(key).getInt("key") should be(key)
      sorted_result(key).getLong("group") should be(key * 100)
      sorted_result(key).getString("value") should be(key.toString)
    }
  }

  def checkArrayTuple[T](result: Array[(T, (Int, Long, String))]) = {
    markup("Checking RightSide Join Results")
    result.length should be(keys.length)
    for (key <- keys) {
      val sorted_result = result.map(_._2).sortBy(_._1)
    sorted_result(key)._1 should be(key)
    sorted_result(key)._2 should be(key * 100)
    sorted_result(key)._3 should be(key.toString)
  }
  }

  def checkArrayFullRow[T](result: Array[(T, FullRow)]) = {
    markup("Checking RightSide Join Results")
    result.length should be(keys.length)
    for (key <- keys) {
      val sorted_result = result.map(_._2).sortBy(_.key)
      sorted_result(key).key should be(key)
      sorted_result(key).group should be(key * 100)
      sorted_result(key).value should be(key.toString)
    }
  }

  "A Tuple RDD specifying partition keys" should "be joinable with Cassandra" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val someCass = source.joinWithCassandraTable(keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayCassandraRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a tuple from Cassandra" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val someCass = source.joinWithCassandraTable[(Int, Long, String)](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayTuple(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a case class from cassandra" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val someCass = source.joinWithCassandraTable[FullRow](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayFullRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be repartitionable" in {
    val source = sc.parallelize(keys).map(Tuple1(_))
    val repart = source.repartitionByCassandraReplica(keyspace, tableName, 10)
    repart.partitions.length should be(conn.hosts.size * 10)
    val someCass = repart.joinWithCassandraTable(keyspace, tableName)
    someCass.getPartitions.foreach {
      case e: EndpointPartition =>
        conn.hosts should contain(e.endpoints.head)
      case _ =>
        fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
    }
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }

  "A case-class RDD specifying partition keys" should "be retrievable from Cassandra" in {
    val source = sc.parallelize(keys).map(x => new KVRow(x))
    val someCass = source.joinWithCassandraTable(keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayCassandraRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a tuple from Cassandra" in {
    val source = sc.parallelize(keys).map(x => new KVRow(x))
    val someCass = source.joinWithCassandraTable[(Int, Long, String)](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayTuple(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a case class from cassandra" in {
    val source = sc.parallelize(keys).map(x => new KVRow(x))
    val someCass = source.joinWithCassandraTable[FullRow](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayFullRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be repartitionable" in {
    val source = sc.parallelize(keys).map(x => new KVRow(x))
    val repart = source.repartitionByCassandraReplica(keyspace, tableName, 10)
    repart.partitions.length should be(conn.hosts.size * 10)
    val someCass = repart.joinWithCassandraTable(keyspace, tableName)
    someCass.getPartitions.foreach {
      case e: EndpointPartition =>
        conn.hosts should contain(e.endpoints.head)
      case _ =>
        fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
    }
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }


  "A Tuple RDD specifying partitioning keys and clustering keys " should "be retrievable from Cassandra" in {
    val source = sc.parallelize(keys).map(x => (x, x * 100: Long))
    val someCass = source.joinWithCassandraTable(keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayCassandraRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a tuple from Cassandra" in {
    val source = sc.parallelize(keys).map(x => (x, x * 100: Long))
    val someCass = source.joinWithCassandraTable[(Int, Long, String)](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayTuple(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a case class from cassandra" in {
    val source = sc.parallelize(keys).map(x => (x, x * 100: Long))
    val someCass = source.joinWithCassandraTable[FullRow](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayFullRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be repartitionable" in {
    val source = sc.parallelize(keys).map(x => (x, x * 100: Long))
    val repart = source.repartitionByCassandraReplica(keyspace, tableName, 10)
    repart.partitions.length should be(conn.hosts.size * 10)
    val someCass = repart.joinWithCassandraTable(keyspace, tableName)
    someCass.getPartitions.foreach {
      case e: EndpointPartition =>
        conn.hosts should contain(e.endpoints.head)
      case _ =>
        fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
    }
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }


  "A CassandraRDD " should "be joinable with Cassandra" in {
    val source = sc.cassandraTable(keyspace, otherTable)
    val someCass = source.joinWithCassandraTable(keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayCassandraRow(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a tuple from Cassandra" in {
    val source = sc.cassandraTable(keyspace, otherTable)
    val someCass = source.joinWithCassandraTable[(Int, Long, String)](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayTuple(result)
    checkLeftSide(leftSide, result)
  }

  it should "be retreivable as a case class from cassandra" in {
    val source = sc.cassandraTable(keyspace, otherTable)
    val someCass = source.joinWithCassandraTable[FullRow](keyspace, tableName)
    val result = someCass.collect
    val leftSide = source.collect
    checkArrayFullRow(result)
    checkLeftSide(leftSide, result)
  }

  it should " be retreivable without repartitioning" in {
    val someCass = sc.cassandraTable(keyspace, otherTable).joinWithCassandraTable(keyspace, tableName)
    someCass.toDebugString should not contain "ShuffledRDD"
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }

  it should "be repartitionable" in {
    val source = sc.cassandraTable(keyspace, otherTable)
    val repart = source.repartitionByCassandraReplica(keyspace, tableName, 10)
    repart.partitions.length should be(conn.hosts.size * 10)
    val someCass = repart.joinWithCassandraTable(keyspace, tableName)
    someCass.getPartitions.foreach {
      case e: EndpointPartition =>
        conn.hosts should contain(e.endpoints.head)
      case _ =>
        fail("Unable to get endpoints on repartitioned RDD, This means preferred locations will be broken")
    }
    val result = someCass.collect
    checkArrayCassandraRow(result)
  }

  "A Joined CassandraRDD " should " support select clauses " in {
    val someCass = sc.cassandraTable(keyspace, otherTable).joinWithCassandraTable(keyspace, tableName).select("value")
    val results = someCass.collect.map(_._2).map(_.getInt("value")).sorted
    results should be(keys.toArray)
  }

  it should " support where clauses" in {
    val someCass = sc.parallelize(keys).map(x => new KVRow(x)).joinWithCassandraTable(keyspace, tableName).where("group >= 500")
    val results = someCass.collect.map(_._2)
    results should have length (keys.filter(_ >= 5).length)
  }

  it should " throw an exception if you try to filter on a column in the Partition key" in {
    intercept[IllegalArgumentException] {
      val someCass = sc.parallelize(keys).map(x => new KVRow(x)).joinWithCassandraTable(keyspace, tableName).where("key = 200")
    }
  }

}
