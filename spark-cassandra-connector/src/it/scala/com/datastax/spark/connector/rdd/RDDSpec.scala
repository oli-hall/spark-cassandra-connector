package com.datastax.spark.connector.rdd

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{FlatSpec, Matchers}

class NormalRDDSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate{

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(cassandraHost))
  implicit val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)

  val bigTableRowCount = 100000
  val keys = Seq(1,2,3)

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS rdd_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS rdd_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    for (value <- keys) {
      session.execute(s"INSERT INTO rdd_test.key_value (key, group, value) VALUES ($value, ${value * 100}, '${value.toString}')")
    }
  }

  "A RDD" should "be able to be mapped to Hosts" in {
    val keys = sc.parallelize(1 to 3).map(Tuple1(_))
    val result = keys.keyByReplica("rdd_test","key_value").map(_._1)collect()
    result should have length 3
    result.foreach(println)
    result.foreach( _ should contain (cassandraHost))
  }

  it should "Be Able to Repartition by Replica" in {
    val keys = sc.parallelize(1 to 10000).map(Tuple1(_)).partitionByReplica("rdd_test","key_value", 20)
    val partitions = keys.partitions
    partitions should have size 20
    partitions.foreach(println(_))
  }



}
