package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{Partition, Partitioner}


case class EndpointPartition(index: Int, endpoint: Set[InetAddress]) extends Partition

class ReplicaPartitioner(partitionsPerReplicaSet: Int, connector:CassandraConnector) extends Partitioner {
    val hosts = connector.hosts
    val hostMap = hosts.zipWithIndex.toMap
    //TODO We Need JAVA-312 to get sets of replicas instead of single endpoints
    val indexMap = (0 to partitionsPerReplicaSet).flatMap(offset => hostMap.map { case (hosts, index) => (index + offset, Set(hosts))}).toMap
    val numHosts = hosts.size
    val rand = new java.util.Random()

    override def getPartition(key: Any): Int = {
      val replicaSet = key.asInstanceOf[Set[InetAddress]]
      val offset = rand.nextInt(partitionsPerReplicaSet)
      hostMap.getOrElse(replicaSet.last, rand.nextInt(numHosts)) + offset
    }

    override def numPartitions: Int = partitionsPerReplicaSet * numHosts

    def getEndpointParititon(partition: Partition): EndpointPartition = {
      val endpoints = indexMap.getOrElse(partition.index, throw new RuntimeException(s"${indexMap} : Can't get an endpoint for Partition $partition.index"))
      new EndpointPartition(index = partition.index, endpoint = endpoints)
    }

  }

