package com.datastax.spark.connector.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

/**
 * RDD created by repartitionByCassandraReplica with preferred locations mapping to the CassandraReplicas
 * each partition was created for.
 */
protected[connector] class CassandraPartitionedRDD[T](prev: RDD[T])(implicit ct: ClassTag[T]) extends RDD[T](prev) {

  //We aren't going to change the data
  override def compute(split: Partition, context: TaskContext): Iterator[T] = prev.iterator(split, context)

  //W
  @transient override val partitioner: Option[Partitioner] = prev.partitioner

  /**
   * If this RDD was partitioned using the ReplicaPartitioner then that means we can get preffered locations
   * for each partition, otherwise we will rely on the previous RDD's partitioning.
   * @return
   */
  override def getPartitions: Array[Partition] = {
    partitioner match {
      case Some(rp: ReplicaPartitioner) => prev.partitions.map(partition => rp.getEndpointParititon(partition))
      case _ => throw new IllegalArgumentException("CassandraPartitionedRDD hasn't been partitioned by ReplicaPartitioner. This should be impossible")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split match {
      case epp: ReplicaPartition =>
        epp.endpoints.map(_.getHostAddress).toSeq // We were previously partitioned using the ReplicaPartitioner
      case other: Partition => throw new IllegalArgumentException("CassandraPartitionedRDD doesn't have Endpointed Partitions. This should be impossible.")
    }
  }
}
