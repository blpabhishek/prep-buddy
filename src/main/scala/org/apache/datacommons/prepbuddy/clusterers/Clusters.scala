package org.apache.datacommons.prepbuddy.clusterers


import scala.collection.mutable.ListBuffer

/**
  * Clusters is a collection of cluster of a column.
  */
class Clusters {
    private val clusters: ListBuffer[Cluster] = ListBuffer.empty

    def getAllClusters: List[Cluster] = clusters.toList

    def getClustersWithSizeGreaterThan(threshold: Int): List[Cluster] = clusters.filter(_.size > threshold).toList

    def getClustersExactlyOfSize(size: Int): List[Cluster] = clusters.filter(_.size == size).toList

    def add(key: String, tuple: (String, Int)) {
        val option: Option[Cluster] = getClusterOf(key)
        var cluster: Cluster = option.orNull
        if (cluster == null) {
            cluster = new Cluster(key)
            clusters += cluster
        }
        cluster.add(tuple)
    }

    private def getClusterOf(key: String): Option[Cluster] = clusters.find(_.isOfKey(key))
}
