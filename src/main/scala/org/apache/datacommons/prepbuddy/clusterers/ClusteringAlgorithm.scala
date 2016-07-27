package org.apache.datacommons.prepbuddy.clusterers

/**
  * ClusteringAlgorithm is for implementing the algorithm which can be use to clustering the column value
  */
trait ClusteringAlgorithm extends Serializable {
    def getClusters(tuples: Array[(String, Int)]): Clusters
}
