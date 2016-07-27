package org.apache.datacommons.prepbuddy.api.java

import org.apache.datacommons.prepbuddy.clusterers.Cluster


class JavaCluster(cluster: Cluster) {
    def scalaCluster: Cluster = cluster

    def contains(tuple: (String, Integer)): Boolean = cluster.contain(tuple.asInstanceOf[(String, Int)])

    def size: Int = cluster.size

    def containsValue(value: String): Boolean = cluster.containsValue(value)
}
