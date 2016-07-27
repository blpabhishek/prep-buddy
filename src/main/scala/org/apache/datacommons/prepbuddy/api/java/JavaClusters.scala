package org.apache.datacommons.prepbuddy.api.java

import java.util.{List => JList}

import org.apache.datacommons.prepbuddy.clusterers.Clusters

import scala.collection.JavaConverters._


class JavaClusters(clusters: Clusters) extends Serializable {

    def getAllClusters: JList[JavaCluster] = clusters.getAllClusters.map(new JavaCluster(_)).asJava

    def getClustersWithSizeGreaterThan(threshold: Int): JList[JavaCluster] = {
        clusters.getClustersWithSizeGreaterThan(threshold).map(new JavaCluster(_)).asJava
    }

    def getClustersExactlyOfSize(size: Int): JList[JavaCluster] = {
        clusters.getClustersExactlyOfSize(size).map(new JavaCluster(_)).asJava
    }
}
