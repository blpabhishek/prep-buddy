package org.apache.datacommons.prepbuddy.clusterers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.exceptions.ApplicationException
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class ClusterTest extends SparkTestCase {
    test("should Give Cluster Of Similar Column Values") {
        val dataSet = Array(
            "CLUSTER Of Finger print",
            "finger print of\tcluster",
            "finger print of\tcluster",
            "finger print for cluster"
        )
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val clusters: Clusters = transformableRDD.clusters(0, new SimpleFingerprintAlgorithm)
        val listOfCluster: List[Cluster] = clusters.getAllClusters

        assert(2 == listOfCluster.size)
        assert(listOfCluster.head.contain(("CLUSTER Of Finger print", 1)))
        assert(listOfCluster.head.contain(("finger print of\tcluster", 2)))

        assert(!listOfCluster.head.contain(("finger print for cluster", 1)))
    }

    test("cluster By NGramFingerPrint Should Give Clusters By NGramMethod") {
        val dataSet = Array("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster")
        val initialDataset: RDD[String] = sparkContext.parallelize(dataSet)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)

        val clusters: Clusters = initialRDD.clusters(0, new NGramFingerprintAlgorithm(1))
        val clustersWithSizeGreaterThanOne: List[Cluster] = clusters.getClustersWithSizeGreaterThan(2)

        assert(1 == clustersWithSizeGreaterThanOne.size)
        assert(3 == clustersWithSizeGreaterThanOne.head.size)
    }

    test("clusterUsing LevenshteinDistance Should Give Clusters ByDistanceMethod") {
        val dataSet = Array("cluster Of Finger print", "finger print of cluster", "finger print for cluster")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val clusters: Clusters = transformableRDD.clusters(0, new LevenshteinDistance)
        val clustersWithSizeGreaterThanOne: List[Cluster] = clusters.getClustersWithSizeGreaterThan(1)

        assert(1 == clustersWithSizeGreaterThanOne.size)
        assert(2 == clustersWithSizeGreaterThanOne.head.size)
    }

    test("should throw NEGATIVE_COLUMN_INDEX exception when negative column index is passed to cluster") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val thrown: ApplicationException = intercept[ApplicationException] {
            initialRDD.clusters(-10, new SimpleFingerprintAlgorithm())
        }

        assert(thrown.getMessage == "Column index can not be negative.")
    }

    test("should throw COLUMN_INDEX_OUT_OF_BOUND exception when the given column index is more than the current rdd") {
        val initialDataSet: RDD[String] = sparkContext.parallelize(Array(
            "1,NULL,2,3,4", "2,N/A,23,21,23",
            "3,N/A,21,32,32", "4,-,2,3,4",
            "5,,54,32,54", "6,32,22,33,23"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)

        val thrown: ApplicationException = intercept[ApplicationException] {
            initialRDD.clusters(5, new SimpleFingerprintAlgorithm())
        }

        assert(thrown.getMessage == "Column index is out of bound.")
    }
}
