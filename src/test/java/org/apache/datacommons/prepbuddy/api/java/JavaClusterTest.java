package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.clusterers.LevenshteinDistance;
import org.apache.datacommons.prepbuddy.clusterers.NGramFingerprintAlgorithm;
import org.apache.datacommons.prepbuddy.clusterers.SimpleFingerprintAlgorithm;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JavaClusterTest extends JavaSparkTestCase {
    @Test
    public void shouldGiveClusterOfSimilarColumnValues() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaClusters clusters = initialRDD.clusters(0, new SimpleFingerprintAlgorithm());

        Tuple2<String, Integer> expected1 = new Tuple2<>("CLUSTER Of Finger print", 1);
        Tuple2<String, Integer> expected2 = new Tuple2<>("finger print of cluster", 1);
        Tuple2<String, Integer> expected3 = new Tuple2<>("finger print for cluster", 1);

        List<JavaCluster> listOfCluster = clusters.getAllClusters();
        assertEquals(2, listOfCluster.size());

        assertTrue(listOfCluster.get(0).contains(expected1));
        assertTrue(listOfCluster.get(0).contains(expected2));
        assertFalse(listOfCluster.get(0).contains(expected3));
        initialRDD.collect();
    }

    @Test
    public void clusterByNGramFingerPrintShouldGiveClustersByNGramMethod() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaClusters clusters = initialRDD.clusters(0, new NGramFingerprintAlgorithm(1));

        List<JavaCluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(2);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(3, clustersWithSizeGreaterThanOne.get(0).size());
    }

    @Test
    public void clusterUsingLevenshteinDistanceShouldGiveClustersByDistanceMethod() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("cluster Of Finger print", "finger print of cluster", "finger print for cluster"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaClusters clusters = initialRDD.clusters(0, new LevenshteinDistance());

        List<JavaCluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(1);
        assertEquals(1, clustersWithSizeGreaterThanOne.size());
        assertEquals(2, clustersWithSizeGreaterThanOne.get(0).size());

    }
}
