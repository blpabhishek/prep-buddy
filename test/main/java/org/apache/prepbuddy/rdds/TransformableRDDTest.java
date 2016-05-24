package org.apache.prepbuddy.rdds;

import org.apache.commons.io.FileUtils;
import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.groupingops.Cluster;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.SimpleFingerprintAlgorithm;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransformableRDDTest extends SparkTestCase {
    @Test
    public void shouldEncryptAColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("1,X", "2,Y", "3,Z", "4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        JavaRDD<String> dataSet = encryptedRDD.decrypt(0);
        assertEquals(dataSet.collect(), initialDataset.collect());
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("5,d,", "2,43", "13,re", "4,42"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        BigInteger sum = encryptedRDD.sum(0);

        assertEquals(new BigInteger("24"), sum);
    }

    @Test
    public void shouldBeAbleToGetAverageOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,3,s", "1,2,Y", "Z,1,p", "A,1,N"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 1);
        double average = encryptedRDD.average(1);

        assertEquals(1.75, average, 0.01);
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumnForDouble() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("3.1,X", "2.1,Y", "13.1,Z", "4.1,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);
        BigInteger sum = encryptedRDD.sum(0);

        assertEquals(new BigInteger("22"), sum);
    }

    @Test
    public void shouldBeAbleToReadAndWrite() throws IOException {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("3,X", "2,Y", "13,Z", "4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);

        FileUtils.deleteDirectory(new File("data/somePlace"));
        encryptedRDD.saveAsTextFile("data/somePlace");
        JavaRDD<String> javaRDD = javaSparkContext.textFile("data/somePlace");
        HomomorphicallyEncryptedRDD rdd = new HomomorphicallyEncryptedRDD(javaRDD, keyPair, FileType.CSV);
        JavaRDD<String> decrypt = rdd.decrypt(0);

        assertEquals(decrypt.collect(), initialDataset.collect());
    }

    @Test
    public void shouldChangeValueOfFieldOfMatchesClusters() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        Clusters clusters = initialRDD.clusters(0, new SimpleFingerprintAlgorithm());

        List<Cluster> clustersWithSizeGreaterThanOne = clusters.getClustersWithSizeGreaterThan(1);
        TransformableRDD afterMergeCluster = initialRDD.replaceValues(clustersWithSizeGreaterThanOne.get(0), "Finger print", 0);

        List<String> listOfValues = afterMergeCluster.collect();

        Assert.assertTrue(listOfValues.contains("Finger print"));
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsBasedWholeRecord() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicatedRDD = initialRDD.deduplicate();
        assertEquals(4, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsBasedOnColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicatedRDD = initialRDD.deduplicate(Arrays.asList(0, 3));
        assertEquals(3, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsBasedOnColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD duplicates = initialRDD.detectDuplicates(Arrays.asList(0, 3));
        assertEquals(1, duplicates.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsBasedOnWholeRecord() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD duplicates = initialRDD.detectDuplicates();
        assertEquals(0, duplicates.count());
    }

    @Test
    public void shouldBeAbleToSelectAColumnFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        JavaRDD<String> columnValues = initialRDD.select(0);
        List<String> expectedValues = Arrays.asList("Smith", "John", "John", "Smith");
        assertEquals(expectedValues, columnValues.collect());
    }
}
