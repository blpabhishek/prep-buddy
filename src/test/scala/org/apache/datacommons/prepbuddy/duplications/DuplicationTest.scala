package org.apache.datacommons.prepbuddy.duplications

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD
import org.junit.Assert._

class DuplicationTest extends SparkTestCase {
    test("should deduplicate a dataset by considering all the columns") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,USA,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val deduplicatedRDD: TransformableRDD = initialRDD.deduplicate()
        assert(4 == deduplicatedRDD.count)
    }

    test("should deduplicate a dataset by considering the given columns as primary key") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,USA,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val deduplicatedRDD: TransformableRDD = initialRDD.deduplicate(List(0, 1))

        assertEquals(2, deduplicatedRDD.count)
    }

    test("should detect duplicates in a dataSet by considering the given columns as primary key") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,UK,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val duplicates: Array[String] = initialRDD.duplicates(List(0, 1, 3)).collect()

        assertEquals(4, duplicates.length)
        assert(duplicates.contains("John,Male,USA,12343"))
        assert(duplicates.contains("John,Male,India,12343"))
        assertFalse(duplicates.contains("Smith,Male,USA,12345"))
    }

    test("should detect duplicates from a dataset by considering all the columns as primary key") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "John,Male,UK,12343",
            "Smith,Male,USA,12342",
            "John,Male,India,12343",
            "Smith,Male,USA,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val duplicates: Array[String] = initialRDD.duplicates().collect()

        assertEquals(1, duplicates.length)
        assert(duplicates.contains("Smith,Male,USA,12342"))
        assertFalse(duplicates.contains("Smith,Male,USA,12345"))
    }

    test("should be able to detect duplicates at the specified column") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "Cory,Male,India,12343",
            "John,Male,Japan,122343",
            "Adam,Male,India,1233243",
            "Smith,Male,Singapore,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val duplicatesAtCol2: Array[String] = initialRDD.duplicatesAt(2).collect()

        assertEquals(2, duplicatesAtCol2.length)

        assertTrue(duplicatesAtCol2.contains("India"))
        assertTrue(duplicatesAtCol2.contains("USA"))

        assertFalse(duplicatesAtCol2.contains("Singapore"))
        assertFalse(duplicatesAtCol2.contains("Japan"))
    }

    test("should deduplicate a particular column") {
        val records: Array[String] = Array(
            "Smith,Male,USA,12345",
            "John,Male,USA,12343",
            "Cory,Male,India,12343",
            "John,Male,Japan,122343",
            "Adam,Male,India,1233243",
            "Smith,Male,Singapore,12342"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(records)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val uniqueValuesAtCol2: Array[String] = initialRDD.unique(2).collect()

        assertEquals(4, uniqueValuesAtCol2.length)
        assert(uniqueValuesAtCol2.contains("USA"))
        assert(uniqueValuesAtCol2.contains("India"))
        assert(uniqueValuesAtCol2.contains("Japan"))
        assert(uniqueValuesAtCol2.contains("Singapore"))
    }
}
