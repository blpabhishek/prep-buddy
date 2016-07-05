package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD
import org.junit.Assert._


class TransformableRDDTest extends SparkTestCase {

    test("textfacets highest should give one highest pair if only one pair found") {
        val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        assert(5 == transformableRDD.count())
    }

    test("should drop the specified column from the given rdd") {
        val data = Array(
            "John,Male,21,Canada",
            "Smith, Male, 30, UK",
            "Larry, Male, 23, USA",
            "Fiona, Female,18,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)
        val transformedRows: Array[String] = transformableRDD.dropColumn(2).collect()

        assert(transformedRows.contains("John,Male,Canada"))
        assert(transformedRows.contains("Smith,Male,UK"))
        assert(transformedRows.contains("Larry,Male,USA"))
        assert(transformedRows.contains("Fiona,Female,USA"))
    }

    test("toDoubleRdd should give double RDD of given column index") {
        val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataSet, CSV)
        val doubleRdd: RDD[Double] = transformableRDD.toDoubleRDD(0)
        val collected: Array[Double] = doubleRdd.collect()
        val expected: Double = 3

        assert(collected.contains(expected))
        assert(collected.contains(1))
        assert(collected.contains(2))
        assert(collected.contains(4))
        assert(collected.contains(5))
    }

    test("text facet should give count of Pair") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val textFacets: TextFacets = transformableRDD.listFacets(0)
        assertEquals(2, textFacets.count)
    }

    test("should remove rows are based on a predicate") {
        val dataSet = Array("A,1", "B,2", "C,3", "D,4", "E,5")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val predicate = (record: RowRecord) => {
            val valueAt: String = record.valueAt(0)
            valueAt.equals("A") || valueAt.equals("B")
        }
        val finalRDD: TransformableRDD = transformableRDD.removeRows(predicate)
        assertEquals(3, finalRDD.count)
    }

    test("toDoubleRDD should give rdd of double") {
        val dataSet = Array("A,1.0", "B,2.9", "C,3", "D,4", "E,w")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val doubleRDD: RDD[Double] = transformableRDD.toDoubleRDD(1)
        val collected: Array[Double] = doubleRDD.collect()
        assertTrue(collected.contains(1.0))
        assertTrue(collected.contains(2.9))
        assertTrue(collected.contains(3.0))
        assertTrue(collected.contains(4.0))
    }

    test("select should give selected column of the RDD") {
        val dataSet = Array("A,1.0", "B,2.9", "C,3", "D,4", "E,0")
        val initialRDD: RDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)
        val selectedColumn: RDD[String] = transformableRDD.select(1)

        assert(selectedColumn.collect sameElements Array("1.0", "2.9", "3", "4", "0"))
    }

    test("listFacets should give facets of given column indexes") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array("A,B,C", "D,E,F", "G,H,I"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val listFacets: TextFacets = initialRDD.listFacets(Array(1, 2))
        val listOfHighest: Array[(String, Int)] = listFacets.highest

        assert(3 == listOfHighest.length)
    }

    test("should return a double rdd by multiplying the given column indexes") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array("1,2", "1,3", "1,4"))
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val doubleRdd: RDD[Double] = initialRDD.multiplyColumns(0, 1)
        val collected: Array[Double] = doubleRdd.collect()

        assert(3 == collected.length)
        assert(collected.contains(2.0))
        assert(collected.contains(3.0))
        assert(collected.contains(4.0))
    }
}
