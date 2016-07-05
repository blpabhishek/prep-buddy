package org.apache.datacommons.prepbuddy.transformers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD

class MergeJoinTest extends SparkTestCase {
    test("should merge the given columns with the separator by removing the original columns") {
        val data = Array(
            "John,Male,21,Canada",
            "Smith, Male, 30, UK",
            "Larry, Male, 23, USA",
            "Fiona, Female,18,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.mergeColumns(List(0, 3, 1), "_").collect()

        assert(result.length == 4)
        assert(result.contains("21,John_Canada_Male"))
        assert(result.contains("23,Larry_USA_Male"))
    }

    test("should merge the given columns with space when no separator is passed") {
        val data = Array(
            "John,Male,21,Canada",
            "Smith, Male, 30, UK",
            "Larry, Male, 23, USA",
            "Fiona, Female,18,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.mergeColumns(List(0, 3, 1)).collect()

        assert(result.length == 4)
        assert(result.contains("21,John Canada Male"))
        assert(result.contains("23,Larry USA Male"))
    }

    test("should merge the given columns by keeping the original columns") {
        val data = Array(
            "John,Male,21,Canada",
            "Smith, Male, 30, UK",
            "Larry, Male, 23, USA",
            "Fiona, Female,18,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.mergeColumns(List(0, 3, 1), "_", retainColumns = true).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,Canada,John_Canada_Male"))
        assert(result.contains("Larry,Male,23,USA,Larry_USA_Male"))
    }

    test("should split the specified column value according to the given lengths by removing original columns") {
        val data = Array(
            "John,Male,21,+914382313832,Canada",
            "Smith, Male, 30,+015314343462, UK",
            "Larry, Male, 23,+009815432975, USA",
            "Fiona, Female,18,+891015709854,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.splitColumnByLength(3, List(3, 10)).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,Canada,+91,4382313832"))
        assert(result.contains("Smith,Male,30,UK,+01,5314343462"))
    }
}
