package org.apache.datacommons.prepbuddy.transformers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.types.{CSV, TSV}
import org.apache.spark.rdd.RDD

class MergeSplitTest extends SparkTestCase {
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

        val result: Array[String] = transformableRDD.splitByFieldLength(3, List(3, 10)).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,Canada,+91,4382313832"))
        assert(result.contains("Smith,Male,30,UK,+01,5314343462"))
    }

    test("should not throw exception while splitting column by lengths when total lengths exceed the string") {
        val data = Array(
            "John,Male,21,+914382313832,Canada",
            "Smith, Male, 30,+015314343462, UK",
            "Larry, Male, 23,+009815432975, USA",
            "Fiona, Female,18,+891015709854,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.splitByFieldLength(3, List(3, 8, 4, 5)).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,Canada,+91,43823138,32,"))
        assert(result.contains("Smith,Male,30,UK,+01,53143434,62,"))
    }

    test("should split the specified column value according to the given lengths by keeping original columns") {
        val data = Array(
            "John,Male,21,+914382313832,Canada",
            "Smith, Male, 30,+015314343462, UK",
            "Larry, Male, 23,+009815432975, USA",
            "Fiona, Female,18,+891015709854,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.splitByFieldLength(3, List(3, 10), retainColumn = true).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,+914382313832,Canada,+91,4382313832"))
        assert(result.contains("Smith,Male,30,+015314343462,UK,+01,5314343462"))
    }

    test("should split the specified column value by delimiter while keeping the original column") {
        val data = Array(
            "John,Male,21,+91-4382313832,Canada",
            "Smith, Male, 30,+01-5314343462, UK",
            "Larry, Male, 23,+00-9815432975, USA",
            "Fiona, Female,18,+89-1015709854,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.splitByDelimiter(3, "-", retainColumn = true).collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,+91-4382313832,Canada,+91,4382313832"))
        assert(result.contains("Smith,Male,30,+01-5314343462,UK,+01,5314343462"))
    }

    test("should split the specified column value by delimiter while removing the original column") {
        val data = Array(
            "John,Male,21,+91-4382313832,Canada",
            "Smith, Male, 30,+01-5314343462, UK",
            "Larry, Male, 23,+00-9815432975, USA",
            "Fiona, Female,18,+89-1015709854,USA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, CSV)

        val result: Array[String] = transformableRDD.splitByDelimiter(3, "-").collect()

        assert(result.length == 4)
        assert(result.contains("John,Male,21,Canada,+91,4382313832"))
        assert(result.contains("Smith,Male,30,UK,+01,5314343462"))
    }

    test("should split the given column by delimiter into given number of split") {
        val data = Array(
            "John\tMale\t21\t+91-4382-313832\tCanada",
            "Smith\tMale\t30\t+01-5314-343462\tUK",
            "Larry\tMale\t23\t+00-9815-432975\tUSA",
            "Fiona\tFemale\t18\t+89-1015-709854\tUSA"
        )
        val dataset: RDD[String] = sparkContext.parallelize(data)
        val transformableRDD: TransformableRDD = new TransformableRDD(dataset, TSV)

        val result: Array[String] = transformableRDD.splitByDelimiter(3, "-", maxSplit = 2).collect()

        assert(result.length == 4)
        assert(result.contains("John\tMale\t21\tCanada\t+91\t4382-313832"))
        assert(result.contains("Smith\tMale\t30\tUK\t+01\t5314-343462"))
    }
}
