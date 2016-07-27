package org.apache.datacommons.prepbuddy.utils

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class PivotTableTest extends SparkTestCase {
    test("pivotByCount should give pivoted table of given row and column indexes") {
        val pivotTable: PivotTable[Int] = new PivotTable[Int](0)
        pivotTable.addEntry("row", "column1", 5)

        val expected: Int = 5
        val actual: Int = pivotTable.valueAt("row", "column1")
        assert(expected == actual)

        assert(0 == pivotTable.valueAt("row", "column"))

    }

    test("pivotTable can be transformed") {
        val pivotTable: PivotTable[Int] = new PivotTable[Int](0)
        pivotTable.addEntry("row", "column1", 5)
        pivotTable.addEntry("row", "column2", 6)


        val transformed: PivotTable[Double] = pivotTable.transform((value: Any) => {
            0.9
        }, 10.0).asInstanceOf[PivotTable[Double]]


        val value: Any = transformed.valueAt("row", "column1")
        assert(0.9 == value)

        assert(10.0 == transformed.valueAt("row", "column"))
    }

    test("pivotByCount should give pivoted table of given column") {
        val dataSet = Array("known,new,long,home,skips",
            "unknown,new,short,work,reads",
            "unknown,follow Up,long,work,skips",
            "known,follow Up,long,home,skips",
            "known,new,short,home,reads",
            "known,follow Up,long,work,skips",
            "unknown,follow Up,short,work,skips",
            "unknown,new,short,work,reads",
            "known,follow Up,long,home,skips",
            "known,new,long,work,skips",
            "unknown,follow Up,short,home,skips",
            "known,new,long,work,skips",
            "known,follow Up,short,home,reads",
            "known,new,short,work,reads",
            "known,new,short,home,reads",
            "known,follow Up,short,work,reads",
            "known,new,short,home,reads",
            "unknown,new,short,work,reads")
        val initialDataSet: RDD[String] = sparkContext.parallelize(dataSet)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataSet)
        val pivotTable: PivotTable[Integer] = initialRDD.pivotByCount(4, Seq[Int](0, 1, 2, 3))

        val expected: Int = 7
        val actual: Integer = pivotTable.valueAt("skips", "long")
        assert(expected == actual)

        assert(0 == pivotTable.valueAt("reads", "long"))
    }
}
