package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

abstract class AbstractRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {
    val DEFAULT_SAMPLE_SIZE: Int = 1000
    protected val sampleRecords = takeSample(withReplacement = false, num = DEFAULT_SAMPLE_SIZE)
    protected val columnLength = getNumberOfColumns

    private def getNumberOfColumns: Int = {
        val columnLengthAndCount: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()
        sampleRecords.foreach((record) => {
            val columnCount: Int = fileType.parse(record).length
            if (columnLengthAndCount.contains(columnCount)) {
                val count: Int = columnLengthAndCount.apply(columnCount)
                columnLengthAndCount.put(columnCount, count + 1)
            }
            else {
                columnLengthAndCount.put(columnCount, 1)
            }
        })
        getHighestCountKey(columnLengthAndCount)
    }

    private def getHighestCountKey(lengthWithCount: mutable.HashMap[Int, Int]): Int = {
        lengthWithCount.reduce((first, second) => {
            if (first._2 > second._2) first else second
        })._1
    }
}
