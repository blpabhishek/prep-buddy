package org.apache.datacommons.prepbuddy.examples

import org.apache.datacommons.prepbuddy.smoothers.SimpleMovingAverageMethod
import org.apache.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SmootherMain {
    def main(args: Array[String]) {
        if (args.length != 2) {
            System.out.println("--> FilePath with Assertion Result is Need To Be Specified")
            System.exit(0)
        }
        val conf: SparkConf = new SparkConf().setAppName("Smoother Main")
        val sc: SparkContext = new SparkContext(conf)
        val filePath: String = args(0)
        val expectedResultPath: String = args(1)

        val csvInput: RDD[String] = sc.textFile(filePath, 1)
        val movingAverage: SimpleMovingAverageMethod = new SimpleMovingAverageMethod(3)
        val smooth: Array[Double] = movingAverage.smooth(csvInput.map(CSV.parse(_).select(3))).collect()
        println("Smoother Count" + smooth.length)

        println("=========================================================")
        val expected: Array[Double] = sc.textFile(expectedResultPath).map(_.toDouble).collect()
        if (smooth sameElements expected) println("Assertion Successful") else println("Assertion Failed")
        sc.stop()
    }
}
