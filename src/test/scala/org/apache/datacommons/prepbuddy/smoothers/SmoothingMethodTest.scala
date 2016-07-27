package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.spark.rdd.RDD

class SmoothingMethodTest extends SparkTestCase {
    test("should be able to duplicate data of given window size to previous partition") {
        val data = Array("1", "2", "3")
        val dataSet: RDD[String] = sparkContext.parallelize(data, 3)
        val method: SmoothingMethod = new SmoothingMethod {
            override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = ???
        }
        val doubleRdd: RDD[Double] = method.prepare(dataSet, 3)
        val collected: Array[Double] = doubleRdd.collect()
        assert(5 == collected.length)
    }

    test("should be able to smooth by simple moving average method") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array(
            "3", "4", "5",
            "6", "7", "8",
            "9", "10", "11",
            "12", "13", "14"), 3)
        val movingAverage: SimpleMovingAverageMethod = new SimpleMovingAverageMethod(3)
        val rdd: RDD[Double] = movingAverage.smooth(initialDataset)

        val averages: Array[Double] = rdd.collect()
        assert(averages(0) == 4.0)
        assert(averages(1) == 5.0)
        assert(averages(2) == 6.0)
        assert(averages(3) == 7.0)
        assert(averages(4) == 8.0)
        assert(averages(5) == 9.0)
        assert(averages(6) == 10.0)
        assert(averages(7) == 11.0)
        assert(averages(8) == 12.0)
        assert(averages(9) == 13.0)

        assert(!averages.contains(14.0))
    }

    test("should smooth the values by weighted moving average method") {
        val initialDataset: RDD[String] = sparkContext.parallelize(Array(
            "10", "12", "16", "13",
            "17", "19", "15", "20",
            "22", "19", "21", "19"), 3)

        val weights: Weights = new Weights(3)
        weights.add(0.166)
        weights.add(0.333)
        weights.add(0.5)

        val movingAverage: WeightedMovingAverageMethod = new WeightedMovingAverageMethod(3, weights)
        val rdd: RDD[Double] = movingAverage.smooth(initialDataset)
        val collect: Array[Double] = rdd.collect()
        val collected: Array[Double] = rdd.collect().map("%1.2f".format(_).toDouble)

        assert(collected(0) == 13.66)
        assert(collected(1) == 13.82)
        assert(collected(2) == 15.49)
        assert(collected(3) == 17.32)

    }
}
