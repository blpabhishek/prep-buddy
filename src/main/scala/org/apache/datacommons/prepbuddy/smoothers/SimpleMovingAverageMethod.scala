package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.rdd.RDD

/**
  * A smoothing method which smooths data based on Simple Moving Average which is the unweighted mean of
  * the previous n data. This method ensure that variations in the mean are aligned
  * with the variations in the data rather than being shifted in time.
  */
class SimpleMovingAverageMethod(windowSize: Int) extends SmoothingMethod {
    override def smooth(singleColumnDataset: RDD[String]): RDD[Double] = {
        val duplicateRDD: RDD[Double] = prepare(singleColumnDataset, windowSize)
        duplicateRDD.mapPartitions(_.sliding(windowSize).map(average))
    }

    private def average(windowValues: Seq[Double]) = windowValues.sum / windowSize

}
