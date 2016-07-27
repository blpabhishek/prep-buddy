package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD

/**
  * A normalizer technique which normalizes data by their standard score.
  * Formula for Z Score Normalization : (X - Mean) / Standard Deviation.
  */
class ZScoreNormalizer extends NormalizationStrategy {

    private var standardDeviation: Double = 0

    private var mean: Double = 0

    override def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit = {
        val doubleRDD = transformableRDD.toDoubleRDD(columnIndex)
        standardDeviation = doubleRDD.stdev
        mean = doubleRDD.mean
    }

    override def normalize(rawValue: String): String = {
        String.valueOf((rawValue.toDouble - mean) / standardDeviation)
    }
}
