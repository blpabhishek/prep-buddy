package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD

/**
  * A normalizer which scales the data within the specified range.
  * Default range is (0,1)
  * A' = (A - min(A)) / (max(A) - min(A)) * (D-C) + C
  * where (C,D) is the range and A is the value.
  */
class MinMaxNormalizer(minRange: Int = 0, maxRange: Int = 1) extends NormalizationStrategy {

    private var maxValue: Double = 0
    private var minValue: Double = 0

    override def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit = {
        val doubleRDD = transformableRDD.toDoubleRDD(columnIndex)
        maxValue = doubleRDD.max
        minValue = doubleRDD.min
    }

    override def normalize(rawValue: String): String = {
        val normalizedValue: Double = (rawValue.toDouble - minValue) / (maxValue - minValue)
        val normalizedValueInRange = normalizedValue * (maxRange - minRange) + minRange
        normalizedValueInRange.toString
    }
}
