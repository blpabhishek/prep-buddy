package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD

class MinMaxNormalizer(minRange:Int = 0,maxRange:Int=1) extends NormalizationStrategy{

    var maxValue :Double = 0
    var minValue :Double = 0
    override def prepare(transformableRDD: TransformableRDD, columnIndex: Int) :Unit={
        val doubleRDD = transformableRDD.toDoubleRDD(columnIndex)
        maxValue = doubleRDD.max
        minValue = doubleRDD.min
    }

    override def normalize(rawValue: String) :String= {
        val normalizedValue = (rawValue.toDouble - minValue) / (maxValue - minValue) * (maxRange - minRange) + minRange
        String.valueOf(normalizedValue)
    }
}
