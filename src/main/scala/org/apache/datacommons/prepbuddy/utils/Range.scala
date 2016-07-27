package org.apache.datacommons.prepbuddy.utils

class Range(lowerBound: Double = 0, upperBound: Double = 0) {
    def this(lowerBound: Int, upperBound: Int) = this(lowerBound.toDouble, upperBound.toDouble)

    def contains(aValue: Double): Boolean = {
        aValue >= lowerBound && aValue <= upperBound
    }
}

