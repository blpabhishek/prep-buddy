package org.apache.datacommons.prepbuddy.utils

import scala.collection.immutable.HashMap

class NumberMap() {
    private var numbers: Map[String, Double] = new HashMap[String, Double]()

    def put(key: String, value: Double): Unit = {
        numbers += (key -> value)
    }

    def keyWithHighestValue: String = {
        var highestTuple = ("", 0.0)
        numbers.foreach((tuple) => {
            val value: Double = tuple._2
            if (value > highestTuple._2) highestTuple = tuple
        })
        highestTuple._1
    }
}
