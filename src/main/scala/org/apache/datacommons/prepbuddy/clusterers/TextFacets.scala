package org.apache.datacommons.prepbuddy.clusterers

import org.apache.spark.rdd.RDD

class TextFacets(facets: RDD[(String, Int)]) {
    private val tuples: Array[(String, Int)] = facets.collect()

    def getFacetsBetween(lowerBound: Int, upperBound: Int): Array[(String, Int)] = {
        val tuplesBetween: Array[(String, Int)] = tuples.filter((tuple) => {
            val currentTuple = tuple._2
            isInRange(currentTuple, lowerBound, upperBound)
        })
        tuplesBetween
    }

    def lowest: List[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => {
            currentTuple < peakTuple
        })
    }

    def highest: List[(String, Int)] = {
        getPeakListFor((currentTuple, peakTuple) => {
            currentTuple > peakTuple
        })
    }

    def getPeakListFor(compareFunction: (Int, Int) => Boolean): List[(String, Int)] = {
        var list: List[(String, Int)] = List()
        var peakTuple: (String, Int) = tuples(0)
        list = list.:+(peakTuple)
        for (tuple <- tuples) {
            if (compareFunction(tuple._2, peakTuple._2)) {
                peakTuple = tuple
                list = list.drop(list.size)
                list = list.:+(peakTuple)
            }
            if ((tuple._2 == peakTuple._2) && !(tuple == peakTuple)) {
                list = list.:+(tuple)
            }
        }
        list
    }

    private def isInRange(currentTupleValue: Integer, minimum: Int, maximum: Int): Boolean = {
        currentTupleValue >= minimum && currentTupleValue <= maximum
    }

    def count: Long = facets.count
}
