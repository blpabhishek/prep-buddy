package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}

/**
  * Contains weights in sequence for the weighted sliding window.
  */
class Weights(limit: Int) extends Serializable {
    private var weights: List[Double] = List()

    def multiplyWith(list: List[Double]): List[Double] = {
        weights.zip(list).map((tuple) => tuple._1 * tuple._2)
    }

    def get(index: Int): Double = weights(index)

    private def sumWith(value: Double): Double = "%1.1f".format(weights.sum + value).toDouble

    def add(value: Double): Unit = {
        if (size == limit) throw new ApplicationException(ErrorMessages.SIZE_LIMIT_IS_EXCEEDED)
        if(size == limit - 1 && sumWith(value) != 1.0 ){
            throw new ApplicationException(ErrorMessages.WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE)
        }
        weights = weights :+ value
    }

    def size : Int = weights.length
}
