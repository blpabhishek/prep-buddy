package org.apache.datacommons.prepbuddy.exceptions

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.{SimpleMovingAverageMethod, WeightedMovingAverageMethod, Weights}
import org.apache.datacommons.prepbuddy.utils.Probability
import org.apache.spark.rdd.RDD

class ExceptionTest extends SparkTestCase {
    test("Weighted moving average should not be creatable when weights and window size is not equal") {
        val weights: Weights = new Weights(3)
        val thrown = intercept[ApplicationException] {
            new WeightedMovingAverageMethod(1, weights)
        }
        assert(thrown.getMessage == "Window size and weighs size should be same.")
    }

    test("weights add should throw exception when sum of weights is not equal to one") {
        val weights: Weights = new Weights(3)
        weights.add(0.166)
        weights.add(0.333)
        weights.add(0.5)

        val otherWeights: Weights = new Weights(2)
        otherWeights.add(0.777)
        val thrown = intercept[ApplicationException] {
            otherWeights.add(0.333)
        }
        assert(thrown.getMessage == "To calculate weighted moving average weights sum should be up to one.")
    }

    test("weights should throw exception if size is exceeded") {
        val weights: Weights = new Weights(2)
        weights.add(0.5)
        weights.add(0.5)
        val thrown = intercept[ApplicationException] {
            weights.add(0.3)
        }
        assert(thrown.getMessage == "Can not add value more than size limit.")
    }

    test("Probability should not create new object if probability value is not valid") {
        val thrown = intercept[ApplicationException] {
            new Probability(1.1)
        }
        assert(thrown.getMessage == "Probability can not be less than zero or greater than 1")

        val otherThrown = intercept[ApplicationException] {
            new Probability(-1)
        }
        assert(otherThrown.getMessage == "Probability can not be less than zero or greater than 1")
    }

    test("smooth should throw exception if given column is not numeric") {
        val data = Array(
            "one two, three",
            "two one, four"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(data)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val thrown = intercept[ApplicationException] {
            initialRDD.smooth(0, new SimpleMovingAverageMethod(2))
        }
        assert(thrown.getMessage == "Values of column are not numeric")
    }

    test("toDoubleRDD should throw exception if given column is not numeric") {
        val data = Array(
            "one two, three",
            "two one, four"
        )
        val initialDataset: RDD[String] = sparkContext.parallelize(data)
        val initialRDD: TransformableRDD = new TransformableRDD(initialDataset)
        val thrown = intercept[ApplicationException] {
            initialRDD.toDoubleRDD(0)
        }
        assert(thrown.getMessage == "Values of column are not numeric")
    }
}
