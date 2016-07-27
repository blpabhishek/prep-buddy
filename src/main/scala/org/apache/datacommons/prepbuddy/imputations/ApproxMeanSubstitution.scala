package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * An imputation strategy that imputes the missing values by an approx mean of the values in data set.
  * This implementation is only for imputing numeric columns.
  * Recommended when imputing on large data set.
  */
class ApproxMeanSubstitution() extends ImputationStrategy {
    private var approxMean: Double = 0

    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val timeOut: Int = 20000
        approxMean = rdd.toDoubleRDD(missingDataColumn).meanApprox(timeOut).getFinalValue().mean
    }

    override def handleMissingData(record: RowRecord): String = approxMean.toString
}
