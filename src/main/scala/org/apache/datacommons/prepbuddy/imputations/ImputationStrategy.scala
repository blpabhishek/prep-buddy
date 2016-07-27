package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * A contract for an imputation strategy
  */
trait ImputationStrategy extends Serializable {
    def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int)

    def handleMissingData(record: RowRecord): String
}
