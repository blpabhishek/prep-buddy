package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * ModeSubstitution is a simplest imputation strategy used for filling missing values
  * in a data set.It imputes the missing values by most occurring values in data set.
  */
class ModeSubstitution() extends ImputationStrategy {
    private var mode: (String, Int) = null

    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val listFacets: TextFacets = rdd.listFacets(missingDataColumn)
        mode = listFacets.highest.head
    }

    override def handleMissingData(record: RowRecord): String = mode._1.toString
}
