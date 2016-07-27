package org.apache.datacommons.prepbuddy.rdds

import org.apache.spark.sql.DataFrame

class AnalyzableDF(dataFrame: DataFrame) extends DataFrame(dataFrame.sqlContext, dataFrame.queryExecution.logical) {
    def percentageOfMissingValue(columnName: String): Double = {
        val totalRecordCount: Double = count
        val missingRecordCount: Double = select(columnName).filter(columnName + " is null").count
        (missingRecordCount / totalRecordCount) * 100
    }
}