package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.DFTestCase
import org.apache.spark.sql.{DataFrame, SQLContext}

class AnalyzableDFTest extends DFTestCase {

    //    object CallRecordSchema {
    //        private val user = StructField("User", LongType)
    //        private val other = StructField("Other", LongType)
    //        private val direction = StructField("Direction", StringType)
    //        private val duration = StructField("Duration", IntegerType)
    //        private val timestamp = StructField("Timestamp", StringType)
    //
    //        def getSchema: StructType = StructType(Array(user, other, direction, duration, timestamp))
    //    }

    test("should determine the extent of missing value in the column") {
        val sqlContext = new SQLContext(sparkContext)
        import sqlContext.implicits._
        val callRecords: DataFrame = Seq[(Long, Long, String, Integer, String)](
            (7434677419L, 7641036117L, "Incoming", null, "Wed Sep 15 19:17:44 +0100 2010"),
            (7641036117L, 1666472054L, "Outgoing", 0, "Mon Feb 11 07:18:23 +0000 1980"),
            (7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326239L, "Missed", null, "Mon Feb 11 08:04:42 +0000 1980"),
            (7641036117L, 7371326239L, "Incoming", 4, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7681546436L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            (7641036117L, 7371326239L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7681546437L, "Incoming", null, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7681546437L, "Outgoing", null, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7681546431L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            (7641036117L, 7371326235L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326236L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7681546437L, "Missed", 12, "Mon Feb 11 08:04:42 +0000 1980"),
            (7641036117L, 7371326238L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980"),
            (7641036117L, 7371326230L, "Incoming", 45, "Mon Feb 11 07:45:42 +0000 1980)")
        ).toDF("User", "Other", "Direction", "Duration", "Timestamp")

        val analyzableCallRecords: AnalyzableDF = new AnalyzableDF(callRecords)

        assert(20.0 == analyzableCallRecords.percentageOfMissingValue("Duration"))
    }
}
