package org.apache.datacommons.prepbuddy.api.java

import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.types.FileType
import org.apache.spark.api.java.JavaRDD

class JavaTransformableRDD(rdd: JavaRDD[String], fileType: FileType) extends JavaRDD[String](rdd.rdd) {
    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd)

    def deduplicate: JavaTransformableRDD = new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD(), fileType)

    def impute(columnIndex: Int, imputationStrategy: ImputationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, imputationStrategy), fileType)
    }
}
