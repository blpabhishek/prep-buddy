package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD

trait NormalizationStrategy extends Serializable {
    def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit

    def normalize(rawValue: String): String
}
