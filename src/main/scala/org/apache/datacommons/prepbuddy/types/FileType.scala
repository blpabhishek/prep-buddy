package org.apache.datacommons.prepbuddy.types

import org.apache.datacommons.prepbuddy.utils.RowRecord

/**
  * File formats that are supported by TransformableRDD
  */
class FileType(delimiter: String) extends Serializable {
    def appendDelimiter(row: String): String = row + delimiter

    def join(record: RowRecord): String = record.mkString(delimiter)

    def parse(record: String): RowRecord = new RowRecord(record.split(delimiter, -1).map(_.trim))
}

object CSV extends FileType(",")

object TSV extends FileType("\t")

