package org.apache.datacommons.prepbuddy.rdds

import java.lang.Double._
import java.security.MessageDigest

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.Buffer

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filterFunction = (record: String) => {
            val rowRecord = new RowRecord(fileType.parseRecord(record))
            !predicate(rowRecord)
        }
        val filteredRDD = this.filter(filterFunction)
        new TransformableRDD(filteredRDD, this.fileType)
    }

    def impute(columnIndex: Int, strategy: ImputationStrategy): TransformableRDD = {
        strategy.prepareSubstitute(this, columnIndex)
        val transformed: RDD[String] = map((record) => {
            val columns: Array[String] = fileType.parseRecord(record)
            val value: String = columns(columnIndex)
            var replacementValue: String = value
            if (value.equals(null) || value.trim.isEmpty) {
                replacementValue = strategy.handleMissingData(new RowRecord(columns))
            }

            columns(columnIndex) = replacementValue
            fileType.join(columns)
        })

        new TransformableRDD(transformed, fileType)
    }

    def dropColumn(columnIndex: Int): TransformableRDD = {
        val transformed: RDD[String] = map((record: String) => {
            val recordInBuffer: Buffer[String] = fileType.parseRecord(record).toBuffer
            recordInBuffer.remove(columnIndex)
            fileType.join(recordInBuffer.toArray)
        })
        new TransformableRDD(transformed, fileType)
    }


    def deduplicate(columnIndexes: List[Int]): TransformableRDD = {
        val fingerprintedRDD: RDD[(Long, String)] = map((record) => {
            val columnsAsArray: Array[String] = fileType.parseRecord(record)
            val primaryKeys: Array[String] = getPrimaryKeyValues(columnIndexes, columnsAsArray)
            val fingerprint = generateFingerPrint(primaryKeys)
            (fingerprint, record)
        })
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    def deduplicate(): TransformableRDD = {
        deduplicate(List.empty)
    }

    private def getPrimaryKeyValues(columnIndexes: List[Int], columnValues: Array[String]): Array[String] = {
        if (columnIndexes.isEmpty)
            return columnValues

        var primaryKeys: List[String] = List()
        for (columnIndex <- columnIndexes)
            primaryKeys = primaryKeys.:+(columnValues(columnIndex))
        primaryKeys.toArray
    }

    def listFacets(columnIndex: Int): TextFacets = {
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val columns: Array[String] = fileType.parseRecord(record)
            (columns(columnIndex), 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey((accumulator, record) => {
            accumulator + record
        })
        new TextFacets(facets)
    }

    private def generateFingerPrint(columns: Array[String]): Long = {
        val concatenatedString: String = columns.mkString("")
        val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
        algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
        BigInt(algorithm.digest()).longValue()
    }

    def toDoubleRDD(columnIndex: Int): RDD[Double] = {
        val filtered: RDD[String] = this.filter((record: String) => {
            val rowRecord: Array[String] = fileType.parseRecord(record)
            val value: String = rowRecord.apply(columnIndex)
            val numberMatcher: String = "[+-]?\\d+.?\\d+"
            !value.trim.isEmpty || value.matches(numberMatcher) || value == null
        })

        filtered.map((record) => {
            val recordAsArray: Array[String] = fileType.parseRecord(record)
            val value: String = recordAsArray(columnIndex)
            parseDouble(value)
        })
    }

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}

