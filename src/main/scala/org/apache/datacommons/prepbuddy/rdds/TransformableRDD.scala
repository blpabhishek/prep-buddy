package org.apache.datacommons.prepbuddy.rdds

import java.security.MessageDigest

import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

    def dropColumn(columnIndex: Int): TransformableRDD = {
        val transformed: RDD[String] = this.map((record: String) => {
            val recordInBuffer: mutable.Buffer[String] = fileType.parseRecord(record).toBuffer
            recordInBuffer.remove(columnIndex)
            fileType.join(recordInBuffer.toArray)
        })
        new TransformableRDD(transformed, fileType)
    }



  def deduplicate(): TransformableRDD = {

    val mappedRDD: RDD[(Long, String)] = this.map((record) => {
      val columns: Array[String] = this.fileType.parseRecord(record)
      val fingerprint = generateFingerPrint(columns)
      (fingerprint, record)
    })

    val reducedRDD: RDD[(Long, String)] = mappedRDD.reduceByKey((accumulator, record) => {
      record
    })
    new TransformableRDD(reducedRDD.values, fileType)
  }

  private def generateFingerPrint(columns: Array[String]): Long = {
    val concatenatedString: String = columns.mkString("")
    val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
    algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
    BigInt(algorithm.digest()).longValue()
  }


  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    parent.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions

    override protected def getPartitions: Array[Partition] = parent.partitions
}

