package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.Partitioner

/**
  * A Patitioner that puts each row in a partition that we specify by key.
  */
class KeyPartitioner(numOfPartitions: Int) extends Partitioner{
    override def numPartitions: Int = numOfPartitions

    override def getPartition(key: Any): Int = key.toString.toInt
}
