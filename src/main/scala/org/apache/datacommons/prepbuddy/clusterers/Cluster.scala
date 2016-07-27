package org.apache.datacommons.prepbuddy.clusterers

import scala.collection.mutable.ListBuffer

/**
  * Cluster contains groups of values by their specified key
  */
class Cluster(key: String) extends Serializable {

    private val tuples: ListBuffer[(String, Int)] = ListBuffer.empty

    def contain(tuple: (String, Int)): Boolean = tuples.contains(tuple)

    def size: Int = tuples.size

    def isOfKey(key: String): Boolean = key.equals(this.key)

    def containsValue(value: String): Boolean = tuples.exists(_._1 == value)

    def add(tuple: (String, Int)): Unit = tuples += tuple

}
