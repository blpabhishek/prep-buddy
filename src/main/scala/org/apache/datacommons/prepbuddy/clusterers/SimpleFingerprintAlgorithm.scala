package org.apache.datacommons.prepbuddy.clusterers

import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * This algorithm generates a key using Simple Fingerprint Algorithm for
  * every cardinal value (facet) in column and add them to the Cluster.
  */
class SimpleFingerprintAlgorithm extends FingerprintAlgorithm with Serializable {

    def getClusters(tuples: Array[(String, Int)]): Clusters = super.getClusters(tuples, generateSimpleFingerprint)

    def generateSimpleFingerprint(value: String): String = {
        val fragments: Array[String] = StringUtils.split(removeAllPunctuations(value.trim.toLowerCase))
        rearrangeAlphabetically(fragments)
    }

    def rearrangeAlphabetically(fragments: Array[String]): String = {
        val set: mutable.TreeSet[String] = new mutable.TreeSet()
        set ++= fragments
        set.mkString(" ")
    }
}
