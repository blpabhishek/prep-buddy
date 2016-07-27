package org.apache.datacommons.prepbuddy.clusterers

import scala.collection.mutable

/**
  * This algorithm generates a key using N Gram Fingerprint Algorithm for
  * every cardinal value (facet) in column and add them to the Cluster.
  */
class NGramFingerprintAlgorithm(nGram: Int) extends FingerprintAlgorithm {
    def getClusters(tuples: Array[(String, Int)]): Clusters = super.getClusters(tuples, generateNGramFingerprint)

    def generateNGramFingerprint(value: String): String = {
        val someString: String = removeAllPunctuations(value.trim.toLowerCase)
        val set: mutable.TreeSet[String] = getNGramSetOf(someString)
        set.mkString("")
    }

    private def getNGramSetOf(someString: String): mutable.TreeSet[String] = {
        val set: mutable.TreeSet[String] = new mutable.TreeSet[String]()
        val charArray: Array[Char] = someString.toCharArray
        var index = 0
        while (index + nGram <= charArray.length) {
            set.add(new String(charArray, index, nGram))
            index += 1
        }
        set
    }
}
