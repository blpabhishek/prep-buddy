package org.apache.datacommons.prepbuddy.clusterers

import java.util.regex.Pattern

abstract class FingerprintAlgorithm extends ClusteringAlgorithm {
    private val PunctuationMatcher: Pattern = Pattern.compile("\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]")

    def removeAllPunctuations(someString: String): String = PunctuationMatcher.matcher(someString).replaceAll("")

    protected def getClusters(tuples: Array[(String, Int)], fingerPrintFunction: (String) => String): Clusters = {
        val clusters: Clusters = new Clusters
        tuples.foreach((tuple) => {
            val key: String = fingerPrintFunction(tuple._1)
            clusters.add(key, tuple)
        })
        clusters
    }
}
