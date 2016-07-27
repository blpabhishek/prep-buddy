package org.apache.datacommons.prepbuddy.examples

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FacetsMain {
    def main(args: Array[String]) {
        if (args.length == 0) {
            System.out.println("--> File Path Need To Be Specified")
            System.exit(0)
        }
        val conf: SparkConf = new SparkConf().setAppName("Facets")
        val sc: SparkContext = new SparkContext(conf)
        val filePath: String = args(0)
        val csvInput: RDD[String] = sc.textFile(filePath, args(1).toInt)
        val inputRdd: TransformableRDD = new TransformableRDD(csvInput)
        val textFacets: TextFacets = inputRdd.listFacets(1)
        val count: Long = textFacets.count
        System.out.println("-->>> Total " + count)
        sc.stop()
    }
}

