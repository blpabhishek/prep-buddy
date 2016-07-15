package org.apache.datacommons.prepbuddy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkTestCase extends FunSuite with BeforeAndAfterAll {
    var sparkContext: SparkContext = null

    override def beforeAll() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        sparkContext = new SparkContext(sparkConf)
    }

    override def afterAll() {
        sparkContext.stop()
    }
}
