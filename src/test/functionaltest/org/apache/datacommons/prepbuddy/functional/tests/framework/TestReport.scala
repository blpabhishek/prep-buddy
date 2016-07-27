package org.apache.datacommons.prepbuddy.functional.tests.framework

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TestReport {
    private val results: ListBuffer[TestResult] = mutable.ListBuffer.empty

    def add(testResults: TestResult): Unit = results += testResults

    def show(): Unit = {
        println("=====================================================================================================")
        results.foreach(result => println(result.getOutcome + "\r\n"))
        val failedCount: Int = results.count(_.isFailed)
        val passedCount: Int = results.size - failedCount
        println("-> " + passedCount + " Test Passed and " + failedCount + " Test has been Failed.")
        println("=====================================================================================================")
    }

    def hasFailingTest: Boolean = results.exists(_.isFailed)
}
