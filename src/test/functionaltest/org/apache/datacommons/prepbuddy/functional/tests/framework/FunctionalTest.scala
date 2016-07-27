package org.apache.datacommons.prepbuddy.functional.tests.framework

import java.io.{File, FileReader}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class FunctionalTest extends App {
    private val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName)
    protected val sc: SparkContext = new SparkContext(sparkConf)
    protected val datasetPath = getDatasetPath

    private var testNames: ListBuffer[String] = ListBuffer.empty
    private val testReport = new TestReport

    private def getDatasetPath: String = {
        val configFile: File = new File("testConfig.properties")
        val reader: FileReader = new FileReader(configFile)
        val props: Properties = new Properties()
        props.load(reader)
        props.getProperty("testDataSet")
    }

    def shutDown(): Unit = sc.stop()

    def test(testName: String)(testFunction: => Unit) {
        validateTestEnvironment(testName)
        testNames += testName
        val testResult: TestResult = runTest(testName, testFunction)
        testReport.add(testResult)
    }

    def runTest(testName: String, testFunction: => Unit): TestResult = {
        val exceptionalTestFun: () => Any = testFunction _
        val testResult: TestResult = new TestResult(testName)
        try {
            exceptionalTestFun()
            testResult.markAsSuccess()
        } catch {
            case err: AssertionError => testResult.markAsFailure(err)
        }
        testResult
    }

    def validateTestEnvironment(testName: String) {
        if (testNames.contains(testName)) {
            throw new DuplicateTestNameException("Duplicate test name: " + testName)
        }
    }

    def printReport() {
        testReport.show()
        shutDown()
        if (testReport.hasFailingTest) {
            throw new AssertionFailedException("Test is failing because of assertion failure")
        }
    }
}