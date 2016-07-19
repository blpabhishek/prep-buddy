package specs

import framework.{AssertionFailedException, DuplicateTestNameException, TestReport, TestResult}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class FunctionalTestRunner extends App {
    private val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName)
    protected val sc: SparkContext = new SparkContext(sparkConf)

    private var testNames: ListBuffer[String] = ListBuffer.empty
    private val testReport = new TestReport

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