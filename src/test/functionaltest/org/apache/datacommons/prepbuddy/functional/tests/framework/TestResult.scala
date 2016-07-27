package org.apache.datacommons.prepbuddy.functional.tests.framework

class TestResult(testName: String) {
    private var message: String = null
    private var cause: AssertionError = null
    private var passed: Boolean = false

    def markAsSuccess() {
        message = "PASSED => ".concat(testName)
        passed = true
    }

    def markAsFailure(error: AssertionError) {
        cause = error
        message = getFailureMessage(error)
        passed = false
    }

    private def getFailureMessage(error: AssertionError): String = {
        "FAILED => " + testName + "\r\n" +
            "\t" + getCause.toUpperCase + "\n" +
            "\t\t" + error.getStackTrace.mkString("\r\n\t\t")
    }

    def getCause: String = cause.getMessage

    def getOutcome: String = message

    def isFailed: Boolean = !passed
}
