package org.apache.datacommons.prepbuddy.functional.tests.framework

class AssertionFailedException(errorMessage: String) extends Throwable {
    override def getMessage: String = errorMessage
}
