package org.apache.datacommons.prepbuddy.functional.tests.framework

class DuplicateTestNameException(errorMessage: String) extends Throwable {
    override def getMessage: String = errorMessage
}
