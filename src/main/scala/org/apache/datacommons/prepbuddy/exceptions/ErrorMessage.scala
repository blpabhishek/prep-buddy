package org.apache.datacommons.prepbuddy.exceptions

class ErrorMessage(key: String, msg: String) {
    def getMessage: String = msg
}
