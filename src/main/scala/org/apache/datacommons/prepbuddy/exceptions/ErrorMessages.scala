package org.apache.datacommons.prepbuddy.exceptions

object ErrorMessages {
    val COLUMN_VALUES_ARE_NOT_NUMERIC: ErrorMessage = {
        val message: String = "Values of column are not numeric"
        val key: String = "COLUMN_VALUES_ARE_NOT_NUMERIC"
        new ErrorMessage(key, message)
    }

    val WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE: ErrorMessage = {
        val message: String = "To calculate weighted moving average weights sum should be up to one."
        val key: String = "WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE"
        new ErrorMessage(key, message)
    }
    val WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING: ErrorMessage = {
        val message: String = "Window size and weighs size should be same."
        val key: String = "WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING"
        new ErrorMessage(key, message)
    }
    val SIZE_LIMIT_IS_EXCEEDED: ErrorMessage = {
        val key: String = "SIZE_LIMIT_IS_EXCEEDED"
        val message: String = "Can not add value more than size limit."
        new ErrorMessage(key, message)
    }

    val COLUMN_INDEX_OUT_OF_BOUND: ErrorMessage = {
        val key: String = "COLUMN_INDEX_OUT_OF_BOUND"
        val message: String = "Column index is out of bound."
        new ErrorMessage(key, message)
    }
    val NEGATIVE_COLUMN_INDEX: ErrorMessage = {
        val key: String = "NEGATIVE_COLUMN_INDEX"
        val message: String = "Column index can not be negative."
        new ErrorMessage(key, message)
    }
    val PROBABILITY_IS_NOT_IN_RANGE: ErrorMessage = {
        val key: String = "PROBABILITY_IS_NOT_IN_RANGE"
        val message: String = "Probability can not be less than zero or greater than 1"
        new ErrorMessage(key, message)
    }
}
