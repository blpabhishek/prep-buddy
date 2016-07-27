package org.apache.datacommons.prepbuddy.utils

import org.apache.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}

class Probability(probabilityValue: Double) extends Serializable {
    if (probabilityValue > 1 || probabilityValue < 0) {
        throw new ApplicationException(ErrorMessages.PROBABILITY_IS_NOT_IN_RANGE)
    }
    def divide(other: Probability): Probability = {
        new Probability(this.probabilityValue / other.doubleValue)
    }

    def multiply(other: Probability): Probability = {
        new Probability(this.probabilityValue * other.doubleValue)
    }

    def doubleValue: Double = probabilityValue
}
