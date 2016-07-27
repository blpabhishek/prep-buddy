package org.apache.datacommons.prepbuddy.qualityanalyzers

import java.util.Locale


abstract class DataType {
    def isOfType(sampleData: List[String]): Boolean

    protected def matches(regex: String, samples: List[String]): Boolean = {
        val matches: List[String] = samples.filter(_.matches(regex))
        val threshold = Math.round(samples.length * 0.75)
        matches.size > threshold
    }


    protected def matchInDictionary(sampleData: List[String], dictionary: Set[String]): Boolean = {
        val sampleInLowerCase: Set[String] = sampleData.map(_.toLowerCase).toSet
        val qualifyingLimit: Long = Math.round(sampleInLowerCase.size * 0.75)
        val size: Int = dictionary.intersect(sampleInLowerCase).size
        size >= qualifyingLimit
    }

    protected def isCategorical(sampleData: List[String], categoricalSize: Int): Boolean = {
        var set: Set[String] = Set()
        set = set.++:(sampleData)
        set.size <= categoricalSize
    }
}

object ALPHANUMERIC_STRING extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = true
}

object DECIMAL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[+-]?(\\.?\\d+|\\d+\\.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}

object INTEGER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[+-]?\\d+$"
        matches(EXPRESSION, sampleData)
    }
}

object EMAIL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$"
        matches(EXPRESSION, sampleData)
    }
}

object CURRENCY extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^(\\p{Sc})(\\d+|\\d+.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}

object URL extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$"
        matches(EXPRESSION, sampleData)
    }
}

object IP_ADDRESS extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
        matches(EXPRESSION, sampleData)
    }
}

object ZIP_CODE_US extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[0-9]{5}(?:-[0-9]{4})?$"
        matches(EXPRESSION, sampleData)
    }
}

object MOBILE_NUMBER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^(([+]\\d+\\s)|0)?\\d{10}$"
        matches(EXPRESSION, sampleData)
    }
}

object SOCIAL_SECURITY_NUMBER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^(\\d{3}-\\d{2}-\\d{4})$"
        matches(EXPRESSION, sampleData)
    }
}

object TIMESTAMP extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[Z]?)"
        matches(EXPRESSION, sampleData)
    }
}

object LONGITUDE extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[-+](180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$"
        matches(EXPRESSION, sampleData)
    }
}

object LATITUDE extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val EXPRESSION: String = "^[-+]([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$"
        matches(EXPRESSION, sampleData)
    }
}

object COUNTRY_NAME extends DataType {

    override def isOfType(sampleData: List[String]): Boolean = {
        val countryNames: Set[String] = getCountryNames
        matchInDictionary(sampleData, countryNames)
    }

    private def getCountryNames: Set[String] = {
        val isoCountries: Array[String] = Locale.getISOCountries
        val countryList: Array[String] = isoCountries.map((country) => {
            val locale = new Locale("", country)
            locale.getDisplayCountry.toLowerCase
        })
        countryList.toSet
    }
}

object COUNTRY_CODE_3_CHARACTER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val countryNames: Set[String] = getCountryCodes
        matchInDictionary(sampleData, countryNames)
    }

    private def getCountryCodes: Set[String] = {
        val isoCountries: Array[String] = Locale.getISOCountries
        val countryList: Array[String] = isoCountries.map((country) => {
            val locale = new Locale("", country)
            locale.getISO3Country.toLowerCase
        })
        countryList.toSet
    }
}

object COUNTRY_CODE_2_CHARACTER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val countryNames: Set[String] = Locale.getISOCountries.map(_.toLowerCase).toSet
        matchInDictionary(sampleData, countryNames)
    }
}

object CATEGORICAL_STRING extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val categoricalSize: Int = 3
        isCategorical(sampleData, categoricalSize)
    }
}

object CATEGORICAL_INTEGER extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val categoricalSize: Int = 2
        isCategorical(sampleData, categoricalSize)
    }
}

object EMPTY extends DataType {
    override def isOfType(sampleData: List[String]): Boolean = {
        val emptyRepresentation: Set[String] = Set("N\\A", "\\N", "NULL", "blank", "(blank)", "NaN", "NA")
        matchInDictionary(sampleData, emptyRepresentation.map(_.toLowerCase))
    }
}

