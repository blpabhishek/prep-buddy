package org.apache.datacommons.prepbuddy.qualityanalyzers

import org.scalatest.FunSuite


class TypeAnalyzerTest extends FunSuite {
    test("should be Able to Detect the type as a string") {
        val dataSet = List("facebook", "Vampire", "teachStreet", "twitter")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING eq typeAnalyzer.getType)
    }
    test("should be able to detect type as decimal") {
        val dataSet = List(".56", "290.56", "23", "2345676543245.7654564", "405.34", "234.65", "34.12")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL eq typeAnalyzer.getType)
    }

    test("should Be Able To Give The Type As Decimal For Decimal") {
        val dataSet = List("12", "23", "0.56", ".56", ".23", "0.7654564", "67.44", "34.23", "12.12", "-0.23", "-23.0")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL eq typeAnalyzer.getType)
    }
    test("should be able to give type as integer") {
        val dataSet = List("12", "23", "34", "-14", "-23")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(INTEGER eq typeAnalyzer.getType)
    }
    test("should be able to give the type as email") {
        val dataSet = List("max@fireworks.in", "jst@mls.co.in", "bil_man@cil.com", "bill.se@gmail.com", "a@b.ci")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(EMAIL eq typeAnalyzer.getType)
    }
    test("should be able to give the type as currency") {
        val dataSet: List[String] = List("$2", "€223.78", "₹23")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(CURRENCY eq typeAnalyzer.getType)
    }
    test("should give the type as String if found multiple type") {
        val dataSet: List[String] = List("$2", "€223.78", "₹23", "max@fireworks.in", "jst@mls.co.in")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING eq typeAnalyzer.getType)
    }
    test("should be Able to give the type as URL") {
        val dataSet: List[String] = List("http://some.com/file/xl", "http://www.si.com", "https://www.si.co.in")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(URL eq typeAnalyzer.getType)
    }
    test("should be able to give IP Address as Type") {
        val dataSet: List[String] = List("19.55.12.34", "11.45.78.34", "10.4.22.246", "10.4.22.246")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(IP_ADDRESS eq typeAnalyzer.getType)
    }
    test("should be able to give type as US ZIP Code") {
        val dataSet: List[String] = List("12345", "23456-1234", "12345", "34567")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ZIP_CODE_US eq typeAnalyzer.getType)
    }
    test("should be able to give type as mobile Number") {
        val dataSet: List[String] = List("6723459812", "9992345678", "04576893245", "02345678901", "03465789012")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(MOBILE_NUMBER eq typeAnalyzer.getType)
    }

    test("should Be Able To Give Type As Mobile Number With Code") {
        val dataSet: List[String] = List("+12 6723459812", "+92 9992345678", "+343 2424432489", "+324 5435454456")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(MOBILE_NUMBER eq typeAnalyzer.getType)
    }

    test("should be able to give type as timestamp") {
        val dataSet = List("2010-07-21T08:52:05.222", "2016-05-20T12:51:00.282Z", "2016-05-20T13:04:41.632Z")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(TIMESTAMP eq typeAnalyzer.getType)
    }

    test("should Be Able To Detect Longitude As Type") {
        val dataSet: List[String] = List("-122.42045568910925", "-73.9683", "-155.93665", "-107.4799714831678")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(LONGITUDE eq typeAnalyzer.getType)
    }

    test("should Be Able To Detect LATITUDE As Type") {
        val dataSet: List[String] = List("+40.2201", "+40.7415", "+19.05939", "+36.99409882257", "-82.180507982090035")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(LATITUDE eq typeAnalyzer.getType)
    }
    test("should be able to give type as social security Number") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("409-52-2002", "876-66-4978", "123-46-4878"))
        assert(SOCIAL_SECURITY_NUMBER eq typeAnalyzer.getType)
    }
    test("should be able to give country name as a type") {
        val dataSet: List[String] = List("India", "China", "South Africa", "United States", "Uganda")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(COUNTRY_NAME eq typeAnalyzer.getType)
    }
    test("should Be Able To Give Type As Country Code 3 CHARACTER") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("IND", "USA", "CHN"))
        assert(COUNTRY_CODE_3_CHARACTER eq typeAnalyzer.getType)
    }
    test("should Be Able To Give Type As Country Code 2 CHARACTER") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("IN", "IR", "US"))
        assert(COUNTRY_CODE_2_CHARACTER eq typeAnalyzer.getType)
    }
    test("should Be Able To Infer The Data Type As Integer") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("0", "1", "2", "3", "4", "5"))
        assert(INTEGER eq typeAnalyzer.getType)
    }
    test("should Be Able To Infer The Data Type As CATEGORICAL_INTEGER ") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("0", "1", "0", "1", "0", "1", "0", "1"))
        assert(CATEGORICAL_INTEGER eq typeAnalyzer.getType)
    }
    test("should Be Able To Infer The Data Type As CATEGORICAL_STRING ") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("A", "A", "B", "B", "C", "C", "C", "C"))
        assert(CATEGORICAL_STRING eq typeAnalyzer.getType)
    }
    test("should be able to detect type as EMPTY for empty values") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("N\\A", "\\N", "N\\A", "N\\A", "NA", "NAN"))
        assert(EMPTY eq typeAnalyzer.getType)
    }
    test("should Not Give The Type As Country Code2 CHARACTER When The Data Consist Of NAorNULL") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("\\N", "N\\A"))
        assert(COUNTRY_CODE_2_CHARACTER != typeAnalyzer.getType)
    }
    test("should Not Classify As Latitude Or Longitude") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("40.2201", "40.7415", "19.05939", "100", "182"))
        assert(DECIMAL eq typeAnalyzer.getType)
    }
}
