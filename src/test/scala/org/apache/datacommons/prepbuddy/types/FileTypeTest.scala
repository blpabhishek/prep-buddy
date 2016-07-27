package org.apache.datacommons.prepbuddy.types

import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.scalatest.FunSuite

class FileTypeTest extends FunSuite {
    test("should be able to parse CSV file and return a RowRecord") {
        val actual: RowRecord = CSV.parse("Name, Gender,Age")

        val expected: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))

        assert(actual.select(0) == expected.select(0))
        assert(actual.select(1) == expected.select(1))
        assert(actual.select(2) == expected.select(2))
    }

    test("should generate RowRecord into CSV format") {
        val record: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))

        val expected: String = "Name,Gender,Age"

        assert(expected == CSV.join(record))
    }

    test("should be able to parse TSV file and return a RowRecord") {
        val actual: RowRecord = TSV.parse("Name\t Gender\tAge")

        val expected: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))

        assert(actual.select(0) == expected.select(0))
        assert(actual.select(1) == expected.select(1))
        assert(actual.select(2) == expected.select(2))
    }

    test("should generate RowRecord into TSV format") {
        val record: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))

        val expected: String = "Name\tGender\tAge"

        assert(expected == TSV.join(record))
    }
}
