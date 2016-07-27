package org.apache.datacommons.prepbuddy.utils

import org.apache.datacommons.prepbuddy.SparkTestCase

class RowRecordTest extends SparkTestCase {
    test("should return number of columns in the record") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert(3 == rowRecord.length)
    }

    test("should return the value at the given column index") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert("z" == rowRecord.select(2))
    }

    test("should return a new RowRecord excluding the given columns") {
        val rowRecord: RowRecord = new RowRecord(Array("a", "b", "c", "x", "y", "z"))
        val newRowRecord: RowRecord = rowRecord.valuesNotAt(List(0, 2, 3, 4))

        assert(2 == newRowRecord.length)
        assert("b" == newRowRecord.select(0))
        assert("z" == newRowRecord.select(1))
    }

    test("should not modify the original row record while calling valueNotAt method") {
        val rowRecord: RowRecord = new RowRecord(Array("a", "b", "c", "x", "y", "z"))
        rowRecord.valuesNotAt(List(0, 2, 3, 4))

        assert(6 == rowRecord.length)
        assert("a" == rowRecord.select(0))
    }

    test("should gives back a string by joining all the columns by the given delimiter") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert("x-y-z" == rowRecord.mkString("-"))
    }

    test("should return true if the record contains empty value") {
        val rowRecordWithSpaceAsEmpty: RowRecord = new RowRecord(Array("x", " ", "z"))
        val rowRecordWithTabAsEmpty: RowRecord = new RowRecord(Array("x", "\t", "z"))

        assert(rowRecordWithSpaceAsEmpty.hasEmptyColumn)
        assert(rowRecordWithTabAsEmpty.hasEmptyColumn)
    }

    test("should return false if the record does not contains empty value") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert(!rowRecord.hasEmptyColumn)
    }

    test("should return a new RowRecord by replace the value at the given index with new value") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))
        val newRecord: RowRecord = rowRecord.replace(1, "Y")

        assert("Y" == newRecord.select(1))
    }

    test("should not modify the original row record while calling replace method") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))
        val newRecord: RowRecord = rowRecord.replace(1, "Y")

        assert("y" == rowRecord.select(1))
        assert("Y" == newRecord.select(1))
    }

    test("should return a new RowRecord containing the columns at the specified index") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))
        val newRecord: RowRecord = rowRecord.select(List(0, 2))

        assert(2 == newRecord.length)
        assert("x" == newRecord.select(0))
        assert("z" == newRecord.select(1))
    }

    test("should not modify the original row record while calling valuesAt method") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))
        val newRecord: RowRecord = rowRecord.select(List(0, 2))

        assert(3 == rowRecord.length)
        assert(2 == newRecord.length)
    }

    test("should return true if the specified index contains a number") {
        val rowRecord: RowRecord = new RowRecord(Array("2", "y", "100.023"))

        assert(rowRecord.isNumberAt(2))
    }

    test("should return false if the specified index does not contains a number") {
        val rowRecord: RowRecord = new RowRecord(Array("2", "y", "100.023"))

        assert(!rowRecord.isNumberAt(1))
    }

    test("should return fingerprint by considering the given columns") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert(3088996759045414165L == rowRecord.fingerprintBy(List(0, 1)))
    }

    test("should return fingerprint by considering all the columns when passed empty list") {
        val rowRecord: RowRecord = new RowRecord(Array("x", "y", "z"))

        assert(-7382504379390136226L == rowRecord.fingerprintBy(List.empty))
    }
}
