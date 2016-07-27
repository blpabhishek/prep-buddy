package org.apache.datacommons.prepbuddy.utils

import java.security.MessageDigest

import org.apache.commons.lang.math.NumberUtils

class RowRecord(columnValues: Array[String]) {
    def length: Int = columnValues.length

    def select(columnIndex: Int): String = columnValues(columnIndex)

    def select(columnIndexes: List[Int]): RowRecord = {
        val filteredValues: Array[String] = columnIndexes.map(columnValues(_)).toArray
        new RowRecord(filteredValues)
    }

    def valuesNotAt(columnIndexes: List[Int]): RowRecord = {
        val valuesExcludingGivenIndexes: Array[String] = columnValues.view.zipWithIndex
            .filterNot { case (value, index) => columnIndexes.contains(index) }
            .map(_._1)
            .toArray
        new RowRecord(valuesExcludingGivenIndexes)
    }

    def mkString(delimiter: String): String = columnValues.mkString(delimiter)

    def hasEmptyColumn: Boolean = columnValues.exists(_.trim.isEmpty)

    def replace(columnIndex: Int, newValue: String): RowRecord = {
        val newRecord: Array[String] = columnValues.clone()
        newRecord(columnIndex) = newValue
        new RowRecord(newRecord)
    }

    def appendColumns(values: Array[String]): RowRecord = {
        val recordWithAppendedColumn: Array[String] = (columnValues.toList ::: values.toList).toArray
        new RowRecord(recordWithAppendedColumn)
    }

    def fingerprintBy(columnIndexes: List[Int]): Long = {
        val keyValues: List[String] = if (columnIndexes.isEmpty) columnValues.toList else columnIndexes.map(select)
        val concatenatedString: String = keyValues.mkString("")
        val algorithm: MessageDigest = MessageDigest.getInstance("MD5")
        algorithm.update(concatenatedString.getBytes, 0, concatenatedString.length)
        BigInt(algorithm.digest()).longValue()
    }

    def isNumberAt(columnIndex: Int): Boolean = NumberUtils.isNumber(columnValues(columnIndex))
}
