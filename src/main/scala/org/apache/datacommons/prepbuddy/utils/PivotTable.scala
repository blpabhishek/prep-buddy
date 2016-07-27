package org.apache.datacommons.prepbuddy.utils

import scala.collection.mutable

class PivotTable[T](defaultValue: T) extends Serializable {
    /**
      *Represents a table of organized and summarized selected columns and rows of data.
      */
    private var lookUpTable: mutable.Map[String, mutable.Map[String, T]] = {
        new mutable.HashMap[String, mutable.Map[String, T]]()
    }

    def transform(transformedFunction: (Any) => Any, defValue: Any): Any = {
        val table = new PivotTable[Any](defValue)
        for (rowTuple <- lookUpTable; columnTuple <- rowTuple._2)
            table.addEntry(rowTuple._1, columnTuple._1, transformedFunction(columnTuple._2))
        table
    }

    def addEntry(rowKey: String, columnKey: String, value: T): Unit = {
        if (!lookUpTable.contains(rowKey)) {
            val columnMap = new mutable.HashMap[String, T]().withDefaultValue(defaultValue)
            lookUpTable += (rowKey -> columnMap)
        }
        lookUpTable(rowKey) += (columnKey -> value)
    }

    def valueAt(rowKey: String, columnKey: String): T = {
        lookUpTable(rowKey)(columnKey)
    }
}
