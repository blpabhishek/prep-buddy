package org.apache.datacommons.prepbuddy.utils

import scala.collection.{Set, mutable}

class PivotTable[T] (defaultValue: T) {

    def transform(transformedFunction: (Any) => Any, defValue: Any) = {
        val table = new PivotTable[Any](defValue)
        lookUpTable.keysIterator.foreach((each) => {
            val columns = lookUpTable(each)
            columns.keysIterator.foreach((eachColumn) => {
                val columnValue = columns(eachColumn)
                table.addEntry(each, eachColumn, transformedFunction(columnValue))
            })
        })
        table
    }

    private var lookUpTable: mutable.Map[String, mutable.Map[String, T]] = new mutable.HashMap[String, mutable.Map[String, T]]()

    def valueAt(rowKey: String, columnKey: String): T = {
        lookUpTable(rowKey)(columnKey)
    }
    def addEntry(rowKey: String, columnKey: String, value: T) = {
        if (!lookUpTable.contains(rowKey)) {
            val columnMap = new mutable.HashMap[String, T]().withDefaultValue(defaultValue)
            lookUpTable += (rowKey -> columnMap)
        }
        lookUpTable(rowKey) += (columnKey -> value)
    }


}
