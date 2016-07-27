package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.clusterers.{Cluster, ClusteringAlgorithm, Clusters, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.{PivotTable, RowRecord}
import org.apache.spark.rdd.RDD

class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends AbstractRDD(parent, fileType) {

    private def getFileType: FileType = fileType

    /**
      * Returns a new RDD containing smoothed values of @columnIndex using @smoothingMethod
      *
      * @param columnIndex     Column Index
      * @param smoothingMethod Method that will be used for smoothing of the data
      * @return RDD<Double>
      */
    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): RDD[Double] = {
        validateColumnIndex(columnIndex)
        validateNumericColumn(columnIndex)
        val cleanRDD: TransformableRDD = removeRows(!_.isNumberAt(columnIndex))
        val columnDataset: RDD[String] = cleanRDD.select(columnIndex)
        smoothingMethod.smooth(columnDataset)
    }

    /**
      * Returns a new TransformableRDD containing only the elements that satisfy the matchInDictionary.
      *
      * @param predicate A matchInDictionary function, which gives bool value for every row.
      * @return TransformableRDD
      */
    def removeRows(predicate: (RowRecord) => Boolean): TransformableRDD = {
        val filteredRDD = filter((record: String) => {
            val rowRecord = fileType.parse(record)
            !predicate(rowRecord)
        })
        new TransformableRDD(filteredRDD, fileType)
    }

    /**
      * Returns a new TransformableRDD by replacing the @cluster's text with specified @newValue
      *
      * @param cluster     Cluster of similar values to be replaced
      * @param newValue    Value that will be used to replace all the cluster value
      * @param columnIndex Column index
      * @return TransformableRDD
      */
    def replaceValues(cluster: Cluster, newValue: String, columnIndex: Int): TransformableRDD = {
        val mapped: RDD[String] = map((row) => {
            var rowRecord: RowRecord = fileType.parse(row)
            val value: String = rowRecord.select(columnIndex)
            if (cluster.containsValue(value)) rowRecord = rowRecord.replace(columnIndex, newValue)
            fileType.join(rowRecord)
        })
        new TransformableRDD(mapped, fileType)
    }

    /**
      * Returns a new TransformableRDD by applying the function on all rows marked as @flag
      *
      * @param symbol            Symbol that has been used for flagging.
      * @param symbolColumnIndex Symbol column index
      * @param mapFunction       map function
      * @return TransformableRDD
      */
    def mapByFlag(symbol: String, symbolColumnIndex: Int, mapFunction: (String) => String): TransformableRDD = {
        val mappedRDD: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val symbolColumn: String = rowRecord.select(symbolColumnIndex)
            if (symbolColumn.equals(symbol)) mapFunction(record) else record
        })
        new TransformableRDD(mappedRDD, fileType)
    }

    /**
      * Returns a new TransformableRDD that contains records flagged by @symbol
      * based on the evaluation of @markerPredicate
      *
      * @param symbol          Symbol that will be used to flag
      * @param markerPredicate A matchInDictionary which will determine whether to flag a row or not
      * @return TransformableRDD
      */
    def flag(symbol: String, markerPredicate: (RowRecord) => Boolean): TransformableRDD = {
        val flagged: RDD[String] = map((record) => {
            var newRow: String = fileType.appendDelimiter(record)
            val rowRecord: RowRecord = fileType.parse(record)
            if (markerPredicate(rowRecord)) newRow += symbol
            newRow
        })
        new TransformableRDD(flagged, fileType)
    }

    /**
      * Returns number of column in this rdd
      *
      * @return int
      */
    def numberOfColumns(): Int = columnLength

    /**
      * Returns Clusters that has all cluster of text of @columnIndex according to @algorithm
      *
      * @param columnIndex         Column Index
      * @param clusteringAlgorithm Algorithm to be used to form clusters
      * @return Clusters
      */
    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): Clusters = {
        validateColumnIndex(columnIndex)
        val textFacets: TextFacets = listFacets(columnIndex)
        val rdd: RDD[(String, Int)] = textFacets.rdd
        val tuples: Array[(String, Int)] = rdd.collect

        clusteringAlgorithm.getClusters(tuples)
    }

    /**
      * Returns a new TextFacet containing the cardinal values of @columnIndex
      *
      * @param columnIndex index of the column
      * @return TextFacets
      */
    def listFacets(columnIndex: Int): TextFacets = {
        validateColumnIndex(columnIndex)
        val columnValuePair: RDD[(String, Int)] = map(record => (fileType.parse(record).select(columnIndex), 1))
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey(_ + _)
        new TextFacets(facets)
    }

    /**
      * Returns a RDD of double which is a product of the values in @firstColumn and @secondColumn
      *
      * @param firstColumn  First Column Index
      * @param secondColumn Second Column Index
      * @return RDD[Double]
      */
    def multiplyColumns(firstColumn: Int, secondColumn: Int): RDD[Double] = {
        validateColumnIndex(firstColumn :: secondColumn :: Nil)
        val rddOfNumbers: TransformableRDD = removeRows((record) => {
            !record.isNumberAt(firstColumn) || !record.isNumberAt(secondColumn)
        })
        rddOfNumbers.map((row) => {
            val firstColumnValue: String = fileType.parse(row).select(firstColumn)
            val secondColumnValue: String = fileType.parse(row).select(secondColumn)
            firstColumnValue.toDouble * secondColumnValue.toDouble
        })
    }

    /**
      * Generates a PivotTable by pivoting data in the pivotalColumn
      *
      * @param pivotalColumn            Pivotal Column
      * @param independentColumnIndexes Independent Column Indexes
      * @return PivotTable
      */
    def pivotByCount(pivotalColumn: Int, independentColumnIndexes: Seq[Int]): PivotTable[Integer] = {
        validateColumnIndex(independentColumnIndexes.+:(pivotalColumn).toList)
        val table: PivotTable[Integer] = new PivotTable[Integer](0)
        independentColumnIndexes.foreach((index) => {
            val facets: TextFacets = listFacets(pivotalColumn :: index :: Nil)
            val tuples = facets.rdd.collect()
            tuples.foreach((tuple) => {
                val split: Array[String] = tuple._1.split("\n")
                table.addEntry(split(0), split(1), tuple._2)
            })
        })
        table
    }

    /**
      * Returns a new TextFacet containing the facets of @columnIndexes
      *
      * @param columnIndexes List of column index
      * @return TextFacets
      */
    def listFacets(columnIndexes: List[Int]): TextFacets = {
        validateColumnIndex(columnIndexes)
        val columnValuePair: RDD[(String, Int)] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val joinedValue: String = rowRecord.select(columnIndexes).mkString("\n")
            (joinedValue, 1)
        })
        val facets: RDD[(String, Int)] = columnValuePair.reduceByKey(_ + _)
        new TextFacets(facets)
    }

    /**
      * Returns a TransformableRDD by splitting the @column according to the specified lengths
      *
      * @param column       Column index of the value to be split
      * @param fieldLengths List of integers specifying the number of character each split value will contains
      * @param retainColumn false when you want to remove the column value at @column in the result TransformableRDD
      * @return TransformableRDD
      */
    def splitByFieldLength(column: Int, fieldLengths: List[Int], retainColumn: Boolean = false): TransformableRDD = {
        validateColumnIndex(column)

        def splitString(value: String): Array[String] = {
            var result = Array.empty[String]
            var columnValue = value
            for (length <- fieldLengths) {
                val splitValue: String = {
                    if (columnValue.length >= length) columnValue.take(length) else columnValue
                }.mkString
                result = result.:+(splitValue)
                columnValue = columnValue.drop(length)
            }
            result
        }

        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val splitValue: Array[String] = splitString(rowRecord.select(column))
            val result: RowRecord = arrangeRecords(rowRecord, column :: Nil, splitValue, retainColumn)
            fileType.join(result)
        })
        new TransformableRDD(transformed, fileType)
    }

    private def arrangeRecords(rowRecord: RowRecord, cols: List[Int], result: Array[String], retainColumn: Boolean) = {
        if (retainColumn) rowRecord.appendColumns(result) else rowRecord.valuesNotAt(cols).appendColumns(result)
    }

    /**
      * Returns a new TransformableRDD by splitting the @column by the delimiter provided
      *
      * @param column       Column index of the value to be split
      * @param delimiter    delimiter or regEx that will be used to split the value @column
      * @param retainColumn false when you want to remove the column value at @column in the result TransformableRDD
      * @param maxSplit     Maximum number of split to be added to the result TransformableRDD
      * @return TransformableRDD
      */
    def splitByDelimiter(column: Int, delimiter: String, retainColumn: Boolean = false, maxSplit: Int = -1):
    TransformableRDD = {
        validateColumnIndex(column)
        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val splitValue: Array[String] = rowRecord.select(column).split(delimiter, maxSplit)
            val result: RowRecord = arrangeRecords(rowRecord, List(column), splitValue, retainColumn)
            fileType.join(result)
        })
        new TransformableRDD(transformed, fileType)
    }

    /**
      * Returns a new TransformableRDD by merging 2 or more columns together
      *
      * @param columns       List of columns to be merged
      * @param separator     Separator to be used to separate the merge value
      * @param retainColumns false when you want to remove the column value at @column in the result TransformableRDD
      * @return TransformableRDD
      */
    def mergeColumns(columns: List[Int], separator: String = " ", retainColumns: Boolean = false): TransformableRDD = {
        validateColumnIndex(columns)
        val transformedRDD: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val mergedValue: String = rowRecord.select(columns).mkString(separator)
            val result: RowRecord = arrangeRecords(rowRecord, columns, Array(mergedValue), retainColumns)
            fileType.join(result)
        })
        new TransformableRDD(transformedRDD, fileType)
    }

    /**
      * Returns a new TransformableRDD by normalizing values of the given column using different Normalizers
      *
      * @param columnIndex Column Index
      * @param normalizer  Normalization Strategy
      * @return TransformableRDD
      */
    def normalize(columnIndex: Int, normalizer: NormalizationStrategy): TransformableRDD = {
        validateColumnIndex(columnIndex)
        normalizer.prepare(this, columnIndex)
        val rdd: RDD[String] = map((record) => {
            val columns: RowRecord = fileType.parse(record)
            val normalizedColumn = normalizer.normalize(columns.select(columnIndex))
            val normalizedRecord: RowRecord = columns.replace(columnIndex, normalizedColumn)
            fileType.join(normalizedRecord)
        })
        new TransformableRDD(rdd, fileType)
    }

    /**
      * Returns a new TransformableRDD by imputing missing values of the @columnIndex using the @strategy
      *
      * @param column   Column index
      * @param strategy Imputation strategy
      * @return TransformableRDD
      */
    def impute(column: Int, strategy: ImputationStrategy): TransformableRDD = impute(column, strategy, List.empty)

    /**
      * Returns a new TransformableRDD by imputing missing values and @missingHints of the @columnIndex using the @strategy
      *
      * @param columnIndex  Column Index
      * @param strategy     Imputation Strategy
      * @param missingHints List of Strings that may mean empty
      * @return TransformableRDD
      */
    def impute(columnIndex: Int, strategy: ImputationStrategy, missingHints: List[String]): TransformableRDD = {
        validateColumnIndex(columnIndex)
        strategy.prepareSubstitute(this, columnIndex)
        val transformed: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val value: String = rowRecord.select(columnIndex)
            var replacementValue: String = value
            if (value == null || value.isEmpty || missingHints.contains(value)) {
                replacementValue = strategy.handleMissingData(rowRecord)
            }

            fileType.join(rowRecord.replace(columnIndex, replacementValue))
        })

        new TransformableRDD(transformed, fileType)
    }

    /**
      * Returns a new TransformableRDD by dropping the @columnIndex
      *
      * @param columnIndex The column that will be dropped.
      * @return TransformableRDD
      */
    def drop(columnIndex: Int, columnIndexes: Int*): TransformableRDD = {
        val columnsToBeDroped: List[Int] = columnIndexes.+:(columnIndex).toList
        validateColumnIndex(columnsToBeDroped)
        val transformed: RDD[String] = map((record: String) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val resultRecord: RowRecord = rowRecord.valuesNotAt(columnsToBeDroped)
            fileType.join(resultRecord)
        })
        new TransformableRDD(transformed, fileType)
    }

    /**
      * Returns a new RDD containing the duplicate values at the specified column
      *
      * @param columnIndex Column where to look for duplicates
      * @return RDD
      */
    def duplicatesAt(columnIndex: Int): RDD[String] = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_).select(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).duplicates()
    }

    /**
      * Returns a new TransformableRDD containing duplicate records of this TransformableRDD by considering all the columns as primary key.
      *
      * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
      */
    def duplicates(): TransformableRDD = duplicates(List.empty)

    /**
      * Returns a new TransformableRDD containing unique duplicate records of this TransformableRDD by considering the given columns as primary key.
      *
      * @param primaryKeyColumns A list of integers specifying the columns that will be combined to create the primary key
      * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
      */
    def duplicates(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRecord: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val recordsGroupedByKey: RDD[(Long, List[String])] = fingerprintedRecord.aggregateByKey(List.empty[String])(
            (accumulatorValues, currentValue) => accumulatorValues.::(currentValue),
            (aggregator1, aggregator2) => aggregator1 ::: aggregator2
        )
        val duplicateRecords: RDD[String] = recordsGroupedByKey.filter(_._2.size != 1).flatMap(_._2)
        new TransformableRDD(duplicateRecords, fileType).deduplicate()
    }

    /**
      * Returns a new TransformableRDD containing unique duplicate records of this TransformableRDD by considering all the columns as primary key.
      *
      * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
      */
    def deduplicate(): TransformableRDD = deduplicate(List.empty)

    /**
      * Returns a new TransformableRDD containing unique duplicate records of this TransformableRDD by considering the given columns as primary key.
      *
      * @param primaryKeyColumns A list of integers specifying the columns that will be combined to create the primary key
      * @return TransformableRDD A new TransformableRDD consisting unique duplicate records.
      */
    def deduplicate(primaryKeyColumns: List[Int]): TransformableRDD = {
        validateColumnIndex(primaryKeyColumns)
        val fingerprintedRDD: RDD[(Long, String)] = generateFingerprintedRDD(primaryKeyColumns)
        val reducedRDD: RDD[(Long, String)] = fingerprintedRDD.reduceByKey((accumulator, record) => record)
        new TransformableRDD(reducedRDD.values, fileType)
    }

    private def generateFingerprintedRDD(primaryKeyColumns: List[Int]): RDD[(Long, String)] = {
        map(record => {
            val rowRecord: RowRecord = fileType.parse(record)
            val fingerprint = rowRecord.fingerprintBy(primaryKeyColumns)
            (fingerprint, record)
        })
    }

    /**
      * Returns a new TransformableRDD containing the unique elements in the specified column
      *
      * @param columnIndex Column Index
      * @return RDD<String>
      */
    def unique(columnIndex: Int): RDD[String] = {
        validateColumnIndex(columnIndex)
        val specifiedColumnValues: RDD[String] = map(fileType.parse(_).select(columnIndex))
        new TransformableRDD(specifiedColumnValues, fileType).deduplicate()
    }

    /**
      * Zips the other TransformableRDD with this TransformableRDD and
      * returns a new TransformableRDD with current file format.
      * Both the TransformableRDD must have same number of records
      *
      * @param otherRDD Other TransformableRDD from where the columns will be added to this TransformableRDD
      * @return TransformableRDD
      */
    def addColumnsFrom(otherRDD: TransformableRDD): TransformableRDD = {
        val otherRDDInCurrentFileFormat = {
            if (this.getFileType != otherRDD.getFileType) {
                otherRDD.map(record => {
                    val rowRecord: RowRecord = otherRDD.getFileType.parse(record)
                    fileType.join(rowRecord)
                })
            }
            else {
                otherRDD
            }
        }
        val zippedRecords: RDD[(String, String)] = zip(otherRDDInCurrentFileFormat)
        val recordsWithAddedColumns: RDD[String] = zippedRecords.map(row => fileType.appendDelimiter(row._1) + row._2)

        new TransformableRDD(recordsWithAddedColumns, fileType)
    }

    /**
      * Returns a new TransformableRDD containing values of @columnIndexes
      *
      * @param columnIndexes A number of integer values specifying the columns that will be used to create the new table
      * @return TransformableRDD
      */
    def select(columnIndexes: List[Int]): TransformableRDD = {
        validateColumnIndex(columnIndexes)
        val selectedColumnValues: RDD[String] = map((record) => {
            val rowRecord: RowRecord = fileType.parse(record)
            val resultValues: RowRecord = rowRecord.select(columnIndexes)
            fileType.join(resultValues)
        })
        new TransformableRDD(selectedColumnValues, fileType)
    }
}