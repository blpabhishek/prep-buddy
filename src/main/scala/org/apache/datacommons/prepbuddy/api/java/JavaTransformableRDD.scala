package org.apache.datacommons.prepbuddy.api.java

import java.util

import org.apache.datacommons.prepbuddy.clusterers.{ClusteringAlgorithm, TextFacets}
import org.apache.datacommons.prepbuddy.imputations.ImputationStrategy
import org.apache.datacommons.prepbuddy.normalizers.NormalizationStrategy
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.smoothers.SmoothingMethod
import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.datacommons.prepbuddy.utils.PivotTable
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}

import scala.collection.JavaConverters._

class JavaTransformableRDD(rdd: JavaRDD[String], fileType: FileType) extends JavaRDD[String](rdd.rdd) {

    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd, fileType)

    def this(rdd: JavaRDD[String]) {
        this(rdd, CSV)
    }

    /**
      * Returns a new JavaTransformableRDD containing only the elements that satisfy the matchInDictionary.
      *
      * @param rowPurger A matchInDictionary function, which gives bool value for every row.
      * @return JavaTransformableRDD
      */
    def removeRows(rowPurger: RowPurger): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.removeRows(rowPurger.evaluate).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD containing unique duplicate records of this JavaTransformableRDD
      * by considering the given columns as primary key.
      *
      * @param primaryKeyColumns A list of integers specifying the columns that will be combined to create the primary key
      * @return JavaTransformableRDD A new JavaTransformableRDD consisting unique duplicate records.
      */
    def deduplicate(primaryKeyColumns: util.List[Integer]): JavaTransformableRDD = {
        val scalaList: List[Int] = asScalaIntList(primaryKeyColumns.asScala.toList)
        new JavaTransformableRDD(tRDD.deduplicate(scalaList).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD containing unique duplicate records of this JavaTransformableRDD
      * by considering all the columns as primary key.
      *
      * @return JavaTransformableRDD A new JavaTransformableRDD consisting unique duplicate records.
      */
    def deduplicate: JavaTransformableRDD = new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD(), fileType)

    /**
      * Returns a new JavaTransformableRDD containing unique duplicate records of this
      * JavaTransformableRDD by considering the given columns as primary key.
      *
      * @param primaryKeyColumns A list of integers specifying the columns that will be combined to create the primary key
      * @return JavaTransformableRDD A new JavaTransformableRDD consisting unique duplicate records.
      */
    def duplicates(primaryKeyColumns: util.List[Integer]): JavaTransformableRDD = {
        val scalaList: List[Int] = asScalaIntList(primaryKeyColumns.asScala.toList)
        new JavaTransformableRDD(tRDD.duplicates(scalaList).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD containing unique duplicate records of this JavaTransformableRDD
      * by considering all the columns as primary key.
      *
      * @return JavaTransformableRDD A new JavaTransformableRDD consisting unique duplicate records.
      */
    def duplicates: JavaTransformableRDD = new JavaTransformableRDD(tRDD.duplicates().toJavaRDD(), fileType)

    /**
      * Returns a new JavaTransformableRDD by imputing missing values and @missingHints of the @columnIndex
      * using the @strategy
      *
      * @param columnIndex  Column Index
      * @param strategy     Imputation Strategy
      * @param missingHints List of Strings that may mean empty
      * @return JavaTransformableRDD
      */
    def impute(columnIndex: Int, strategy: ImputationStrategy, missingHints: util.List[String]):
    JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, strategy, missingHints.asScala.toList), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD by imputing missing values of the @columnIndex using the @strategy
      *
      * @param columnIndex Column index
      * @param strategy    Imputation strategy
      * @return JavaTransformableRDD
      */
    def impute(columnIndex: Int, strategy: ImputationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.impute(columnIndex, strategy).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaDoubleRDD containing smoothed values of @columnIndex using @smoothingMethod
      *
      * @param columnIndex     Column Index
      * @param smoothingMethod Method that will be used for smoothing of the data
      * @return JavaDoubleRDD
      */
    def smooth(columnIndex: Int, smoothingMethod: SmoothingMethod): JavaDoubleRDD = {
        new JavaDoubleRDD(tRDD.smooth(columnIndex, smoothingMethod))
    }

    /**
      * Returns Clusters that has all cluster of text of @columnIndex according to @algorithm
      *
      * @param columnIndex         Column Index
      * @param clusteringAlgorithm Algorithm to be used to form clusters
      * @return Clusters
      */
    def clusters(columnIndex: Int, clusteringAlgorithm: ClusteringAlgorithm): JavaClusters = {
        new JavaClusters(tRDD.clusters(columnIndex, clusteringAlgorithm))
    }

    /**
      * Returns a new TextFacet containing the cardinal values of @columnIndex
      *
      * @param columnIndex index of the column
      * @return TextFacets
      */
    def listFacets(columnIndex: Int): TextFacets = tRDD.listFacets(columnIndex)

    /**
      * Returns a new TextFacet containing the facets of @columnIndexes
      *
      * @param columnIndexes List of column index
      * @return TextFacets
      */
    def listFacets(columnIndexes: util.List[Integer]): TextFacets = {
        tRDD.listFacets(asScalaIntList(columnIndexes.asScala.toList))
    }

    private def asScalaIntList(ls: List[Integer]): List[Int] = ls.map(x => x: Int)

    /**
      * Returns a new JavaTransformableRDD by normalizing values of the given column using different Normalizers
      *
      * @param columnIndex Column Index
      * @param normalizer  Normalization Strategy
      * @return JavaTransformableRDD
      */
    def normalize(columnIndex: Int, normalizer: NormalizationStrategy): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.normalize(columnIndex, normalizer).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD containing values of @columnIndexes
      *
      * @param columnIndexes A number of integer values specifying the columns that will be used to create the new table
      * @return JavaTransformableRDD
      */
    def select(columnIndexes: util.List[Integer]): JavaTransformableRDD = {
        val scalaList: List[Int] = asScalaIntList(columnIndexes.asScala.toList)
        new JavaTransformableRDD(tRDD.select(scalaList).toJavaRDD(), fileType)
    }

    /**
      * Returns a JavaRDD of given column
      *
      * @param columnIndex Column index
      * @return JavaRDD[String]
      */
    def select(columnIndex: Int): JavaRDD[String] = {
        tRDD.select(columnIndex).toJavaRDD()
    }

    /**
      * Returns number of column in this rdd
      *
      * @return Int
      */
    def numberOfColumns: Int = tRDD.numberOfColumns()

    /**
      * Generates a PivotTable by pivoting data in the pivotalColumn
      *
      * @param pivotalColumn            Pivotal Column
      * @param independentColumnIndexes Independent Column Indexes
      * @return PivotTable
      */
    def pivotByCount(pivotalColumn: Int, independentColumnIndexes: util.List[Integer]): PivotTable[Integer] = {
        tRDD.pivotByCount(pivotalColumn, asScalaIntList(independentColumnIndexes.asScala.toList))
    }

    /**
      * Returns a new JavaTransformableRDD by merging @columnIndexes with default separator
      *
      * @param columnIndexes
      * @return JavaTransformableRDD
      */
    def mergeColumns(columnIndexes: util.List[Integer]): JavaTransformableRDD = {
        mergeColumns(columnIndexes = columnIndexes, " ", retainColumns = false)
    }

    /**
      * Returns a new JavaTransformableRDD by merging @columnIndexes
      *
      * @param columnIndexes List of columns to be merged
      * @param separator     Separator to be used to separate the merge value
      * @param retainColumns false when you want to remove the column value at @column in the result TransformableRDD
      * @return JavaTransformableRDD
      */
    def mergeColumns(columnIndexes: util.List[Integer], separator: String = " ", retainColumns: Boolean = false):
    JavaTransformableRDD = {
        val toScalaList: List[Int] = asScalaIntList(columnIndexes.asScala.toList)
        val mergedRDD: JavaRDD[String] = tRDD.mergeColumns(toScalaList, separator, retainColumns).toJavaRDD()
        new JavaTransformableRDD(mergedRDD, fileType)
    }

    /**
      * Returns a JavaTransformableRDD by splitting the @column according to the specified lengths
      *
      * @param columnIndex  Column index of the value to be split
      * @param fieldLengths List of integers specifying the number of character each split value will contains
      * @param retainColumn false when you want to remove the column value at @column in the result TransformableRDD
      * @return JavaTransformableRDD
      */
    def splitByFieldLength(columnIndex: Int, fieldLengths: util.List[Integer], retainColumn: Boolean):
    JavaTransformableRDD = {
        val toScalaList: List[Int] = asScalaIntList(fieldLengths.asScala.toList)
        val splitRDD: JavaRDD[String] = tRDD.splitByFieldLength(columnIndex, toScalaList, retainColumn).toJavaRDD()
        new JavaTransformableRDD(splitRDD, fileType)
    }

    //    TODO: Implement overload method for mex split functionality
    /**
      * Returns a new JavaTransformableRDD by splitting the @column by the delimiter provided
      *
      * @param columnIndex  Column index of the value to be split
      * @param delimiter    delimiter or regEx that will be used to split the value @column
      * @param retainColumn false when you want to remove the column value at @column in the result JavaTransformableRDD
      * @return JavaTransformableRDD
      */
    def splitByDelimiter(columnIndex: Int, delimiter: String, retainColumn: Boolean): JavaTransformableRDD = {
        val rdd: JavaRDD[String] = tRDD.splitByDelimiter(columnIndex, delimiter, retainColumn).toJavaRDD()
        new JavaTransformableRDD(rdd, fileType)
    }

    /**
      * Returns a new JavaTransformableRDD that contains records flagged by @symbol
      * based on the evaluation of @markerPredicate
      *
      * @param symbol          Symbol that will be used to flag
      * @param markerPredicate A matchInDictionary which will determine whether to flag a row or not
      * @return JavaTransformableRDD
      */
    def flag(symbol: String, markerPredicate: MarkerPredicate): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.flag(symbol, markerPredicate.evaluate).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD by applying the function on all rows marked as @flag
      *
      * @param symbol            Symbol that has been used for flagging.
      * @param symbolColumnIndex Symbol column index
      * @param function          map function
      * @return JavaTransformableRDD
      */
    def mapByFlag(symbol: String, symbolColumnIndex: Int, function: Function[String, String]): JavaTransformableRDD = {
        val mappedRDD: JavaRDD[String] = tRDD.mapByFlag(symbol, symbolColumnIndex, function.call).toJavaRDD()
        new JavaTransformableRDD(mappedRDD, fileType)
    }

    /**
      * Returns a new JavaTransformableRDD by dropping the @columnIndex
      *
      * @param columnIndex The column that will be dropped.
      * @return JavaTransformableRDD
      */
    def drop(columnIndex: Int): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.drop(columnIndex).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaRDD[String] containing the duplicate values at the specified column
      *
      * @param columnIndex Column where to look for duplicates
      * @return JavaRDD[String]
      */
    def duplicatesAt(columnIndex: Int): JavaRDD[String] = tRDD.duplicatesAt(columnIndex).toJavaRDD()

    /**
      * Zips the other JavaTransformableRDD with this TransformableRDD and
      * returns a new JavaTransformableRDD with current file format.
      * Both the JavaTransformableRDD must have same number of records
      *
      * @param other Other JavaTransformableRDD from where the columns will be added to this JavaTransformableRDD
      * @return JavaTransformableRDD
      */
    def addColumnsFrom(other: JavaTransformableRDD): JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.addColumnsFrom(other.tRDD).toJavaRDD(), fileType)
    }

    /**
      * Returns a new JavaTransformableRDD by replacing the @cluster's text with specified @newValue
      *
      * @param cluster     Cluster of similar values to be replaced
      * @param newValue    Value that will be used to replace all the cluster value
      * @param columnIndex Column index
      * @return JavaTransformableRDD
      */
    def replaceValues(cluster: JavaCluster, newValue: String, columnIndex: Int): JavaTransformableRDD = {
        val replacedRDD: JavaRDD[String] = tRDD.replaceValues(cluster.scalaCluster, newValue, columnIndex).toJavaRDD()
        new JavaTransformableRDD(replacedRDD, fileType)
    }

    /**
      * Returns a new JavaRDD containing the unique elements in the specified column
      *
      * @param columnIndex Column Index
      * @return JavaRDD[String]
      */
    def unique(columnIndex: Int): JavaRDD[String] = tRDD.unique(columnIndex).toJavaRDD()

    /**
      * Returns a JavaDoubleRDD which is a product of the values in @firstColumn and @secondColumn
      *
      * @param firstColumn  First Column Index
      * @param secondColumn Second Column Index
      * @return JavaDoubleRDD
      */
    def multiplyColumns(firstColumn: Int, secondColumn: Int): JavaDoubleRDD = {
        new JavaDoubleRDD(tRDD.multiplyColumns(firstColumn, secondColumn))
    }

    /**
      * Returns a JavaDoubleRdd of given column index
      *
      * @param columnIndex Column index
      * @return JavaDoubleRDD
      */
    def toDoubleRDD(columnIndex: Int): JavaDoubleRDD = {
        new JavaDoubleRDD(tRDD.toDoubleRDD(columnIndex))
    }
}
