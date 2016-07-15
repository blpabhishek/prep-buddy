package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.clusterers.TextFacets
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.{NumberMap, PivotTable, Probability, RowRecord}


class NaiveBayesSubstitution(independentColumnIndexes: Int*) extends strategy {
    private var probs: PivotTable[Probability] = null
    private var permissibleValues: Array[String] = null

    override def prepareSubstitute(rdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val trainingSet: TransformableRDD = rdd.removeRows(_.hasEmptyColumn)
        val facets: TextFacets = trainingSet.listFacets(missingDataColumn)
        permissibleValues = facets.cardinalValues
        val rowKeys: Array[(String, Int)] = facets.rdd.collect()
        val frequencyTable: PivotTable[Integer] = {
            rdd.pivotByCount(missingDataColumn, independentColumnIndexes)
        }
        val totalRows: Long = trainingSet.count()

        probs = frequencyTable.transform((eachValue) => {
            val oneProbability: Double = eachValue.toString.toDouble / totalRows
            new Probability(oneProbability)
        }, new Probability(0)).asInstanceOf[PivotTable[Probability]]

        rowKeys.foreach((each) => {
            val probability: Probability = new Probability(each._2.toString.toDouble / totalRows)
            probs.addEntry(each._1, each._1, probability)
        })

    }

    override def handleMissingData(record: RowRecord): String = {
        val numbers: NumberMap = new NumberMap()
        for (permissibleValue <- permissibleValues) {
            var naiveProbability: Probability = new Probability(1)
            val permissibleProb: Probability = probs.valueAt(permissibleValue, permissibleValue)
            independentColumnIndexes.foreach((columnIndex) => {
                val columnValue: String = record.valueAt(columnIndex).trim
                val probability: Probability = probs.valueAt(permissibleValue, columnValue)
                naiveProbability = naiveProbability.multiply(probability.divide(permissibleProb))
            })
            naiveProbability = naiveProbability.multiply(permissibleProb)
            numbers.put(permissibleValue, naiveProbability.doubleValue)
        }
        numbers.keyWithHighestValue
    }
}

