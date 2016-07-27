package org.apache.datacommons.prepbuddy.clusterers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.api.java.JavaRDD

class FacetTests extends SparkTestCase {

    test("should drop the specified column from the given rdd") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val highest = textFaceted.highest.head

        assert(textFaceted.highest.length == 1)

        val expected = ("A", 3)
        assert(expected == highest)
    }

    test("highest() should give one pair in List if only one highest pair is found") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "X,P")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val listOfHighest = textFaceted.highest

        assert(2 == listOfHighest.length)
        assert(2 == textFaceted.count)

        assert(listOfHighest.contains(("A", 3)))
        assert(listOfHighest.contains(("X", 3)))
    }

    test("lowest() should give list of lowest pairs if more than one pair found") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val listOfLowest = textFaceted.lowest

        assert(3 == textFaceted.count)
        assert(2 == listOfLowest.length)
        assert(listOfLowest.contains(("X", 2)))
        assert(listOfLowest.contains(("Q", 2)))
    }

    test("getFacetsBetween() Should Give List Of Faceted Pair In Given Range") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val facetedPair = textFaceted.getFacetsBetween(2, 3)

        assert(4 == textFaceted.count)
        assert(3 == facetedPair.length)
        assert(facetedPair.contains(("X", 2)))
        assert(facetedPair.contains(("Q", 2)))
        assert(facetedPair.contains(("A", 3)))
    }

    test("cardinalValues() Should return cardinal values for the text facets") {
        val dataSet = Array("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val cardinalValues: Array[String] = textFaceted.cardinalValues

        assert(cardinalValues sameElements Array("Q", "A", "X", "W"))
    }

    test("highest() should ignore the empty string in list while faceting") {
        val dataSet = Array("X,Y", " ,B", " ,Z", " ,Q", ",E", "X,P")
        val initialRDD: JavaRDD[String] = sparkContext.parallelize(dataSet)
        val transformableRDD: TransformableRDD = new TransformableRDD(initialRDD)

        val textFaceted: TextFacets = transformableRDD.listFacets(0)
        val listOfHighest = textFaceted.highest

        assert(1 == listOfHighest.length)
        assert(2 == textFaceted.count)
        assert(listOfHighest.contains(("X", 2)))
    }
}
