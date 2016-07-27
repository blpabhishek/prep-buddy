package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.clusterers.TextFacets;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaFacetTest extends JavaSparkTestCase {
    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFacets = initialRDD.listFacets(0);
        assertEquals(2, textFacets.count());
    }

    @Test
    public void _TextFacets_highestShouldGiveOneHighestPairIfOnlyOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);

        Tuple2<String, Integer> expected = new Tuple2<>("A", 3);
        Tuple2[] listOfHighest = textFaceted.highest();

        assertEquals(1, listOfHighest.length);

        Tuple2 actual = listOfHighest[0];
        assertEquals(2, textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_highestShouldGiveListOfHighestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E", "X,P"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);
        Tuple2[] listOfHighest = textFaceted.highest();

        assertEquals(2, listOfHighest.length);
        assertEquals(2, textFaceted.count());

        Tuple2<String, Integer> expected1 = new Tuple2<>("A", 3);
        Tuple2<String, Integer> expected2 = new Tuple2<>("X", 3);

        assertTrue(listOfHighest[0].equals(expected1));
        assertTrue(listOfHighest[1].equals(expected2));
    }

    @Test
    public void _TextFacet_lowestShouldGiveOnePairInListIfOnlyOneLowestPairIsFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);

        Tuple2[] listOfLowest = textFaceted.lowest();

        assertEquals(1, listOfLowest.length);

        Tuple2<String, Integer> expected = new Tuple2<>("X", 2);
        Tuple2 actual = listOfLowest[0];

        assertEquals(2, textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_lowestShouldGiveListOfLowestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);

        Tuple2[] listOfLowest = textFaceted.lowest();
        assertEquals(3, textFaceted.count());
        assertEquals(2, listOfLowest.length);

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);

        assertTrue(listOfLowest[0].equals(expected2));
        assertTrue(listOfLowest[1].equals(expected1));
    }

    @Test
    public void _TextFacet_getFacetsBetweenShouldGiveListOfFacetedPairInGivenRange() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E", "Q,E", "Q,R", "W,E"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFaceted = initialRDD.listFacets(0);

        Tuple2[] facetedPair = textFaceted.getFacetsBetween(2, 3);

        assertEquals(4, textFaceted.count());
        assertEquals(3, facetedPair.length);

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);
        Tuple2<String, Integer> expected3 = new Tuple2<>("A", 3);

        assertTrue(facetedPair[0].equals(expected2));
        assertTrue(facetedPair[1].equals(expected3));
        assertTrue(facetedPair[2].equals(expected1));
    }

    @Test
    public void _TextFacet_getFacetsShouldGiveListOfFacetedForTheProvideColumns() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("A,B", "A,B", "A,B", "A,B"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        TextFacets textFacets = initialRDD.listFacets(Arrays.asList(0, 1));

        assertEquals(1, textFacets.count());
    }
}
