package org.apache.prepbuddy.facets;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class FacetUtilsTest extends SparkTestCase{
    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        FacetUtils facetUtils = new FacetUtils();
        TextFacets textFacets = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);
        assertEquals(2, textFacets.count());
    }

    @Test
    public void _TextFacets_highestShouldGiveOneHighestPairIfOnlyOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        FacetUtils facetUtils = new FacetUtils();
        TextFacets textFaceted = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);

        Tuple2<String, Integer> expected = new Tuple2<>("A", 3);
        List<Tuple2> listOfHighest = textFaceted.highest();

        assertEquals(1, listOfHighest.size());

        Tuple2 actual = listOfHighest.get(0);
        assertEquals(2,textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_highestShouldGiveListOfHighestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","X,P"));
        FacetUtils facetUtils = new FacetUtils();

        TextFacets textFaceted = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);
        List<Tuple2> listOfHighest = textFaceted.highest();

        assertEquals(2, listOfHighest.size());
        assertEquals(2,textFaceted.count());

        Tuple2<String, Integer> expected1 = new Tuple2<>("A", 3);
        Tuple2<String, Integer> expected2 = new Tuple2<>("X", 3);

        assertTrue(listOfHighest.contains(expected1));
        assertTrue(listOfHighest.contains(expected2));
    }
    @Test
    public void _TextFacet_lowestShouldGiveOnePairInListIfOnlyOneLowestPairIsFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        FacetUtils facetUtils = new FacetUtils();
        TextFacets textFaceted = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> listOfLowest = textFaceted.lowest();

        assertEquals(1, listOfLowest.size());

        Tuple2<String, Integer> expected = new Tuple2<>("X", 2);
        Tuple2 actual = listOfLowest.get(0);

        assertEquals(2, textFaceted.count());
        assertEquals(expected, actual);
    }

    @Test
    public void _TextFacet_lowestShouldGiveListOfLowestPairsIfMoreThanOnePairFound() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","Q,E","Q,R"));
        FacetUtils facetUtils = new FacetUtils();
        TextFacets textFaceted = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> listOfLowest = textFaceted.lowest();
        assertEquals(3, textFaceted.count());
        assertEquals(2, listOfLowest.size());

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);

        assertTrue(listOfLowest.contains(expected1));
        assertTrue(listOfLowest.contains(expected2));

    }
    @Test
    public void _TextFacet_getFacetsBetweenShouldGiveListOfFacetedPairInGivenRange() throws Exception {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E","Q,E","Q,R","W,E"));
        FacetUtils facetUtils = new FacetUtils();
        TextFacets textFaceted = facetUtils.listTextFacets(initialDataset, 0, FileType.CSV);

        List<Tuple2> facetedPair = textFaceted.getFacetsBetween(2,3);

        assertEquals(4, textFaceted.count());
        assertEquals(3, facetedPair.size());

        Tuple2<String, Integer> expected1 = new Tuple2<>("X", 2);
        Tuple2<String, Integer> expected2 = new Tuple2<>("Q", 2);
        Tuple2<String, Integer> expected3 = new Tuple2<>("A", 3);

        assertTrue(facetedPair.contains(expected1));
        assertTrue(facetedPair.contains(expected2));
        assertTrue(facetedPair.contains(expected3));



    }
}