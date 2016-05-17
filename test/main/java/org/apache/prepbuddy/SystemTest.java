package org.apache.prepbuddy;

import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.filetypes.Type;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.SimpleFingerprintAlgorithm;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformation.ColumnMerger;
import org.apache.prepbuddy.transformation.MarkerPredicate;
import org.apache.prepbuddy.transformation.SplitByDelimiter;
import org.apache.prepbuddy.transformation.SplitByFieldLength;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class SystemTest extends SparkTestCase {

    @Test
    public void shouldExecuteASeriesOfTransformsOnADataset() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicated = initialRDD.deduplicate();
        assertEquals(2, deduplicated.count());

        TransformableRDD purged = deduplicated.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(1).equals("YY");
            }
        });
        assertEquals(1, purged.count());

        TransformableRDD marked = purged.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return true;
            }
        });

        assertEquals(1, marked.count());
        assertEquals("X,Y,,*", marked.first());

        TransformableRDD mapedRDD = marked.mapByFlag("*", 3, new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                return "Star " + row;
            }
        });

        assertEquals(1, mapedRDD.count());
        assertEquals("Star X,Y,,*", mapedRDD.first());

        TransformableRDD imputedRDD = purged.impute(2, new MissingDataHandler() {
            @Override
            public String handleMissingData(RowRecord record) {
                return "Male";
            }
        });
        assertEquals("X,Y,Male", imputedRDD.first());

        TransformableRDD numericRDD = imputedRDD.replace(2, new Replacement<>("Male", 0), new Replacement<>("Female", 1));

        assertEquals(1, numericRDD.count());
        assertEquals("X,Y,0", numericRDD.first());
    }

    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z", "A,Q", "A,E"));
        TransformableRDD rdd = new TransformableRDD(initialDataset);
        TextFacets facets = rdd.listFacets(0);
        assertEquals(2, facets.count());
    }

    @Test
    public void shouldBeAbleToSplitTheGivenColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Collections.singletonList("FirstName LastName MiddleName,850"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD splitColumnRDD = initialRDD.splitColumn(0, new SplitByDelimiter(" ", false));
        assertEquals("FirstName,LastName,MiddleName,850", splitColumnRDD.first());

        TransformableRDD splitColumnRDDByKeepingColumn = initialRDD.splitColumn(0, new SplitByDelimiter(" ", true));
        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", splitColumnRDDByKeepingColumn.first());

        TransformableRDD splitColumnByLengthRDD = initialRDD.splitColumn(0, new SplitByFieldLength(Arrays.asList(9, 9), false));
        assertEquals("FirstName, LastName,850", splitColumnByLengthRDD.first());

        TransformableRDD splitColumnByLengthRDDByKeepingColumn = initialRDD.splitColumn(0, new SplitByFieldLength(Arrays.asList(9, 9), true));
        assertEquals("FirstName LastName MiddleName,FirstName, LastName,850", splitColumnByLengthRDDByKeepingColumn.first());
    }

    @Test
    public void shouldBeAbleToJoinMultipleColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Collections.singletonList("FirstName,LastName,732,MiddleName"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD joinedColumnRDD = initialRDD.mergeColumns(new ColumnMerger(Arrays.asList(3, 1, 0), false, "_"));
        assertEquals("732,MiddleName_LastName_FirstName", joinedColumnRDD.first());

        TransformableRDD joinedColumnRDDByKeepingOriginals = initialRDD.mergeColumns(new ColumnMerger(Arrays.asList(3, 1, 0), true, "_"));
        assertEquals("FirstName,LastName,732,MiddleName,MiddleName_LastName_FirstName", joinedColumnRDDByKeepingOriginals.first());

        TransformableRDD joinedColumnWithDefault = initialRDD.mergeColumns(new ColumnMerger(Arrays.asList(3, 1, 0), false));
        assertEquals("732,MiddleName LastName FirstName", joinedColumnWithDefault.first());
    }

    @Test
    public void shouldTestAllTheFunctionalityByReadingAFile() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD deduplicateRDD = initialRDD.deduplicate();
        assertEquals(4, deduplicateRDD.count());

        TransformableRDD duplicatesRDD = initialRDD.duplicates();
        assertEquals(1, duplicatesRDD.count());
        assertEquals("07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980", duplicatesRDD.first());

        TransformableRDD removedRowsRDD = deduplicateRDD.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(2).equals("Missed");
            }
        });
        assertEquals(3, removedRowsRDD.count());
        assertFalse(removedRowsRDD.collect().contains("07641036117,07681546436,Missed,0,Mon Feb 11 08:04:42 +0000 1980"));

        TransformableRDD imputedRDD = removedRowsRDD.impute(1, new MissingDataHandler() {
            @Override
            public String handleMissingData(RowRecord record) {
                return "1234567890";
            }
        });
        assertTrue(imputedRDD.collect().contains("07434677419,1234567890,Incoming,211,Wed Sep 15 19:17:44 +0100 2010"));

        TransformableRDD replacedRDD = imputedRDD.replace(3, new Replacement("0", "Zero"));
        assertTrue(replacedRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,Mon Feb 11 07:18:23 +0000 1980"));
        assertTrue(imputedRDD.collect().contains("07434677419,1234567890,Incoming,211,Wed Sep 15 19:17:44 +0100 2010"));

        TransformableRDD flaggedRDD = replacedRDD.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return row.valueAt(2).equals("Incoming");
            }
        });
        assertTrue(flaggedRDD.collect().contains("07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980,*"));
        assertTrue(flaggedRDD.collect().contains("07434677419,1234567890,Incoming,211,Wed Sep 15 19:17:44 +0100 2010,*"));
        assertTrue(flaggedRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,Mon Feb 11 07:18:23 +0000 1980,"));

        TransformableRDD mappedFlagRDD = flaggedRDD.mapByFlag("*", 5, new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                return "PROFIT:" + row;
            }
        });
        assertTrue(mappedFlagRDD.collect().contains("PROFIT:07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980,*"));
        assertTrue(mappedFlagRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,Mon Feb 11 07:18:23 +0000 1980,"));


        TransformableRDD splitBySpaceRDD = mappedFlagRDD.splitColumn(4, new SplitByDelimiter(" ", false));
        assertTrue(splitBySpaceRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,Mon,Feb,11,07:18:23,+0000,1980,"));

        TransformableRDD mergedRDD = splitBySpaceRDD.mergeColumns(new ColumnMerger(Arrays.asList(4, 5, 6, 9, 7, 8), false));
        assertTrue(mergedRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,,Mon Feb 11 1980 07:18:23 +0000"));

        TransformableRDD splitByLengthRDD = mergedRDD.splitColumn(5, new SplitByFieldLength(Arrays.asList(15, 9), false));
        assertTrue(splitByLengthRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,,Mon Feb 11 1980, 07:18:23"));

        Clusters clustersBySimpleFingerprint = splitByLengthRDD.clusters(2, new SimpleFingerprintAlgorithm());
        assertEquals(2, clustersBySimpleFingerprint.getAllClusters().size());

        TextFacets facets = splitByLengthRDD.listFacets(2);
        assertEquals(1, facets.highest().size());
    }
    @Test
    public void shouldBeAbleToInferTheTypeOfADataSetColumn() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        Type type = initialRDD.inferType(2);
        assertEquals(type,Type.STRING);
    }
}
