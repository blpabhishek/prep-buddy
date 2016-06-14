package org.apache.prepbuddy.systemtests;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.datacleansers.imputation.ImputationStrategy;
import org.apache.prepbuddy.groupingops.Clusters;
import org.apache.prepbuddy.groupingops.SimpleFingerprintAlgorithm;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.normalizers.DecimalScalingNormalization;
import org.apache.prepbuddy.normalizers.MinMaxNormalizer;
import org.apache.prepbuddy.normalizers.ZScoreNormalization;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformers.*;
import org.apache.prepbuddy.typesystem.DataType;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

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

        TransformableRDD unflaged = mapedRDD.dropColumn(3);

        assertEquals("Star X,Y,", unflaged.first());
        TransformableRDD imputedRDD = purged.impute(2, new ImputationStrategy() {
            @Override
            public void prepareSubstitute(TransformableRDD rdd, int missingDataColumn) {

            }

            @Override
            public String handleMissingData(RowRecord record) {
                return "Male";
            }
        });
        assertEquals("X,Y,Male", imputedRDD.first());


        TransformableRDD replacedRdd = imputedRDD.replace(2, new ReplacementFunction() {
            @Override
            public String replace(RowRecord record) {
                return "1";
            }
        });
        assertEquals(1, replacedRdd.count());
        assertEquals("X,Y,1", replacedRdd.first());
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

        TransformableRDD splitColumnRDD = initialRDD.splitColumn(new SplitPlan(0, " ", false));
        assertEquals("FirstName,LastName,MiddleName,850", splitColumnRDD.first());

        TransformableRDD splitColumnRDDByKeepingColumn = initialRDD.splitColumn(new SplitPlan(0, " ", true));
        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", splitColumnRDDByKeepingColumn.first());

        TransformableRDD splitColumnByLengthRDD = initialRDD.splitColumn(new SplitPlan(0, Arrays.asList(9, 9), false));
        assertEquals("FirstName, LastName,850", splitColumnByLengthRDD.first());

        TransformableRDD splitColumnByLengthRDDByKeepingColumn = initialRDD.splitColumn(new SplitPlan(0, Arrays.asList(9, 9), true));
        assertEquals("FirstName LastName MiddleName,FirstName, LastName,850", splitColumnByLengthRDDByKeepingColumn.first());
    }

    @Test
    public void shouldBeAbleToJoinMultipleColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Collections.singletonList("FirstName,LastName,732,MiddleName"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);

        TransformableRDD joinedColumnRDD = initialRDD.mergeColumns(new MergePlan(Arrays.asList(3, 1, 0), false, "_"));
        assertEquals("732,MiddleName_LastName_FirstName", joinedColumnRDD.first());

        TransformableRDD joinedColumnRDDByKeepingOriginals = initialRDD.mergeColumns(new MergePlan(Arrays.asList(3, 1, 0), true, "_"));
        assertEquals("FirstName,LastName,732,MiddleName,MiddleName_LastName_FirstName", joinedColumnRDDByKeepingOriginals.first());

        TransformableRDD joinedColumnWithDefault = initialRDD.mergeColumns(new MergePlan(Arrays.asList(3, 1, 0), false));
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

        TransformableRDD duplicatesRDD = initialRDD.getDuplicates();
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

        TransformableRDD imputedRDD = removedRowsRDD.impute(1, new ImputationStrategy() {
            @Override
            public void prepareSubstitute(TransformableRDD rdd, int missingDataColumn) {

            }

            @Override
            public String handleMissingData(RowRecord record) {
                return "1234567890";
            }
        });
        assertTrue(imputedRDD.collect().contains("07434677419,1234567890,Incoming,211,Wed Sep 15 19:17:44 +0100 2010"));

        TransformableRDD replacedRDD = imputedRDD.replace(3, new ReplacementFunction() {
            @Override
            public String replace(RowRecord record) {
                String columnVal = record.valueAt(3);
                return columnVal.equals("0") ? "Zero" : columnVal;
            }
        });
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


        TransformableRDD splitBySpaceRDD = mappedFlagRDD.splitColumn(new SplitPlan(4, " ", false));
        assertTrue(splitBySpaceRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,Mon,Feb,11,07:18:23,+0000,1980,"));

        TransformableRDD mergedRDD = splitBySpaceRDD.mergeColumns(new MergePlan(Arrays.asList(4, 5, 6, 9, 7, 8), false));
        assertTrue(mergedRDD.collect().contains("07641036117,01666472054,Outgoing,Zero,,Mon Feb 11 1980 07:18:23 +0000"));

        TransformableRDD splitByLengthRDD = mergedRDD.splitColumn(new SplitPlan(5, Arrays.asList(15, 9), false));
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
        DataType dataType = initialRDD.inferType(1);
        assertEquals(dataType, DataType.MOBILE_NUMBER);
    }

    @Test
    public void shouldNormalizeRecordsUsingMinMaxNormalizer() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        TransformableRDD finalRDD = initialRDD.normalize(3, new MinMaxNormalizer());
        List<String> normalizedDurations = finalRDD.select(3).collect();
        List<String> expected = Arrays.asList("1.0", "0.0", "0.2132701421800948", "0.2132701421800948", "0.05687203791469194");
        assertEquals(expected, normalizedDurations);
    }

    @Test
    public void shouldNormalizeRecordsUsingZScoreNormalization() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        TransformableRDD finalRDD = initialRDD.normalize(3, new ZScoreNormalization());
        List<String> normalizedDurations = finalRDD.select(3).collect();
        List<String> expected = Arrays.asList("1.944528306701421", "-0.8202659838241843", "-0.2306179123850742", "-0.2306179123850742", "-0.6630264981070882");
        assertEquals(expected, normalizedDurations);
    }

    @Test
    public void shouldNormalizeRecordsUsingDecimalScalingNormalization() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        TransformableRDD finalRDD = initialRDD.normalize(3, new DecimalScalingNormalization());
        List<String> normalizedDurations = finalRDD.select(3).collect();
        List<String> expected = Arrays.asList("2.11", "0.0", "0.45", "0.45", "0.12");
        assertEquals(expected, normalizedDurations);
    }

}
