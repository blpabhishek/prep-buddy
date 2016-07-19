package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.utils.PivotTable;
import org.apache.datacommons.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaTransformableRDDTest extends JavaSparkTestCase {

    @Test
    public void shouldBeAbleToDeduplicate() {
        JavaRDD<String> numbers = javaSparkContext.parallelize(Arrays.asList(
                "One,Two,Three",
                "One,Two,Three",
                "One,Two,Three",
                "Ten,Eleven,Twelve"
        ));
        JavaTransformableRDD javaTransformableRDD = new JavaTransformableRDD(numbers, FileType.CSV);
        JavaTransformableRDD javaRdd = javaTransformableRDD.deduplicate();
        assertEquals(2, javaRdd.count());
    }

    @Test
    public void shouldBeAbleToRemoveRowsAccordingToPredicate() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        JavaTransformableRDD purged = initialRDD.removeRows(new RowPurger() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(1).equals("YY");
            }
        });
        assertEquals(2, purged.count());
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsByConsideringTheGivenColumnsAsPrimaryKey() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        List<Integer> primaryKeyColumns = Arrays.asList(0, 3);
        JavaTransformableRDD deduplicatedRDD = initialRDD.deduplicate(primaryKeyColumns);
        assertEquals(3, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsByConsideringTheGivenColumnsAsPrimaryKey() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaTransformableRDD duplicates = initialRDD.duplicates(Arrays.asList(0, 3));
        assertEquals(2, duplicates.count());

        List<String> listOfDuplicates = duplicates.collect();
        assertTrue(listOfDuplicates.contains("John,Male,USA,12343"));
        assertTrue(listOfDuplicates.contains("John,Male,India,12343"));
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsByConsideringAllTheColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12345"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset);
        JavaTransformableRDD duplicates = initialRDD.duplicates();
        assertEquals(1, duplicates.count());
    }

    @Test
    public void shouldBeAbleToSelectAColumnFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaRDD<String> columnValues = initialRDD.select(0);
        List<String> expectedValues = Arrays.asList("Smith", "John", "John", "Smith");
        assertEquals(expectedValues, columnValues.collect());
    }

    @Test
    public void shouldBeAbleToSelectSomeFeaturesFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        JavaRDD<String> selectedFeatures = initialRDD.select(0, 1);
        List<String> expected = Arrays.asList("Smith,Male", "John,Male", "John,Male", "Smith,Male");
        assertEquals(expected, selectedFeatures.collect());
    }

    @Test
    public void sizeShouldGiveTheNumberOfColumnInRdd() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        int size = initialRDD.numberOfColumns();
        assertEquals(4, size);
    }

    @Test
    public void pivotByCountsShouldGiveCountsWithGivenColumns() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "known,new,long,home,skips",
                "unknown,new,short,work,reads",
                "unknown,follow Up,long,work,skips",
                "known,follow Up,long,home,skips",
                "known,new,short,home,reads",
                "known,follow Up,long,work,skips",
                "unknown,follow Up,short,work,skips",
                "unknown,new,short,work,reads",
                "known,follow Up,long,home,skips",
                "known,new,long,work,skips",
                "unknown,follow Up,short,home,skips",
                "known,new,long,work,skips",
                "known,follow Up,short,home,reads",
                "known,new,short,work,reads",
                "known,new,short,home,reads",
                "known,follow Up,short,work,reads",
                "known,new,short,home,reads",
                "unknown,new,short,work,reads"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet);
        PivotTable<Integer> pivotTable = initialRDD.pivotByCount(4, Arrays.asList(0, 1, 2, 3));
        int valueAtSkipsLong = pivotTable.valueAt("skips", "long");
        assertEquals(7, valueAtSkipsLong);

        int valueAtReadsLong = pivotTable.valueAt("reads", "long");
        assertEquals(0, valueAtReadsLong);
    }

    @Test
    public void shouldMarkRowThatSatisfyTheMarkerPredicate() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset);

        JavaTransformableRDD marked = initialRDD.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return row.valueAt(2).trim().isEmpty();
            }
        });
        List<String> expectedList = Arrays.asList("X,Y,,*", "XX,YY,ZZ,");
        assertEquals(expectedList, marked.collect());
    }

    @Test
    public void shouldMapOnMarkedRow() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset);

        JavaTransformableRDD marked = initialRDD.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return row.valueAt(2).trim().isEmpty();
            }
        });
        JavaRDD<String> mappedByFlagRdd = marked.mapByFlag("*", 3, new Function<String, String>() {
            @Override
            public String call(String oneRow) throws Exception {
                return oneRow + ",Mapped";
            }
        });
        List<String> expectedList = Arrays.asList("X,Y,,*,Mapped", "XX,YY,ZZ,");
        assertEquals(expectedList, mappedByFlagRdd.collect());
    }

    @Test
    public void shouldDropTheGivenColumnIndexFromDataset() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset);

        JavaTransformableRDD marked = initialRDD.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return row.valueAt(2).trim().isEmpty();
            }
        });
        JavaTransformableRDD droppedOneColumnRdd = marked.drop(3);
        List<String> expectedList = Arrays.asList("X,Y,", "XX,YY,ZZ");
        assertEquals(expectedList, droppedOneColumnRdd.collect());
    }
}
