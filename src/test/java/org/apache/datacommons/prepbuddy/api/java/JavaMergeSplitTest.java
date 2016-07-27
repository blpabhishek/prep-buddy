package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class JavaMergeSplitTest extends JavaSparkTestCase {

    @Test
    public void shouldMergeGivenColumnsWithTheSeparator() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes, "_", false).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("21,John_Canada_Male"));
        assertTrue(result.contains("23,Larry_USA_Male"));
    }

    @Test
    public void shouldMergeColumnsBySpaceIfNoSeparatorIsGiven() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("21,John Canada Male"));
        assertTrue(result.contains("23,Larry USA Male"));
    }

    @Test
    public void shouldMergeColumnsByKeepingTheOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes, "_", true).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,John_Canada_Male"));
        assertTrue(result.contains("Larry,Male,23,USA,Larry_USA_Male"));
    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueAccordingToTheGivenLengthsByRemovingOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,+914382313832,Canada",
                "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA",
                "Fiona, Female,18,+891015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByFieldLength(3, Arrays.asList(3, 10), false).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,UK,+01,5314343462"));

    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueAccordingToTheGivenLengthsByKeepingOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,+914382313832,Canada",
                "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA",
                "Fiona, Female,18,+891015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByFieldLength(3, Arrays.asList(3, 10), true).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,+914382313832,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,+015314343462,UK,+01,5314343462"));
    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueByDelimiterWhileKeepingTheOriginalColumn() {
        List<String> data = Arrays.asList(
                "John,Male,21,+91-4382313832,Canada",
                "Smith, Male, 30,+01-5314343462, UK",
                "Larry, Male, 23,+00-9815432975, USA",
                "Fiona, Female,18,+89-1015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByDelimiter(3, "-", true).collect();
        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,+91-4382313832,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,+01-5314343462,UK,+01,5314343462"));
    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueByDelimiterWhileRemovingTheOriginalColumn() {
        List<String> data = Arrays.asList(
                "John,Male,21,+91-4382313832,Canada",
                "Smith, Male, 30,+01-5314343462, UK",
                "Larry, Male, 23,+00-9815432975, USA",
                "Fiona, Female,18,+89-1015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByDelimiter(3, "-", false).collect();
        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,UK,+01,5314343462"));
    }
}
