package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.imputations.*;
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD;
import org.apache.datacommons.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaImputationTest extends JavaSparkTestCase {
    @Test
    public void shouldImputeTheValueWithTheMean() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,20,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new MeanSubstitution());
        List<String> listOfRecord = imputed.collect();
        assertEquals(4, listOfRecord.size());
        String expected = "07641036117,07371326239,Incoming,21.0,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheMeanApprox() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,20,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new ApproxMeanSubstitution());
        List<String> listOfRecord = imputed.collect();
        String expected = "07641036117,07371326239,Incoming,21.0,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheMostOccurredValue() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,31,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new ModeSubstitution());
        List<String> listOfRecord = imputed.collect();
        String expected = "07641036117,07371326239,Incoming,31,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheRegression() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "60,3.1", "61,3.6", "62,3.8", "63,4", "65,4.1", "64,"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(1, new UnivariateLinearRegressionSubstitution(0));
        List<String> listOfRecord = imputed.collect();

        String expected = "64,4.06";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheRegressionTest2() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "3.4,5.67", "3.9,4.81", "2.6,4.93", "1.9,6.21", "2.2,6.83", "3.3,5.61", "1.7,5.45", "2.4,4.94", "2.8,5.73",
                "3.6,"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(1, new UnivariateLinearRegressionSubstitution(0));
        List<String> listOfRecord = imputed.collect();

        String expected = "3.6,5.24";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheMissingValueByConsideringGivenHints() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "1,NULL,2,3,4",
                "2,N/A,23,21,23",
                "3,N/A,21,32,32",
                "4,-,2,3,4",
                "5,,54,32,54",
                "6,32,22,33,23"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);

        List<String> imputedRDD = initialRDD.impute(1, new ImputationStrategy() {
            @Override
            public void prepareSubstitute(TransformableRDD rdd, int missingDataColumn) {

            }

            @Override
            public String handleMissingData(RowRecord record) {
                return "X";
            }
        }, Arrays.asList("N/A", "-", "NA", "NULL")).collect();

        assertTrue(imputedRDD.contains("1,X,2,3,4"));
        assertTrue(imputedRDD.contains("2,X,23,21,23"));
        assertTrue(imputedRDD.contains("3,X,21,32,32"));
        assertTrue(imputedRDD.contains("4,X,2,3,4"));
        assertTrue(imputedRDD.contains("5,X,54,32,54"));
        assertTrue(imputedRDD.contains("6,32,22,33,23"));
    }

    @Test
    public void shouldImputeMissingValuesWithTheNaiveBayes() {
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
                "unknown,new,short,work,reads",
                "unknown,new,long,work,",
                "unknown,follow Up,long,home,",
                "unknown,follow Up,short,home,"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        int[] ints = {1, 2};
        JavaTransformableRDD imputed = initialRDD.impute(4, new NaiveBayesSubstitution(ints));
        List<String> listOfRecord = imputed.collect();

        String expected = "unknown,new,long,work,skips";
        assertTrue(listOfRecord.contains(expected));
        String expected1 = "unknown,new,long,work,skips";
        String expected2 = "unknown,follow Up,short,home,reads";

        assertTrue(listOfRecord.contains(expected1));
        assertTrue(listOfRecord.contains(expected2));
    }
}
