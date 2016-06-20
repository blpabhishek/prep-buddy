package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AnalyzableRDDTest extends SparkTestCase {

    @Test
    public void shouldAnalyzeDataQualityOfAColumn() throws Exception {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"
        ));

        AnalyzableRDD inputRDD = new AnalyzableRDD(initialDataset);
        AnalysisResult report = inputRDD.analyzeColumn(3);
        assertEquals(DataType.INTEGER, report.dataType());


//        assertEquals(new Double(10), report.percentageOfMissingValues());
//        assertEquals(new Double(10), report.percentageOfInconsistentValues());
//        assertEquals(new Double(10), report.percentageOfDuplicateValues());
//        Range range = report.rangeOfValues();
//        assertNotNull(range);
//        DataShape dataShape = report.shapeOfData();
//        Double skewness = report.skewness();
//        Double kurtosis = report.kurtosis();
//        assertEquals(DataShape.NORMAL, dataShape);
//        Outliers outliers = report.outliers();
//        assertNotNull(outliers);
    }
}
