package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.smoothers.SimpleMovingAverageMethod;
import org.apache.datacommons.prepbuddy.smoothers.WeightedMovingAverageMethod;
import org.apache.datacommons.prepbuddy.smoothers.Weights;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaSmoothingTest extends JavaSparkTestCase {
    @Test
    public void shouldSmoothDataSetBySimpleMovingAverage() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"
        ), 3);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        SimpleMovingAverageMethod movingAverage = new SimpleMovingAverageMethod(3);
        JavaDoubleRDD smoothed = transformableRDD.smooth(0, movingAverage);
        List<Double> listOfRecord = smoothed.collect();

        assertEquals(10, listOfRecord.size());
        assertTrue(listOfRecord.contains(4.0));
        assertTrue(listOfRecord.contains(5.0));
        assertTrue(listOfRecord.contains(6.0));
        assertTrue(listOfRecord.contains(7.0));
        assertTrue(listOfRecord.contains(8.0));
        assertTrue(listOfRecord.contains(9.0));
        assertTrue(listOfRecord.contains(10.0));
        assertTrue(listOfRecord.contains(11.0));
        assertTrue(listOfRecord.contains(12.0));
        assertTrue(listOfRecord.contains(13.0));
    }

    @Test
    public void smoothShouldSmoothDataUsingSimpleMovingAverages() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "52,3,53", "23,4,64", "23,5,64", "23,6,64", "23,7,64", "23,8,64", "23,9,64"
        ), 3);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaDoubleRDD transformed = transformableRDD.smooth(1, new SimpleMovingAverageMethod(3));

        Double excepted = 4.0;
        assertEquals(excepted, transformed.first());

        List<Double> expectedList = Arrays.asList(4.0, 5.0, 6.0, 7.0, 8.0);
        assertEquals(expectedList, transformed.collect());
    }

    @Test
    public void smoothShouldSmoothDataUsingWeightedMovingAverages() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "52,10,53", "23,12,64", "23,16,64", "23,13,64", "23,17,64", "23,19,64", "23,15,64"
        ), 3);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        Weights weights = new Weights(3);
        weights.add(0.166);
        weights.add(0.333);
        weights.add(0.5);
        JavaDoubleRDD transformed = transformableRDD.smooth(1, new WeightedMovingAverageMethod(3, weights));

        Double expected = 13.66;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(transformed.first()));
        assertEquals(expected, actual);

        List<Double> movingAverages = transformed.collect();
        Double secondAverage = movingAverages.get(1);
        Double thirdAverage = movingAverages.get(2);
        Double fourthAverage = movingAverages.get(3);

        thirdAverage = Double.parseDouble(new DecimalFormat("##.##").format(thirdAverage));
        fourthAverage = Double.parseDouble(new DecimalFormat("##.##").format(fourthAverage));

        assertTrue(secondAverage.equals(13.82));
        assertTrue(thirdAverage.equals(15.49));
        assertTrue(fourthAverage.equals(17.32));
    }
}
