package org.apache.prepbuddy.transformations.preprocessor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TrimmerTest {

    private JavaRDD<String> inputData;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Deduplication Transformation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        inputData = sc.parallelize(
                Arrays.asList(
                        "  07110730864,07209670163,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010 ",
                        "    07110730864,07209670163,Outgoing,0,Fri Sep 10 06:04:43 +0100 2010",
                        "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010",
                        "07607124303,2327,Outgoing,0,Mon Sep 13 13:54:40 +0100 2010   ",
                        "   07607124303,07167454533,Outgoing,2,Tue Sep 14 14:48:37 +0100 2010   "
                )
        );
    }

    @Test
    public void apply_trims_both_end_of_each_line_if_some_space_exists() {
        List<String> result = new Trimmer(",").apply(inputData).collect();

        List<String> expected = Arrays.asList(
                "07110730864,07209670163,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010",
                "07110730864,07209670163,Outgoing,0,Fri Sep 10 06:04:43 +0100 2010",
                "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010",
                "07607124303,2327,Outgoing,0,Mon Sep 13 13:54:40 +0100 2010",
                "07607124303,07167454533,Outgoing,2,Tue Sep 14 14:48:37 +0100 2010"
        );

        int counter = 0;
        for (String record : result) {
            assertEquals(expected.get(counter++), record);
        }
    }
}