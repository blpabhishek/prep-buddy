package org.apache.prepbuddy.transformations.deduplication;

import org.apache.log4j.Level;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import static org.apache.log4j.Logger.getLogger;
import static org.junit.Assert.assertEquals;

public class DeduplicationTransformationTest extends SparkTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        getLogger("org").setLevel(Level.OFF);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void shouldGiveAnRddWithNoDuplicateRows() throws NoSuchAlgorithmException {
        JavaRDD<String> csvInput = context.parallelize(
                Arrays.asList(
                        "07110730864,07209670163,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010",
                        "07110730864,07209670163,Outgoing,0,Fri Sep 10 06:04:43 +0100 2010",
                        "07607124303,2327,Outgoing,0,Mon Sep 13 13:54:40 +0100 2010",
                        "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010",
                        "07784425582,07981267897,Incoming,474,Thu SEP 09 18:44:34 +0100 2010",
                        "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010",
                        "07607124303,07167454533,OutGoing,2,Tue Sep 14 14:48:37 +0100 2010",
                        "07110730864,07209670163,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010",
                        "07607124303,07167454533,Outgoing,2,Tue Sep 14 14:48:37 +0100 2010",
                        "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010"
                )
        );
        DeduplicationTransformation deduplicationTransformation = new DeduplicationTransformation();
        List results = deduplicationTransformation.apply(csvInput).collect();

        assertEquals(5, results.size());
    }

    @Test
    public void shouldGiveAnRddWithNoDuplicateForOtherLanguageTextAlso() throws NoSuchAlgorithmException {
        JavaRDD<String> csvInput = context.parallelize(
                Arrays.asList(
                        "07607124303,2327，性格外向，0，周一9月13日十三点54分40秒+01002010",
                        "07110730864,07209670163，性格外向，0，周五9月10日6时04分43秒+01002010",
                        "07784425582,07981267897，传入，474，周四9月9日18时44分34秒+01002010",
                        "07607124303,2327，性格外向，0，周一9月13日十三点54分40秒+01002010",
                        "07110730864,07209670163，性格外向，0，周五9月10日6时04分43秒+01002010",
                        "07607124303,2327，性格外向，0，周一9月13日十三点54分40秒+01002010",
                        "07110730864,07209670163，性格外向，0，周五9月10日6时04分43秒+01002010",
                        "07784425582,07981267897，传入，474，周四9月9日18时44分34秒+01002010"
                )
        );
        DeduplicationTransformation deduplicationTransformation = new DeduplicationTransformation();
        List result = deduplicationTransformation.apply(csvInput).collect();

        assertEquals(3, result.size());
    }
}