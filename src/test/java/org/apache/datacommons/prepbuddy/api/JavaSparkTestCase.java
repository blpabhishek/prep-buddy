package org.apache.datacommons.prepbuddy.api;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

public class JavaSparkTestCase implements Serializable {
    protected transient JavaSparkContext javaSparkContext;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    @After
    public void tearDown() throws Exception {
        javaSparkContext.close();
    }
}
