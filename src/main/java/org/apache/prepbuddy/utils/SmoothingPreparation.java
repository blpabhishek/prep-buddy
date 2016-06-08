package org.apache.prepbuddy.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SmoothingPreparation implements Serializable {

    public static JavaRDD<String> prepare(JavaRDD<String> dataset, final int window) {
        JavaRDD<Tuple2<Integer, String>> duplicateRdd = dataset.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<Tuple2<Integer, String>>>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Integer index, Iterator<String> iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                ArrayList<Tuple2<Integer, String>> duplicates = new ArrayList<>();
                int count = 1;
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    Tuple2<Integer, String> tuple = new Tuple2<>(index, next);
                    list.add(tuple);
                    if (count < window) {
                        Tuple2<Integer, String> duplicateTuple = new Tuple2<>(index - 1, next);
                        duplicates.add(duplicateTuple);
                        count++;
                    }
                    duplicates.add(tuple);
                }
                if (index == 0)
                    return list.iterator();
                return duplicates.iterator();
            }
        }, true);

        return keyPartition(duplicateRdd).map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple._2();
            }
        });
    }

    private static JavaPairRDD<Integer, String> keyPartition(JavaRDD<Tuple2<Integer, String>> tupleJavaRDD) {
        return tupleJavaRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple;
            }
        }).partitionBy(new KeyPartitioner(tupleJavaRDD.getNumPartitions()));
    }
}
