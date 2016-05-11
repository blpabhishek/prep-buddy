package org.apache.prepbuddy.groupingops;

import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.prepbuddy.groupingops.FingerprintingAlgorithms.generateSimpleFingerprint;

public class GroupingOps implements Serializable {

    public static TextFacets listTextFacets(JavaRDD<String> initialDataset, final int columnIndex,final FileType fileType) {
        JavaPairRDD<String, Integer> columnValuePair = initialDataset.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                return new Tuple2<>(columnValues[columnIndex], 1);
            }
        });
        JavaPairRDD<String, Integer> facets = columnValuePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return new TextFacets(facets);
    }

    public static Clusters clusterUsingSimpleFingerprint(JavaRDD<String> dataset, final int columnIndex, final FileType type) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = GroupingOps.listTextFacets(dataset, columnIndex, type);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.collect();

        for (Tuple2<String, Integer> tuple : tuples) {
            String key = generateSimpleFingerprint(tuple._1());
            clusters.add(key, tuple);
        }
        return clusters;
    }

    public static Clusters clusterUsingNGramFingerprint(JavaRDD<String> dataset, final int columnIndex, final FileType type, int nGram) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = GroupingOps.listTextFacets(dataset, columnIndex, type);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.take((int) rdd.count());

        for (Tuple2<String, Integer> tuple : tuples) {
            String key = FingerprintingAlgorithms.generateNGramFingerprint(tuple._1(), nGram);
            clusters.add(key, tuple);
        }
        return clusters;
    }

    public static Clusters clusterUsingLevenshteinDistance(JavaRDD<String> dataset, final int columnIndex, final FileType type) {
        Clusters clusters = new Clusters();
        TextFacets textFacets = GroupingOps.listTextFacets(dataset, columnIndex, type);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.take((int) rdd.count());

        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            Tuple2<String, Integer> tuple = tuples.get(i);
            String tupleKey = tuple._1();
            clusters.add(tupleKey, tuple);
            for (int j = i + 1; j < tuples.size(); j++) {
                Tuple2<String, Integer> otherTuple = tuples.get(j);
                String otherTupleKey = otherTuple._1();
                int distance = StringUtils.getLevenshteinDistance(tupleKey, otherTupleKey);
                if (distance < 4 && !(indexes.contains(j))) {
                       clusters.add(tupleKey, otherTuple);
                    indexes.add(j);
                }
            }
        }
        return clusters;
    }
}
