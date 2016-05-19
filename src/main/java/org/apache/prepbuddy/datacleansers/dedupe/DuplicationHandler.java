package org.apache.prepbuddy.datacleansers.dedupe;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DuplicationHandler implements Serializable {

    public JavaRDD deduplicate(JavaRDD inputRecords) {
        JavaPairRDD fingerprintedRecords = inputRecords.mapToPair(new PairFunction<String, Long, String>() {
            @Override
            public Tuple2<Long, String> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record.toLowerCase());
                return new Tuple2<>(fingerprint, record);
            }
        });

        JavaPairRDD uniqueRecordsWithKeys = fingerprintedRecords.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String accumulator, String fullRecord) throws Exception {
                return fullRecord;
            }
        });
        return uniqueRecordsWithKeys.values();
    }

    public JavaRDD duplicates(JavaRDD inputRecords) {
        JavaPairRDD fingerprintedRDD = inputRecords.mapToPair(new PairFunction<String, Long, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Long, Tuple2<String, Integer>> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record.toLowerCase());
                Tuple2<String, Integer> recordOnePair = new Tuple2<>(record, 1);

                return new Tuple2<>(fingerprint, recordOnePair);
            }
        });

        JavaPairRDD fingerprintedRecordCount = fingerprintedRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> currentRecordOnePair) throws Exception {
                int totalRecordOccurrence = accumulator._2() + currentRecordOnePair._2();
                return new Tuple2<>(accumulator._1(), totalRecordOccurrence);
            }
        });

        JavaPairRDD duplicateRecords = fingerprintedRecordCount.filter(new Function<Tuple2<String, Tuple2<String, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<String, Integer>> fingerprintRecordPair) throws Exception {
                Tuple2<String, Integer> recordOccurrencePair = fingerprintRecordPair._2();
                Integer numberOfOccurrence = recordOccurrencePair._2();

                return numberOfOccurrence != 1;
            }
        });

        return duplicateRecords.values().map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> recordCountPair) throws Exception {
                return recordCountPair._1();
            }
        });
    }

    private long generateFingerprint(String record) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(record.getBytes(), 0, record.length());
            return new BigInteger(1, md.digest()).longValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
