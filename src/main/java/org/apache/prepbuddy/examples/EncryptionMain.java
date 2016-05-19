package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

public class EncryptionMain implements Serializable {

    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("Encryption").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath,4);
        if(args.length ==2) {
            JavaRDD<String> header = sc.parallelize(Arrays.asList("user,other,direction,duration,timestamp"));
            csvInput = csvInput.subtract(header).subtract(header);
        }
        TransformableRDD transformableRDD = new TransformableRDD(csvInput,FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);

        double average = encryptedRDD.average(0);
        System.out.println("Average of the first column is = " + average);
        sc.close();
    }
}
