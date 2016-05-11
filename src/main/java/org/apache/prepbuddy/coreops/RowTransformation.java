package org.apache.prepbuddy.coreops;

import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaRDD;

public interface RowTransformation {
    JavaRDD<String> apply(JavaRDD<String> dataset, FileType type);
}
