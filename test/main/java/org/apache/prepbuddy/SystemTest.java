package org.apache.prepbuddy;

import org.apache.prepbuddy.coreops.ColumnTransformation;
import org.apache.prepbuddy.coreops.DataTransformation;
import org.apache.prepbuddy.coreops.DatasetTransformations;
import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.datacleansers.ReplacementFunction;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class SystemTest extends SparkTestCase {

    @Test
    public void shouldExecuteASeriesOfTransformsOnADataset() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        TransformableRDD deduplicated = initialRDD.deduplicate();
        assertEquals(2, deduplicated.count());

        TransformableRDD purged = deduplicated.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(String record) {
                return record.split(",")[1].equals("YY");
            }
        });
        assertEquals(1, purged.count());

        TransformableRDD imputedRDD = purged.impute(2, new MissingDataHandler() {
            @Override
            public String handleMissingData(String[] record) {
                return "Male";
            }
        });
        assertEquals("X,Y,Male", imputedRDD.first());

        TransformableRDD numericRDD = imputedRDD.replace(2, new ReplacementFunction(new Replacement<>("Male", 0),
                new Replacement<>("Female", 1)));

        assertEquals(1, numericRDD.count());
        assertEquals("X,Y,0", numericRDD.first());
    }

    @Test
    public void _TextFacetShouldGiveCountOfPair() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y", "A,B", "X,Z","A,Q","A,E"));
        TransformableRDD rdd = new TransformableRDD(initialDataset);
        TextFacets facets = rdd.listFacets(0);
        assertEquals(2, facets.count());
    }

    @Test
    public void shouldBeAbleToSplitTheGivenColumn() {
        DatasetTransformations datasetTransformations = new DatasetTransformations();

        ColumnTransformation columnTransformation = new ColumnTransformation(0);

        columnTransformation.splitBy(" ", false);

        datasetTransformations.addColumnTransformations(columnTransformation);

        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("FirstName LastName,MiddleName"));

        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);

        String expected = "FirstName,LastName,MiddleName";

        assertEquals(1, transformed.count());
        String actual = transformed.first();
        assertEquals(expected, actual);
    }

    @Test
    public void shouldBeAbleToSplitTheGivenColumnWithTheGivenNumberOfLengths() {
        DatasetTransformations datasetTransformations = new DatasetTransformations();

        ColumnTransformation columnTransformation = new ColumnTransformation(0);

        columnTransformation.splitBy(" ", false);

        datasetTransformations.addColumnTransformations(columnTransformation);

        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("FirstName LastName,MiddleName"));

        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);

        String expected = "FirstName,LastName,MiddleName";

        assertEquals(1, transformed.count());
        String actual = transformed.first();
        assertEquals(expected, actual);
    }
}
