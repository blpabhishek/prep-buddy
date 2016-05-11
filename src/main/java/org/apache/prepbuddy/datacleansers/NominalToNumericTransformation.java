package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.TransformationFunction;
import org.apache.prepbuddy.utils.DefaultValue;
import org.apache.prepbuddy.utils.Replacement;

public class NominalToNumericTransformation implements TransformationFunction {
    private final Replacement[] pairs;
    private final DefaultValue defaultt;

    public NominalToNumericTransformation(DefaultValue defaultt,Replacement... pairs) {
        this.pairs = pairs;
        this.defaultt = defaultt;
    }

    @Override
    public String[] apply(String[] row, int columnIndex) {
        String existingValue = row[columnIndex];
        for (Replacement pair : pairs) {
            if (pair.matches(existingValue)) {
                row[columnIndex] = pair.replacementValue();
                return row;
            }
        }
        row[columnIndex] = defaultt.asString();
        return row;
    }

}
