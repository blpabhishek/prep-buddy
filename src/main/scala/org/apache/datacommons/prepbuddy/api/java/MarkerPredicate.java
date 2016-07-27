package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
    boolean evaluate(RowRecord row);
}
