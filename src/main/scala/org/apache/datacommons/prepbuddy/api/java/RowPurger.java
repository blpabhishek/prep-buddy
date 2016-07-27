package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface RowPurger extends Serializable {
    Boolean evaluate(RowRecord record);
}
