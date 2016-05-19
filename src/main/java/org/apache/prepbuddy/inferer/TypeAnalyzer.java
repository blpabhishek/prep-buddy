package org.apache.prepbuddy.inferer;

import org.apache.prepbuddy.typesystem.BaseDataType;
import org.apache.prepbuddy.typesystem.DataType;

import java.io.Serializable;
import java.util.List;

public class TypeAnalyzer implements Serializable {
    private List<String> sampleData;

    public TypeAnalyzer(List<String> sampleData) {
        this.sampleData = sampleData;
    }

    public DataType getType() {
        BaseDataType type = BaseDataType.getBaseType(sampleData);
        return type.actualType(sampleData);
    }
}
