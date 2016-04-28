package org.apache.prepbuddy.transformations.imputation;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Imputers implements Serializable{
    private Map<Integer, HandlerFunction> handlers = new HashMap<Integer, HandlerFunction>();

    public void add(int columnNumber, HandlerFunction function) {
        handlers.put(columnNumber, function);
    }

    public String[] handle(String[] columnValues) {
        for (Integer columnIndex : handlers.keySet()) {
            if (columnIndex >= columnValues.length || columnIndex < 0)
                throw new ColumnIndexOutOfBoundsException("No column found on index:: " + columnIndex);
            String value = columnValues[columnIndex];
            if (value == null || value.trim().isEmpty()) {
                Object imputedValue = handlers.get(columnIndex).handleMissingField();
                columnValues[columnIndex] = imputedValue.toString();
            }
        }
        return columnValues;
    }


    interface HandlerFunction extends Serializable {
        Object handleMissingField();
    }
}
