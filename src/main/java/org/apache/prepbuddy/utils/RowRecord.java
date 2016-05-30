package org.apache.prepbuddy.utils;

public class RowRecord {
    private String[] columnValues;

    public RowRecord(final String[] columnValues) {
        this.columnValues = columnValues;
    }

    public String valueAt(int columnIndex) {
        return columnValues[columnIndex];
    }

    public int length() {
        return columnValues.length;
    }

    public Boolean hasEmptyColumn() {
        for (String columnValue : columnValues) {
            if (columnValue.trim().isEmpty())
                return true;
        }
        return false;
    }
}
