package org.apache.prepbuddy.smoothingops;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Weights implements Serializable {
    private int size;
    private List<Double> weights;

    public Weights(int windowSize) {
        size = windowSize;
        weights = new ArrayList<>(size);
    }

    public void add(double value) {
        if (weights.size() == size)
            throw new ApplicationException(ErrorMessages.SIZE_LIMIT_IS_EXCEEDED);
        weights.add(value);
    }

    public double get(int index) {
        if (!sumIsUpToOne())
            throw new ApplicationException(ErrorMessages.WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE);
        return weights.get(index);
    }

    public boolean sumIsUpToOne() {
        return sum() == 1;
    }

    private long sum() {
        Double sum = 0.0;
        for (Double value : weights) {
            sum += value;
        }
        return Math.round(sum);
    }

    public int size() {
        return weights.size();
    }
}
