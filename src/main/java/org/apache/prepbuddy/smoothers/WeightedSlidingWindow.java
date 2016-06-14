package org.apache.prepbuddy.smoothers;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;

/**
 * A sliding window which calculates the weighted mean of a window
 * for Weighted Moving Average.
 */
public class WeightedSlidingWindow extends SlidingWindow {
    private Weights weights;

    public WeightedSlidingWindow(int size, Weights weights) {
        super(size);
        if (weights.size() != size)
            throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING);
        this.weights = weights;
    }

    @Override
    public void add(double value) {
        if (isFull()) {
            queue.remove();
        }
        int size = queue.size();
        Double weightValue = weights.get(size) * value;
        queue.add(weightValue);
    }

    public Double average() {
        return sum();
    }
}
