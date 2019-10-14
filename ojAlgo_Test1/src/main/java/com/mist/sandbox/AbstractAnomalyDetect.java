package com.mist.sandbox;

import java.util.ArrayList;

/**
 * Abstract class for anomaly detection
 */
public abstract class AbstractAnomalyDetect {
    protected ArrayList<Float> BaselineWindow;
    protected Float CurrentValue;
    protected Float Sigma;

    /**
     * Prepare - Used to enforce minimum initialization parameters for subclasses
     * @param baseline_window - A time series of observed values which are used to set the context of the current dataval
     * @param current_value - The current observed dataval in the series
     * @param sigma - The sigma dataval which is used as a threshold to determine anomalies
     */
    public void Prepare(ArrayList<Float> baseline_window, Float current_value, Float sigma) {
        this.BaselineWindow = baseline_window;
        this.CurrentValue = current_value;
        this.Sigma = sigma;
    }

    /**
     * Run anomaly detection to determine if the current observed dataval is anomalous.
     * @return True if it is an anomaly
     */
    public abstract Boolean IsAnomaly();
}
