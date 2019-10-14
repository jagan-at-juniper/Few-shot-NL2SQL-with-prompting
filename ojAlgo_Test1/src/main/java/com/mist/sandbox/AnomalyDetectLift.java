package com.mist.sandbox;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * AnomalyDetectLift implements the "Lift" approach to anomaly detection.  It inherits from AbstractAnomalyDetect.
 */
public class AnomalyDetectLift extends AbstractAnomalyDetect {
    Integer current_attempt_count;
    Integer baseline_attempt_count;
    Float base_variance;

    /**
     * Prepare for anomaly detection.
     * @param baseline_window - A time series of observed values which are used to set the context of the current dataval
     * @param current_value - The current observed dataval in the series
     * @param sigma - The sigma dataval which is used as a threshold to determine anomalies
     */
    @Override
    public void Prepare(ArrayList<Float> baseline_window, Float current_value, Float sigma) {
        super.Prepare(baseline_window, current_value, sigma);
    }

    private Float p_val(Integer failure_count, Integer attempts_count) {
        return 1.0f - (new Float(failure_count) / new Float(attempts_count));
    }

    /**
     * Create the baseline time series.  For lift this is a single dataval.  This signature passes a default dataval
     * for baseline variance.
     * @param failure_count
     * @param attempts_count
     * @return
     */
    public ArrayList<Float> CreateBaseline(Integer failure_count, Integer attempts_count) {
        return CreateBaseline(failure_count, attempts_count, 0.5F);
    }

    /**
     * Create the baseline time series.  For lift this is a single dataval.
     * @param failure_count
     * @param attempts_count
     * @param baseline_variance
     * @return Baseline dataval, typically passed to Prepare()
     */
    public ArrayList<Float> CreateBaseline(Integer failure_count, Integer attempts_count, Float baseline_variance) {
        baseline_attempt_count = attempts_count;
        base_variance = baseline_variance;
        return new ArrayList<Float>(Arrays.asList(p_val(failure_count, attempts_count)));
    }

    /**
     * Create the current observed dataval of the time series.
     * @param failure_count
     * @param attempts_count
     * @return Current dataval, typically passed to Prepare()
     */
    public Float CreateCurrentValue(Integer failure_count, Integer attempts_count) {
        current_attempt_count = attempts_count;
        return p_val(failure_count, attempts_count);
    }

    /**
     * Run anomaly detection to determine if the current observed dataval is anomalous.
     * @returnm True if it is an anomaly.
     */
    @Override
    public Boolean IsAnomaly() {
        Float p_baseline = BaselineWindow.get(0);
        double now_error_squared = (((1 - CurrentValue) * CurrentValue) / current_attempt_count) + 1 / Math.pow(current_attempt_count, 2);
        double week_error_squared = (((1 - p_baseline) * p_baseline) / baseline_attempt_count) + 1 / Math.pow(baseline_attempt_count, 2);

        double norm_sigma = (p_baseline - CurrentValue) / Math.sqrt(now_error_squared + Math.pow(base_variance, 2) + week_error_squared);

        return norm_sigma > Sigma;  // "Lift" approach
    }
}