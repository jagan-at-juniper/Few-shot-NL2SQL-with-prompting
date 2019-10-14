package com.mist.sandbox;

public class AnomalyBase {
    protected Float p_val(Integer failure_count, Integer attempts_count) {
        assert failure_count != null : "AnomalyBase.p_val: failure_count cannot be null";
        assert attempts_count != null : "AnomalyBase.p_val: attempts_count cannot be null";
        assert attempts_count > 0 : "AnomalyBase.p_val: attempts_count must be > zero";
        return 1.0f - (new Float(failure_count) / new Float(attempts_count));
    }

    /**
     * Create the baseline value for the SLE
     * @param failure_count The count of failures during the baseline period
     * @param attempts_count The count of attempts during the baseline period
     * @return Baseline value
     */
    public Float CreateBaseline(Integer failure_count, Integer attempts_count) {
        assert failure_count != null : "AnomalyBase.CreateBaseline: failure_count cannot be null";
        assert attempts_count != null : "AnomalyBase.CreateBaseline: attempts_count cannot be null";
        assert attempts_count > 0 : "AnomalyBase.CreateBaseline: attempts_count must be > zero";
        return p_val(failure_count, attempts_count);
    }

    /**
     * Create the current value for the SLE
     * @param failure_count The count of failures during the current period
     * @param attempts_count The count of attempts during the current period
     * @return Current value
     */
    public Float CreateCurrentValue(Integer failure_count, Integer attempts_count) {
        assert failure_count != null : "AnomalyBase.CreateCurrentValue: failure_count cannot be null";
        assert attempts_count != null : "AnomalyBase.CreateCurrentValue: attempts_count cannot be null";
        assert attempts_count > 0 : "AnomalyBase.CreateCurrentValue: attempts_count must be > zero";
        return p_val(failure_count, attempts_count);
    }
}
