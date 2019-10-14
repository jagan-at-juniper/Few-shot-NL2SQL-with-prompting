package com.mist.sandbox;

//import com.alibaba.fastjson.JSON;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;
//import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.net.URI;
import java.util.UUID;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

public class AnomalyOarima extends AnomalyBase {
    public String model_id;
    private int oarima_winsize;
    private Double lrate;
    private Double epsilon;
    private int confidence_interval_winsize;
    public OarimaModel model;

    public enum AnomalyDirection {UP, DOWN};

    /**
     * Constructor.  Initializes model parameters, including storage of model in Redis under model ID.
     * @param model_id - The model ID.  Should be a UUID as a string.  The convenience method get_new_model_id() is
     *                 available for generating new model IDs.
     * @param oarima_winsize - Hyperparameter (Make sure to tune it).  The width of the OARIMA sliding window.
     * @param lrate - Hyperparameter (Make sure to tune it).  The OARIMA learning rate.
     * @param epsilon - Hyperparameter (Make sure to tune it).  The ORIMA tuning parameter.
     * @param confidence_interval_winsize - Hyperparameter (Make sure to tune it).  The width of the sliding window
     *                                    for the confidence interval standard deviation.
     * @throws OarimaException
     */
    public AnomalyOarima(String model_id, int oarima_winsize, Double lrate, Double epsilon, int confidence_interval_winsize) throws OarimaException {
        assert model_id != null : "AnomalyOarima: model_id cannot be null";
        assert model_id.length() != 0 : "AnomalyOarima: model_id cannot be empty";
        assert oarima_winsize > 0 : "AnomalyOarima: oarima_winsize must be greater than zero";
        assert lrate > 0 : "AnomalyOarima: lrate must be greater than zero";
        assert epsilon > 0 : "AnomalyOarima: epsilon must be greater than zero";
        assert confidence_interval_winsize > 0 : "AnomalyOarima: confidence_interval_winsize must be greater than zero";
        this.model_id = model_id;
        this.oarima_winsize = oarima_winsize;
        this.lrate = lrate;
        this.epsilon = epsilon;
        this.confidence_interval_winsize = confidence_interval_winsize;

        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        PrimitiveDenseStore weights = storeFactory.makeZero(1, oarima_winsize);
        for (int i=0; i < oarima_winsize; i++) weights.set(i, 0.1D);

        PrimitiveDenseStore window = storeFactory.makeZero(1, oarima_winsize);
        PrimitiveDenseStore A_trans = storeFactory.makeEye(oarima_winsize, oarima_winsize);
        DescriptiveStatistics stats = new DescriptiveStatistics();

        model = new OarimaModel(A_trans, weights, window, stats);
    }

    public class OarimaModel implements Serializable {
        public PrimitiveDenseStore A_trans;
        public PrimitiveDenseStore weights;
        public PrimitiveDenseStore window;
        public DescriptiveStatistics stats;

        public OarimaModel(PrimitiveDenseStore _A_trans, PrimitiveDenseStore _weights, PrimitiveDenseStore _window,
                            DescriptiveStatistics _stats) {
            assert _A_trans != null : "OarimaPrediction: A_trans cannot be null";
            assert _weights != null : "OarimaPrediction: weights cannot be null";
            assert _window != null : "OarimaPrediction: window cannot be null";
            assert _stats != null : "OarimaPrediction: stats cannot be null";

            A_trans = _A_trans;
            weights = _weights;
            window = _window;
            stats = _stats;
        }

        @Override
        public String toString() {
            return "OarimaModel [A_trans=" + A_trans + ", weights=" + weights + ", window=" + window + ", stats=" + stats + "]";
        }
    }

    public class OarimaException extends Exception {
        public OarimaException(String message){
            super("OarimaException: " + message);
        }
    }
}
