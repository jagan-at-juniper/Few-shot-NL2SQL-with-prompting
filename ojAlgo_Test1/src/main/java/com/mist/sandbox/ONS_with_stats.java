package com.mist.sandbox;

import org.knowm.xchart.*;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;
import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

import java.awt.*;
import java.util.ArrayList;
import java.util.Date;

import com.mist.sandbox.Data.DataPoint;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class ONS_with_stats {
    public static enum AnomalyDirection {UP, DOWN, UPDOWN};

    public static void main(String[] args) throws Exception {
        int conf_int_winsize = 30; // 24; // 48
        int median_winsize = 5; // 4
        double conf_int_mult = 3.0;
        Float lift_sigma_mult = 1.0F;

        // initialize the oarima hyperparams
        int mk = 11;
        Double lrate = 0.1;
        Double epsilon = Math.pow(1, -7);

        System.out.println("Getting data ...");
        String train_filename = "site_10m_office_stage_5_4.json";
        String test_filename = "site_10m_office_stage_5_6.json";
        String X_axis_title = "10 minutes";
        int minutes_in_interval = 10;
        int min_samples_10_minutes = 5;
        int min_samples = minutes_in_interval / 10 * min_samples_10_minutes;

        ArrayList<DataPoint> predict_train_data = new ArrayList<DataPoint>();
        ArrayList<DataPoint> predict_test_data = new ArrayList<DataPoint>();
        ArrayList<DataPoint> conf_int_train_data = new ArrayList<DataPoint>();
        ArrayList<DataPoint> conf_int_test_data = new ArrayList<DataPoint>();

        AnomalyDirection direction = AnomalyDirection.UP;
        Boolean has_ceiling = false;
        Boolean has_floor = false;
        Double ceiling_value = 0.0;
        Double floor_value = 0.0;
        String mode = "TTC";
        Boolean filter_outliers = true;
        Boolean use_percentile_conf_int = true;

        if (mode.equals("TTC")) {
            predict_train_data = Data.get_data_ttc(train_filename);
            for (int i=0; i < predict_train_data.size(); i++) {
                Double orig = predict_train_data.get(i).dataval;
                Double log = orig > 0 ? Math.log(orig) : 0.0;
                predict_train_data.get(i).dataval = log;
            }
            predict_test_data = Data.get_data_ttc(test_filename);
            for (int i=0; i < predict_test_data.size(); i++) {
                Double orig = predict_test_data.get(i).dataval;
                Double log = orig > 0 ? Math.log(orig) : 0.0;
                predict_test_data.get(i).dataval = log;
            }
            conf_int_train_data = Data.get_data_ttc(train_filename);
            conf_int_test_data = Data.get_data_ttc(test_filename);
            direction = AnomalyDirection.UP;
            has_ceiling = true;
            ceiling_value = 20000.0D;
            mk = 11;
            lrate = 0.1;
            epsilon = Math.pow(1, -7);
            conf_int_winsize = 48;
        } else if (mode.equals("STC")) {
            predict_train_data = Data.get_data_stc(train_filename);
            predict_test_data = Data.get_data_stc(test_filename);
            direction = AnomalyDirection.DOWN;
            has_floor = true;
            floor_value = 0.0D;
            mk = 11;
            lrate = 0.1;
            epsilon = Math.pow(1, -7);
            conf_int_winsize = 48;
        }

        DescriptiveStatistics median_stats = new DescriptiveStatistics();
        median_stats.setWindowSize(median_winsize);
        DescriptiveStatistics conf_int_stats = new DescriptiveStatistics();
        conf_int_stats.setWindowSize(conf_int_winsize);

        ArrayList<Double> medians = get_medians(median_stats, predict_train_data); // train the stats

        Init_oarima init_oarima = new Init_oarima(mk).init();
        PrimitiveDenseStore w = init_oarima.getW();
        PrimitiveDenseStore window = init_oarima.getWindow();
        PrimitiveDenseStore A_trans = init_oarima.getA_trans();
        ArrayList<Double> predictions = get_oarima_predictions(predict_train_data, mk, lrate, epsilon, A_trans, w, window, direction); // train oarima

        medians = get_medians(median_stats, predict_test_data); // generate current stats
        predictions = get_oarima_predictions(predict_test_data, mk, lrate, epsilon, A_trans, w, window, direction); // generate current predictions

        double[] whole_series = new double[predict_test_data.size()];
        for (int i=0; i < predict_test_data.size(); i++)
            whole_series[i] = predict_test_data.get(i).dataval;
        double mean = 0.0; // StatUtils.mean(whole_series);
        double std_dev = 0.0; // Math.sqrt(StatUtils.variance(whole_series));
        ArrayList<Double> mean_series = new ArrayList<Double>();
        ArrayList<Double> std_dev_series = new ArrayList<Double>();
        for (int i=0; i < predict_test_data.size(); i++) {
            mean_series.add(mean);
            if (direction.equals(AnomalyDirection.UP))
                std_dev_series.add((std_dev * conf_int_mult) + mean);
            else
                std_dev_series.add((std_dev * conf_int_mult) - mean);
        }

        // init the data arrays for actuals
        ArrayList<Date> dataX = new ArrayList<Date>();
        ArrayList<Double> dataY = new ArrayList<Double>();
        for (int i=0; i < predict_test_data.size(); i++) {
            dataX.add(predict_test_data.get(i).rt);
            dataY.add(predict_test_data.get(i).dataval);
        }

//        ArrayList<Double> weighted = get_sliding_weighted_mean(test_data, 3);

        ArrayList<Double> confidence_intervals = new ArrayList<Double>();
        if (use_percentile_conf_int) {
            if (mode.equals("TTC"))
                conf_int_mult = 3.0;
            else
                conf_int_mult = 1.0;
            confidence_intervals = get_confidence_interval_percentile(conf_int_test_data, conf_int_stats, predictions,
                    conf_int_mult, direction, has_ceiling, has_floor, ceiling_value, floor_value, filter_outliers);
            for (int i=0; i < predictions.size(); i++) {
                Double orig = predictions.get(i);
                Double log = orig > 0 ? Math.exp(orig) : 0.0;
                predictions.set(i, log);
            }
            for (int i=0; i < dataY.size(); i++) {
                Double orig = dataY.get(i);
                Double log = orig > 0 ? Math.exp(orig) : 0.0;
                dataY.set(i, log);
            }
        } else {
            confidence_intervals = get_confidence_interval_std_dev(conf_int_test_data, conf_int_stats, predictions, medians,
                    conf_int_mult, direction, has_ceiling, has_floor, ceiling_value, floor_value, filter_outliers);
        }

        Boolean require_min_samples = false;
        ArrayList<Double> oarima_anomalies_all = get_oarima_anomalies(predict_test_data, confidence_intervals,
                require_min_samples, min_samples, direction);

        require_min_samples = true;
        ArrayList<Double> oarima_anomalies_min_samples = get_oarima_anomalies(predict_test_data, confidence_intervals,
                require_min_samples, min_samples, direction);

        require_min_samples = false;
        ArrayList<Double> lift_anomalies_all = get_lift_anomalies(predict_test_data, lift_sigma_mult, require_min_samples, min_samples);

        require_min_samples = true;
        ArrayList<Double> lift_anomalies_min_samples = get_lift_anomalies(predict_test_data, lift_sigma_mult, require_min_samples, min_samples);

        draw_chart(test_filename, X_axis_title, oarima_anomalies_all, oarima_anomalies_min_samples,
                lift_anomalies_all, lift_anomalies_min_samples,
                dataX, dataY, medians, confidence_intervals, predictions, min_samples, mean_series, std_dev_series);
    }

    public static ArrayList<Double> get_sliding_weighted_mean(ArrayList<DataPoint> data, int winsize) {
        ArrayList<Double> result = new ArrayList<Double>();
        for (int i=0; i < winsize; i++)
            result.add(data.get(i).dataval);

        for (int i=winsize; i < data.size(); i++) {
            Double val = 0.0D;
            int n = 0;
            for (int j=0; j < winsize; j++) {
                DataPoint dp = data.get(i - j);
                val += dp.dataval * dp.attempts;
                n += dp.attempts;
            }
            result.add(val / n);
        }
        return result;
    }

    public static ArrayList<Double> get_oarima_anomalies(ArrayList<DataPoint> data,
                                                         ArrayList<Double> conf_int,
                                                         Boolean require_min_samples, int min_samples,
                                                         AnomalyDirection direction) {
        ArrayList<Double> anomalies = new ArrayList<Double>();
        for (int i=0; i < data.size(); i++) {
            Boolean added_one = false;
            int attempts = data.get(i).attempts;
            if (direction.equals(AnomalyDirection.UP) || direction.equals(AnomalyDirection.UPDOWN)) {
                Double conf_thresh_up = conf_int.get(i);
                if (data.get(i).dataval >= conf_thresh_up) {
                    if ((require_min_samples && (attempts > min_samples)) || (require_min_samples == false)) {
                        anomalies.add(data.get(i).dataval);
                        added_one = true;
                    }
                }
            }
            if (direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UPDOWN)) {
                Double conf_thresh_down = conf_int.get(i);
                if (data.get(i).dataval <= conf_thresh_down) {
                    if ((require_min_samples && (attempts > min_samples)) || (require_min_samples == false)) {
                        anomalies.add(data.get(i).dataval);
                        added_one = true;
                    }
                }
            }
            if (added_one == false)
                anomalies.add(null);
        }
        return anomalies;
    }

    public static ArrayList<Double> get_lift_anomalies(ArrayList<DataPoint> data, Float lift_sigma_mult,
                                                       Boolean require_min_samples, int min_samples) {
        ArrayList<Double> lift_anomalies = new ArrayList<Double>();
        AnomalyDetectLift detector = new AnomalyDetectLift();
        for (int i=0; i < data.size(); i++) {
            Float variance = data.get(i).variance;
            Integer base_attempts = data.get(i).base_bad + data.get(i).base_good;
            ArrayList<Float> baseline_series = detector.CreateBaseline(data.get(i).base_bad, base_attempts, variance);

            Float current_value = detector.CreateCurrentValue(data.get(i).bad, data.get(i).attempts);

            detector.Prepare(baseline_series, current_value, lift_sigma_mult);

            if (detector.IsAnomaly()) {
                if ((require_min_samples && (data.get(i).attempts > min_samples)) || (require_min_samples == false))
                    lift_anomalies.add(data.get(i).dataval);
                else
                    lift_anomalies.add(null);
            } else {
                lift_anomalies.add(null);
            }
        }
        return lift_anomalies;
    }

    public static ArrayList<Double> get_oarima_predictions(ArrayList<DataPoint> data, int mk, Double lrate, Double epsilon,
                                                           PrimitiveDenseStore A_trans, PrimitiveDenseStore w, PrimitiveDenseStore window,
                                                           AnomalyDirection direction) {
        // initialize the ojalgo matrices
        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        A_trans.multiply(epsilon);

        ArrayList<Double> predictions = new ArrayList<Double>();
        for (int i=0; i < data.size(); i++) {
            window.set(i % mk, data.get(i).dataval); // load the window

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
            prediction = direction.equals(AnomalyDirection.UP) ? Math.max(prediction, 0.0) : Math.min(prediction, 1.0);
            predictions.add(prediction);

            // diff = prediction - data(i); # scalar: absolute error
            Double diff = prediction - data.get(i).dataval;

            // grad = 2 * data(i-mk:i-1) * diff; # row vector: gradient?
            MatrixStore grad = window.multiply(2.0D).multiply(diff); // row vector: gradient?

            // A_trans = A_trans - A_trans * grad' * grad * A_trans/(1 + grad * A_trans * grad')
            PrimitiveDenseStore A_trans_div = A_trans.copy(); // copy because we modify A_trans in place for A_trans_div
            // A_trans_div = A_trans/(1 + grad * A_trans * grad') from the equation above
            A_trans_div.modifyAll(DIVIDE.second(1.0D + grad.multiply(A_trans).multiply(grad.transpose()).doubleValue(0, 0)));
            A_trans = (PrimitiveDenseStore)A_trans.subtract(A_trans.multiply(grad.transpose()).multiply(grad).multiply(A_trans_div));

            // w = w - lrate * grad * A_trans ; # update weights
            w = (PrimitiveDenseStore)w.subtract(A_trans.multiply(grad).multiply(lrate));
        }

        return predictions;
    }

    public static ArrayList<Double> get_medians(DescriptiveStatistics stats, ArrayList<DataPoint> data) {
        ArrayList<Double> medians = new ArrayList<Double>();

        for (int i=0; i < data.size(); i++) {
            stats.addValue(data.get(i).dataval);
            double median = stats.getPercentile(50.0);
            medians.add(median);
        }
        return medians;
    }

    public static ArrayList<Double> get_confidence_interval_std_dev(ArrayList<DataPoint> data, DescriptiveStatistics stats,
                                                                    ArrayList<Double> predictions, ArrayList<Double> medians,
                                                                    double sigma_mult, AnomalyDirection direction,
                                                                    Boolean ceiling, Boolean floor,
                                                                    Double ceiling_value, Double floor_value, Boolean filter_outliers) {
        assert direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UP) : "Bad direction";
        assert data.size() > 0 : "Data is empty";
        assert predictions.size() > 0 : "Predictions are empty";

        ArrayList<Double> confidence_interval = new ArrayList<Double>();

        double conf_int = 0.0;
        Double dataval = 0.0;
        stats.addValue(data.get(0).dataval);
        conf_int = get_next_conf_int_stdev(stats, predictions.get(0), sigma_mult, direction, ceiling, floor, ceiling_value, floor_value);
        confidence_interval.add(conf_int);

        for (int i=1; i < data.size(); i++) {
            if (filter_outliers) {
                if (direction.equals(AnomalyDirection.UP))
                    dataval = data.get(i).dataval < conf_int ? data.get(i).dataval : predictions.get(i);
                else
                    dataval = data.get(i).dataval > conf_int ? data.get(i).dataval : predictions.get(i);
            } else {
                dataval = data.get(i).dataval;
            }
            stats.addValue(dataval);
            conf_int = get_next_conf_int_stdev(stats, predictions.get(i), sigma_mult, direction, ceiling, floor, ceiling_value, floor_value);
            confidence_interval.add(conf_int);
        }
        return confidence_interval;
    }

    public static ArrayList<Double> get_confidence_interval_percentile(ArrayList<DataPoint> data, DescriptiveStatistics stats,
                                                                    ArrayList<Double> predictions,
                                                                    double sigma_mult, AnomalyDirection direction,
                                                                    Boolean ceiling, Boolean floor,
                                                                    Double ceiling_value, Double floor_value, Boolean filter_outliers) {
        assert direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UP) : "Bad direction";
        assert data.size() > 0 : "Data is empty";

        ArrayList<Double> confidence_intervals = new ArrayList<Double>();
        double conf_int = 0.0;
        Double dataval = 0.0;
        stats.addValue(data.get(0).dataval);
        conf_int = get_next_conf_int_percentile(stats, predictions.get(0), sigma_mult, direction, ceiling, floor, ceiling_value, floor_value);
        confidence_intervals.add(conf_int);

        for (int i=1; i < data.size(); i++) {
            if (filter_outliers) {
                if (direction.equals(AnomalyDirection.UP))
                    dataval = data.get(i).dataval < conf_int ? data.get(i).dataval : predictions.get(i);
                else
                    dataval = data.get(i).dataval > conf_int ? data.get(i).dataval : predictions.get(i);
            } else {
                dataval = data.get(i).dataval;
            }
            stats.addValue(dataval);
            conf_int = get_next_conf_int_percentile(stats, predictions.get(i), sigma_mult, direction, ceiling, floor, ceiling_value, floor_value);
            confidence_intervals.add(conf_int);
        }
        return confidence_intervals;
    }

    public static double get_next_conf_int_stdev(DescriptiveStatistics stats, Double prediction, double sigma_mult,
                                                 AnomalyDirection direction, Boolean ceiling, Boolean floor,
                                                 Double ceiling_value, Double floor_value) {
        double conf_int = 0.0;
        double interval = stats.getStandardDeviation() * sigma_mult;

        if (direction.equals(AnomalyDirection.UP)) {
            conf_int = prediction + interval;
            conf_int = ceiling ? Math.min(ceiling_value, conf_int) : conf_int;
        } else if (direction.equals(AnomalyDirection.DOWN)) {
            conf_int = prediction - interval;
            conf_int = floor ? Math.max(floor_value, conf_int) : conf_int;
        }
        return conf_int;
    }

    public static double get_next_conf_int_percentile(DescriptiveStatistics stats, Double prediction, double sigma_mult,
                                                 AnomalyDirection direction, Boolean ceiling, Boolean floor,
                                                 Double ceiling_value, Double floor_value) {
        double conf_int = 0.0;
        if (direction.equals(AnomalyDirection.UP)) {
            conf_int = prediction + stats.getPercentile(75.0) * sigma_mult;
            conf_int = ceiling ? Math.min(ceiling_value, conf_int) : conf_int;
        } else if (direction.equals(AnomalyDirection.DOWN)) {
            conf_int = prediction - stats.getPercentile(25.0) * sigma_mult;
            conf_int = floor ? Math.max(floor_value, conf_int) : conf_int;
        }
        return conf_int;
    }

    public static void draw_chart(String filename, String x_axis_title,
                                  ArrayList<Double> anomalies_all, ArrayList<Double> anomalies_min,
                                  ArrayList<Double> lift_anomalies_all, ArrayList<Double> lift_anomalies_min,
                                  ArrayList<Date> dataX, ArrayList<Double> dataY,
                                  ArrayList<Double> medians, ArrayList<Double> conf_intervals, ArrayList<Double> predictions,
                                  int min_samples, ArrayList<Double> means, ArrayList<Double> std_devs) {
        // Create Chart
        XYChart chart = new XYChartBuilder().title("OARIMA Results: " + filename).xAxisTitle(x_axis_title).yAxisTitle("Median TTC (ms)").build();
        XYSeries actual_series = chart.addSeries("Actual", dataX, dataY);
        actual_series.setLineColor(Color.GREEN);
        actual_series.setMarker(SeriesMarkers.NONE);
        XYSeries predict_series = chart.addSeries("Predicted", dataX, predictions);
        predict_series.setLineColor(Color.RED);
        predict_series.setMarker(SeriesMarkers.NONE);
        XYSeries high_series = chart.addSeries("Confidence Interval", dataX, conf_intervals);
        XYSeries median_series = chart.addSeries("Median", dataX, medians);
        median_series.setLineColor(Color.BLUE);
        median_series.setMarker(SeriesMarkers.NONE);
        high_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Area);
        high_series.setLineColor(Color.BLACK);
        high_series.setLineWidth(2.0F);
        high_series.setMarker(SeriesMarkers.NONE);
        XYSeries anomaly_all_series = chart.addSeries("OARIMA Anomalies (All)", dataX, anomalies_all);
        anomaly_all_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        anomaly_all_series.setMarker(SeriesMarkers.CIRCLE);
        anomaly_all_series.setMarkerColor(Color.LIGHT_GRAY);
        XYSeries anomaly_min_series = chart.addSeries("OARIMA Anomalies (Min " + min_samples + " Samples)", dataX, anomalies_min);
        anomaly_min_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        anomaly_min_series.setMarker(SeriesMarkers.CIRCLE);
        anomaly_min_series.setMarkerColor(Color.RED);
        XYSeries lift_anomaly_all_series = chart.addSeries("Lift Anomalies (All)", dataX, lift_anomalies_all);
        lift_anomaly_all_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        lift_anomaly_all_series.setMarker(SeriesMarkers.TRIANGLE_UP);
        lift_anomaly_all_series.setMarkerColor(Color.GRAY);
        XYSeries lift_anomaly_min_series = chart.addSeries("Lift Anomalies(Min " + min_samples + " Samples)", dataX, lift_anomalies_min);
        lift_anomaly_min_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        lift_anomaly_min_series.setMarker(SeriesMarkers.TRIANGLE_UP);
        lift_anomaly_min_series.setMarkerColor(Color.GREEN);
        XYSeries mean_series = chart.addSeries("Mean of Whole Series", dataX, means);
        mean_series.setLineColor(Color.pink);
        mean_series.setMarker(SeriesMarkers.NONE);
        XYSeries std_dev_series = chart.addSeries("Sigma of Whole Series", dataX, std_devs);
        std_dev_series.setLineColor(Color.CYAN);
        std_dev_series.setMarker(SeriesMarkers.NONE);

        chart.getStyler().setMarkerSize(16);
        chart.getStyler().setDatePattern("hh:mm");

        // Show it
        new SwingWrapper<XYChart>(chart).displayChart();
    }

    public static class Init_oarima {
        private int mk;
        private PrimitiveDenseStore w;
        private PrimitiveDenseStore window;
        private PrimitiveDenseStore a_trans;

        public Init_oarima(int mk) {
            this.mk = mk;
        }

        public PrimitiveDenseStore getW() {
            return w;
        }

        public PrimitiveDenseStore getWindow() {
            return window;
        }

        public PrimitiveDenseStore getA_trans() {
            return a_trans;
        }

        public Init_oarima init() {
            PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
            w = storeFactory.makeZero(1, mk);
            for (int i=0; i < mk; i++) w.set(i, 0.1D);

            window = storeFactory.makeZero(1, mk);

            a_trans = storeFactory.makeEye(mk, mk);
            return this;
        }
    }
}
