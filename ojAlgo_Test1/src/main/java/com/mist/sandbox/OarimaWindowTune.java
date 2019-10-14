package com.mist.sandbox;

import com.mist.sandbox.Data.DataPoint;
import com.mist.sandbox.ONS_with_stats.AnomalyDirection;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import java.awt.*;
import java.util.ArrayList;
import java.util.Date;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

public class OarimaWindowTune {
    public enum SERIES_TYPE {STC, TTC};
    public static Boolean log_transform;

    public static void main(String[] args) throws Exception {
        String filename = "amazon_sle_ttc_by_site_1h_430_506.json";
        SERIES_TYPE mode = SERIES_TYPE.TTC;

        ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();
        if (mode.equals(SERIES_TYPE.TTC)) {
            datapoints = Data.get_data_ttc(filename);
        } else if (mode.equals(SERIES_TYPE.STC)) {
            datapoints = Data.get_data_stc(filename);
        }

        double best_lrate = 0.1; // 0.1;
        double best_epsilon = Math.pow(1, -7);

        ArrayList<Double> data = Data.get_doubles_list(datapoints);

        log_transform = false;
        oarima_window_tune("Data Without Log Transform", data, best_lrate, best_epsilon);

        for (int i=0; i < data.size(); i++) {
            Double orig = data.get(i);
            Double log = orig > 0 ? Math.log(orig) : 0.0;
            data.set(i, log);
        }

        log_transform = true;
        oarima_window_tune("Data With Log Transform", data, best_lrate, best_epsilon);
    }

    private static void oarima_window_tune(String title, ArrayList<Double> data, double lrate, double epsilon) {
        int mk_min = 1;
        int mk_max = 20;
        int mk_inc = 1; // (mk_max - mk_min) / 20;

        double[] predictions;
        ArrayList<Double> medians = new ArrayList<Double>();
        ArrayList<Double> windows = new ArrayList<Double>();
        ArrayList<Double> oarima_diffs = new ArrayList<Double>();
        ArrayList<Double> median_diffs = new ArrayList<Double>();
        for (Integer mk=mk_min; mk < mk_max; mk=mk+mk_inc) {
            predictions = get_oarima_predictions(data, mk, lrate, epsilon);
            medians = get_percentile(data, mk,50.0);
            double abs_oarima_diff = 0.0;
            double abs_median_diff = 0.0;
            if (log_transform) {
                for (int i = 0; i < data.size(); i++) {
                    abs_oarima_diff += Math.abs(Math.exp(data.get(i)) - Math.exp(predictions[i]));
                    abs_median_diff += Math.abs(Math.exp(data.get(i)) - Math.exp(medians.get(i)));
                }
            } else {
                for (int i = 0; i < data.size(); i++) {
                    abs_oarima_diff += Math.abs(data.get(i) - predictions[i]);
                    abs_median_diff += Math.abs(data.get(i) - medians.get(i));
                }
            }
            double oarima_mean_diff = abs_oarima_diff / data.size();
            double median_mean_diff = abs_median_diff / data.size();
            windows.add(mk.doubleValue());
            oarima_diffs.add(oarima_mean_diff);
            median_diffs.add(median_mean_diff);
        }
        draw_chart(title, windows, oarima_diffs, median_diffs);
    }

    public static double[] get_oarima_predictions(ArrayList<Double> data, int mk, double lrate, double epsilon) {
        int len = data.size();

        // initialize the ojalgo matrices
        Init_oarima init_oarima = new Init_oarima(mk, epsilon).invoke();
        PrimitiveDenseStore w = init_oarima.getW();
        PrimitiveDenseStore A_trans = init_oarima.getA_trans();
        PrimitiveDenseStore window = init_oarima.getWindow();

        // fill the data arrays
        double[] dataX = new double[len];
        double[] dataY = new double[len];
        double[] predictions = new double[len];
        for (int i=0; i < len; i++) {
            dataX[i] = i;
            dataY[i] = data.get(i);
        }

        // train the model
        for (int i=mk; i < len; i++) {
            int window_idx = 0;
            for (int j=i-mk; j < i; j++) window.set(window_idx++, data.get(j)); // load the window

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            double prediction = w.multiply(window.transpose()).doubleValue(0, 0);

            // diff = prediction - data(i); # scalar: absolute error
            double diff = prediction - data.get(i);

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

        // predict using trained model
        for (int i=0; i < len; i++) {
            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
            predictions[i] = prediction;

            // diff = prediction - data(i); # scalar: absolute error
            double diff = prediction - data.get(i);

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

    public static ArrayList<Double> get_percentile(ArrayList<Double> data, int mk, double percent) {
        DescriptiveStatistics percentile_stats = new DescriptiveStatistics();
        percentile_stats.setWindowSize(mk);

        // train the model
        for (int i=0; i < data.size(); i++) {
            percentile_stats.addValue(data.get(i));
        }

        // predict using trained model
        ArrayList<Double> percentiles = new ArrayList<Double>();
        for (int i=0; i < data.size(); i++) {
            percentile_stats.addValue(data.get(i));
            double percentile = percentile_stats.getPercentile(percent);
            percentiles.add(percentile);
        }
        return percentiles;
    }

    private static class Init_oarima {
        private int mk;
        private double epsilon;
        private PrimitiveDenseStore w;
        private PrimitiveDenseStore a_trans;
        private PrimitiveDenseStore window;

        public Init_oarima(int mk, double epsilon) {
            this.mk = mk;
            this.epsilon = epsilon;
        }

        public PrimitiveDenseStore getW() {
            return w;
        }

        public PrimitiveDenseStore getA_trans() {
            return a_trans;
        }

        public PrimitiveDenseStore getWindow() {
            return window;
        }

        public Init_oarima invoke() {
            PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
            w = storeFactory.makeZero(1, mk);
            for (int i=0; i < mk; i++) w.set(i, 0.1D);

            a_trans = storeFactory.makeEye(mk, mk);
            a_trans.multiply(epsilon);

            window = storeFactory.makeZero(1, mk);
            return this;
        }
    }

    public static void draw_chart(String title, ArrayList<Double> windows, ArrayList<Double> oarima_diffs, ArrayList<Double> median_diffs) {
        // Create Chart
        XYChart chart = new XYChartBuilder().title(title).xAxisTitle("Mean Absolute Error").yAxisTitle("Window Size").build();
        XYSeries oarima_series = chart.addSeries("Oarima Diffs", oarima_diffs, windows);
        oarima_series.setLineColor(Color.GREEN);
        oarima_series.setMarker(SeriesMarkers.NONE);
        XYSeries median_series = chart.addSeries("Median Diffs", median_diffs, windows);
        median_series.setLineColor(Color.BLUE);
        median_series.setMarker(SeriesMarkers.NONE);

        // Show it
        new SwingWrapper<XYChart>(chart).displayChart();
    }
}
