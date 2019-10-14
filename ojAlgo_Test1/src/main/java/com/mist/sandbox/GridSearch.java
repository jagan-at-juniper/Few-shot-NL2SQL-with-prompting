package com.mist.sandbox;

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

import com.mist.sandbox.Data.DataPoint;
import com.mist.sandbox.ONS_with_stats.*;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

public class GridSearch {
    public static enum SERIES_TYPE {STC, TTC};
    public static enum TUNING {OARIMA, CONF};

    public static void main(String[] args) throws Exception {
        AnomalyDirection direction = AnomalyDirection.UP;
        String filename = "amazon_sle_ttc_by_site_1h_430_506.json";
        SERIES_TYPE mode = SERIES_TYPE.TTC;
        TUNING tune = TUNING.OARIMA;
        Boolean has_ceiling = false;
        Boolean has_floor = false;
        Boolean use_log = true;
        Boolean filter_outliers = true;
        Double ceiling_value = 0.0;
        Double floor_value = 0.0;

        ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();
        if (mode.equals(SERIES_TYPE.TTC)) {
            datapoints = Data.get_data_ttc(filename);
            direction = AnomalyDirection.UP;
            has_ceiling = true;
            ceiling_value = 20000.0D;
            if (use_log) {
                has_ceiling = false;
                for (int i=0; i < datapoints.size(); i++) {
                    Double orig = datapoints.get(i).dataval;
                    Double log = orig > 0 ? Math.log(orig) : 0.0;
                    datapoints.get(i).dataval = log;
                }
            }
        } else if (mode.equals(SERIES_TYPE.STC)) {
            datapoints = Data.get_data_stc(filename);
            direction = AnomalyDirection.DOWN;
            has_floor = true;
            floor_value = 0.0D;
        }

        ArrayList<Double> data = Data.get_doubles_list(datapoints);

        int median_winsize = 5;
        DescriptiveStatistics median_stats = new DescriptiveStatistics();
        median_stats.setWindowSize(median_winsize);

        int best_mk = 7;
        double best_lrate = 0.1;
        double best_epsilon = Math.pow(1, -7);

        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        PrimitiveDenseStore w = storeFactory.makeZero(1, best_mk);
        for (int i=0; i < best_mk; i++) w.set(i, 0.1D);

        PrimitiveDenseStore window = storeFactory.makeZero(1, best_mk);

        PrimitiveDenseStore A_trans = storeFactory.makeEye(best_mk, best_mk);
        ArrayList<Double> predictions = ONS_with_stats.get_oarima_predictions(datapoints, best_mk, best_lrate, best_epsilon, A_trans, w, window, direction); // train oarima

        predictions = ONS_with_stats.get_oarima_predictions(datapoints, best_mk, best_lrate, best_epsilon, A_trans, w, window, direction); // generate current predictions

        if (tune.equals(TUNING.OARIMA)) {
            oarima_grid_search(data, best_mk, best_lrate, best_epsilon);
        } else if (tune.equals(TUNING.CONF)) {
            double sigma_mult = 3.0;
            conf_int_grid_search(datapoints, predictions, sigma_mult, direction, has_ceiling, has_floor, ceiling_value, floor_value, filter_outliers);
        }
    }

    private static void oarima_grid_search(ArrayList<Double> data, int best_mk, double best_lrate, double best_epsilon) {
        int mk_min = 1;
        int mk_max = 25;
        int mk_inc = (mk_max - mk_min) / 20;
        System.out.println("mk_min: " + mk_min + ", mk_max: " + mk_max + ", mk_inc: " + mk_inc);

        double lrate_min = 0.1D;
        double lrate_max = Math.pow(10, 3);
        double lrate_inc = (lrate_max - lrate_min) / 100;
        System.out.println("lrate_min: " + lrate_min + ", lrate_max: " + lrate_max + ", lrate_inc: " + lrate_inc);

        double eps_min = Math.pow(10, -7);
        double eps_max = Math.pow(10, -1);
        double eps_inc = (eps_max - eps_min) / 10;
        System.out.println("eps_min: " + eps_min + ", eps_max: " + eps_max + ", eps_inc: " + eps_inc);

        int msg_count = 0;
        int report = 1000;
        int iter = 0;
        double best_RMSE = Math.pow(10, 10);
        for (int mk=mk_min; mk < mk_max; mk=mk+mk_inc) {
            for (double lrate=lrate_min; lrate < lrate_max; lrate=lrate+lrate_inc) {
                for (double epsilon=eps_min; epsilon < eps_max; epsilon=epsilon+eps_inc) {
                    double RMSE = run_oarima_test(data, mk, lrate, epsilon);
                    msg_count++;
                    iter++;
                    if (RMSE < best_RMSE) {
                        best_mk = mk;
                        best_lrate = lrate;
                        best_epsilon = epsilon;
                        best_RMSE = RMSE;
                        System.out.println("New best: RMSE = " + best_RMSE + ", mk: " + best_mk + ", lrate: " + best_lrate + ", epsilon: " + best_epsilon);
                    } else if (msg_count == report) {
                        msg_count = 0;
                        System.out.println(iter + " Curr: RMSE = " + RMSE + ", mk: " + mk + ", lrate: " + lrate + ", epsilon: " + epsilon);
                        System.out.println(iter + " Best: RMSE = " + best_RMSE + ", mk: " + best_mk + ", lrate: " + best_lrate + ", epsilon: " + best_epsilon);
                    }
                }
            }
        }
    }

    public static void conf_int_grid_search(ArrayList<DataPoint> data, ArrayList<Double> predictions,
                                             double sigma_mult, ONS_with_stats.AnomalyDirection direction,
                                            Boolean ceiling, Boolean floor, Double ceiling_value, Double floor_value,
                                            Boolean filter_outliers) {
        int window_min = 1;
        int window_max = 24 * 6;
        int window_inc = 1;
        int msg_count = 0;
        int report = 10;
        int iter = 0;
        int best_window = 0;
        double best_RMSE = Math.pow(10, 10);
        ArrayList<Double> RMSE_list = new ArrayList<Double>();
        ArrayList<Integer> win_list = new ArrayList<Integer>();

        for (int win=window_min; win < window_max; win+=window_inc) {
            DescriptiveStatistics std_dev_stats = new DescriptiveStatistics();
            std_dev_stats.setWindowSize(win);
            ArrayList<Double> conf_int = ONS_with_stats.get_confidence_interval_std_dev(data, std_dev_stats, predictions, null, sigma_mult, direction,
                    ceiling, floor, ceiling_value, floor_value, filter_outliers);
            conf_int = ONS_with_stats.get_confidence_interval_std_dev(data, std_dev_stats, predictions, null, sigma_mult, direction,
                    ceiling, floor, ceiling_value, floor_value, filter_outliers); // repeat with trained stats

            double[] whole_series = new double[data.size()];
            for (int i=0; i < data.size(); i++)
                whole_series[i] = data.get(i).dataval;
            double std_dev = Math.sqrt(StatUtils.variance(whole_series));
            ArrayList<Double> std_dev_series = new ArrayList<Double>();
            for (int i=0; i < data.size(); i++) {
                std_dev_series.add(std_dev * sigma_mult);
            }

            Double RMSE = get_RMSE(std_dev_series, conf_int);
            msg_count++;
            iter++;
            if (RMSE < best_RMSE) {
                best_RMSE = RMSE;
                best_window = win;
                System.out.println("New best: RMSE = " + best_RMSE + ", window = " + best_window);
            } else if (msg_count == report) {
                msg_count = 0;
                System.out.println(iter + " Curr: RMSE = " + RMSE + ", window = " + win);
                System.out.println(iter + " Best: RMSE = " + best_RMSE + ", window = " + best_window);
            }

            RMSE_list.add(RMSE);
            win_list.add(win);
        }
        XYChart chart = new XYChartBuilder().title("Confidence Interval Tuning").xAxisTitle("Window Width").yAxisTitle("RMSE").build();
        XYSeries actual_series = chart.addSeries("RMSE", win_list, RMSE_list);
        actual_series.setLineColor(Color.GREEN);
        actual_series.setMarker(SeriesMarkers.NONE);
        new SwingWrapper<XYChart>(chart).displayChart();
    }

    public static Double get_RMSE(ArrayList<Double> actual, ArrayList<Double> predictions) {
        assert actual.size() == predictions.size();
        Double sum_squared_error = 0.0D;
        for (int i=0; i < actual.size(); i++) {
            Double error = actual.get(i) - predictions.get(i);
            Double square_error =  Math.pow(error, 2);
            sum_squared_error += square_error;
        }
        Double MSE = sum_squared_error / actual.size();
        return Math.sqrt(MSE);
    }

    public static double run_oarima_test(ArrayList<Double> data, int mk, double lrate, double epsilon) {
        int len = data.size();

        // initialize the ojalgo matrices
        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        PrimitiveDenseStore w = storeFactory.makeZero(1, mk);
        for (int i=0; i < mk; i++) w.set(i, 0.1D);

        // fill the data arrays
        double[] dataX = new double[len];
        double[] dataY = new double[len];
        double[] predictions = new double[len];
        for (int i=0; i < len; i++) {
            dataX[i] = i;
            dataY[i] = data.get(i);
        }

        ArrayList<Double> diffs_list = new ArrayList();
        ArrayList<Double> RMSE_list = new ArrayList();
        double SE = 0.0D;
        double total_abs_diffs = 0.0;

        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
        A_trans.multiply(epsilon);

        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);

        for (int i=mk; i < len; i++) {
            int window_idx = 0;
            for (int j=i-mk; j < i; j++) window.set(window_idx++, data.get(j)); // load the window

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
            predictions[i] = prediction;

            // diff = prediction - data(i); # scalar: absolute error
            double diff = prediction - data.get(i);
            total_abs_diffs += Math.abs(diff);

            // grad = 2 * data(i-mk:i-1) * diff; # row vector: gradient?
            MatrixStore grad = window.multiply(2.0D).multiply(diff); // row vector: gradient?

            // A_trans = A_trans - A_trans * grad' * grad * A_trans/(1 + grad * A_trans * grad')
            PrimitiveDenseStore A_trans_div = A_trans.copy(); // copy because we modify A_trans in place for A_trans_div
            // A_trans_div = A_trans/(1 + grad * A_trans * grad') from the equation above
            A_trans_div.modifyAll(DIVIDE.second(1.0D + grad.multiply(A_trans).multiply(grad.transpose()).doubleValue(0, 0)));
            A_trans = (PrimitiveDenseStore)A_trans.subtract(A_trans.multiply(grad.transpose()).multiply(grad).multiply(A_trans_div));

            // w = w - lrate * grad * A_trans ; # update weights
            w = (PrimitiveDenseStore)w.subtract(A_trans.multiply(grad).multiply(lrate));

//            diffs_list.add(diff);
//
//            // SE = SE + diff^2; # squared error
//            SE += Math.pow(diff, 2);
//
//            // MSE = SE / i
//            double MSE = SE / diffs_list.size();
//
//            // rMSE = sqrt(MSE)
//            double RMSE = Math.sqrt(MSE);
//
//            double window_SE = 0.0D;
//            double window_RMSE = 0.0D;
//            if (diffs_list.size() > mk) {
//                for (int wi = diffs_list.size() - mk; wi < diffs_list.size(); wi++) {
//                    window_SE += Math.pow(diffs_list.get(wi), 2);
//                }
//                window_RMSE = Math.sqrt(window_SE / mk);
//                RMSE_list.add(window_RMSE);
//            }
            RMSE_list.add(Math.pow(diff, 2));
        }

//        Collections.sort(RMSE_list);
//        int median_idx = Math.round(RMSE_list.size() / 2);
//        double median_RMSE = RMSE_list.get(median_idx);

        double sum_RMSE = 0.0;
        for (int i=0; i < RMSE_list.size(); i++) {
            sum_RMSE += RMSE_list.get(i);
        }
        double mean_RMSE = sum_RMSE / RMSE_list.size();
        return Math.sqrt(mean_RMSE);

//        return total_abs_diffs;
    }
}
