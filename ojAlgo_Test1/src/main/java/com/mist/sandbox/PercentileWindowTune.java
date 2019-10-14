package com.mist.sandbox;

import com.mist.sandbox.Data.DataPoint;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.markers.SeriesMarkers;

import java.awt.*;
import java.util.ArrayList;

public class PercentileWindowTune {
    public enum SERIES_TYPE {STC, TTC};
    public static Boolean log_transform;

    public static void main(String[] args) throws Exception {
        String filename = "amazon_sle_ttc_by_site_1h_430_506.json";
        SERIES_TYPE mode = SERIES_TYPE.STC;
        double percent = 75.0;

        ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();
        if (mode.equals(SERIES_TYPE.TTC)) {
            datapoints = Data.get_data_ttc(filename);
        } else if (mode.equals(SERIES_TYPE.STC)) {
            datapoints = Data.get_data_stc(filename);
        }

        ArrayList<Double> data = Data.get_doubles_list(datapoints);

        log_transform = false;
        percentile_window_tune("Data Without Log Transform", data, percent);

        for (int i=0; i < data.size(); i++) {
            Double orig = data.get(i);
            Double log = orig > 0 ? Math.log(orig) : 0.0;
            data.set(i, log);
        }

        log_transform = true;
        percentile_window_tune("Data With Log Transform", data, percent);
    }

    private static void percentile_window_tune(String title, ArrayList<Double> data, double percent) {
        int mk_min = 1;
        int mk_max = 100;
        int mk_inc = 1; // (mk_max - mk_min) / 20;

        ArrayList<Double> predictions;
        ArrayList<Double> windows = new ArrayList<Double>();
        ArrayList<Double> diffs = new ArrayList<Double>();
        double[] data_for_whole = new double[data.size()];
        for (int i=0; i < data.size(); i++)
            data_for_whole[i] = data.get(i);
        double whole = StatUtils.percentile(data_for_whole, percent);
        for (Integer mk=mk_min; mk < mk_max; mk=mk+mk_inc) {
            predictions = get_percentile(data, mk, percent);
            double abs_diff = 0.0;
            if (log_transform) {
                for (int i = 0; i < data.size(); i++) {
                    abs_diff += Math.abs(whole - Math.exp(predictions.get(i)));
                }
            } else {
                for (int i = 0; i < data.size(); i++) {
                    abs_diff += Math.abs(whole - predictions.get(i));
                }
            }
            double mean_diff = abs_diff / data.size();
            windows.add(mk.doubleValue());
            diffs.add(mean_diff);
        }
        draw_chart(title, windows, diffs);
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

    public static void draw_chart(String title, ArrayList<Double> windows, ArrayList<Double> diffs) {
        // Create Chart
        XYChart chart = new XYChartBuilder().title(title).xAxisTitle("Mean Absolute Error").yAxisTitle("Window Size").build();
        XYSeries actual_series = chart.addSeries("Diff from actual", diffs, windows);
        actual_series.setLineColor(Color.GREEN);
        actual_series.setMarker(SeriesMarkers.NONE);

        // Show it
        new SwingWrapper<XYChart>(chart).displayChart();
    }
}
