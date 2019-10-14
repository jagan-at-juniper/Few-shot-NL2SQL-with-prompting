package com.mist.sandbox;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import java.awt.*;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;

public class ONS_with_stats_bad {
    public static void main(String[] args) throws Exception {
        System.out.println("Getting data ...");
        String filename = "sle_ttc_anomaly_by_site_1h_office_rt.json";
        String X_axis_title = "10 Minutes";
        ArrayList<DataPoint> data = get_data_rt(filename);
        System.out.println("Got data: " + data);
        int len = data.size();

        DescriptiveStatistics stats_long = new DescriptiveStatistics();
        DescriptiveStatistics stats_short = new DescriptiveStatistics();
        int long_winsize = 48;
        int short_winsize = 4;
        double sigma_mult = 3.0;
        double[] std_dev_high_list = new double[len];
        LinkedList<Double> anomaliesX = new LinkedList<Double>();
        LinkedList<Double> anomaliesY = new LinkedList<Double>();
        stats_long.setWindowSize(long_winsize);
        stats_short.setWindowSize(short_winsize);

        // initialize the oarima hyperparams
        int mk = 7;
        Double lrate = 0.1;
        Double epsilon = Math.pow(1, -7);

        // initialize the ojalgo matrices
        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        PrimitiveDenseStore w = storeFactory.makeZero(1, mk);
        for (int i=0; i < mk; i++) w.set(i, 0.1D);

        // fill the data arrays
        double[] dataX = new double[len];
        double[] dataY = new double[len];
        double[] predictions = new double[len];
        double[] medians = new double[len];

//        for (int i=0; i < len; i++) {
//            DataPoint dp = data.get(i);
//            dataX[i] = dp.rt;
//            dataY[i] = data.get(i);
//        }

//        ArrayList<Double> diffs_list = new ArrayList();
//        Double SE = 0.0D;
//
//        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
//        A_trans.multiply(epsilon);
//
//        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);
//
//        for (int i=mk; i < len; i++) {
//            int window_idx = 0;
//            for (int j=i-mk; j < i; j++) window.set(window_idx++, data.get(j)); // load the window
//
//            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
//            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
//            predictions[i] = prediction;
//
//            // diff = prediction - data(i); # scalar: absolute error
//            Double diff = prediction - data.get(i);
//
//            // grad = 2 * data(i-mk:i-1) * diff; # row vector: gradient?
//            MatrixStore grad = window.multiply(2.0D).multiply(diff); // row vector: gradient?
//
//            // A_trans = A_trans - A_trans * grad' * grad * A_trans/(1 + grad * A_trans * grad')
//            PrimitiveDenseStore A_trans_div = A_trans.copy(); // copy because we modify A_trans in place for A_trans_div
//            // A_trans_div = A_trans/(1 + grad * A_trans * grad') from the equation above
//            A_trans_div.modifyAll(DIVIDE.second(1.0D + grad.multiply(A_trans).multiply(grad.transpose()).doubleValue(0, 0)));
//            A_trans = (PrimitiveDenseStore)A_trans.subtract(A_trans.multiply(grad.transpose()).multiply(grad).multiply(A_trans_div));
//
//            // w = w - lrate * grad * A_trans ; # update weights
//            w = (PrimitiveDenseStore)w.subtract(A_trans.multiply(grad).multiply(lrate));
//
//            stats_short.addValue(data.get(i));
//            double median = stats_short.getPercentile(50.0);
//            medians[i] = median;
//
//            stats_long.addValue(data.get(i));
//            double conf_interval = stats_long.getStandardDeviation() * sigma_mult;
//            std_dev_high_list[i] = Math.max(prediction, median) + conf_interval;
//
//            if (data.get(i) > std_dev_high_list[i]) {
//                anomaliesX.add(new Double(i));
//                anomaliesY.add(data.get(i));
//            }
//        }
//
        // Create Chart
        XYChart chart = QuickChart.getChart("OARIMA Results: " + filename, X_axis_title, "Median TTC (ms)", "Actual", dataX, dataY);
        XYSeries predict_series = chart.addSeries("Predicted", dataX, predictions);
        predict_series.setLineColor(Color.RED);
        predict_series.setMarker(SeriesMarkers.NONE);
        XYSeries high_series = chart.addSeries(sigma_mult + " Sigma", dataX, std_dev_high_list);
        XYSeries median_series = chart.addSeries("Median", dataX, medians);
        median_series.setLineColor(Color.BLUE);
        median_series.setMarker(SeriesMarkers.NONE);
        high_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Area);
        high_series.setLineColor(Color.BLACK);
        high_series.setLineWidth(2.0F);
        high_series.setMarker(SeriesMarkers.NONE);
        XYSeries anomaly_series = chart.addSeries("Anomalies", anomaliesX, anomaliesY);
        anomaly_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        anomaly_series.setMarker(SeriesMarkers.CIRCLE);
        anomaly_series.setMarkerColor(Color.red);
        chart.getStyler().setMarkerSize(16);

        // Show it
        new SwingWrapper(chart).displayChart();
//
//        System.out.println("Max diff = " + Collections.max(diffs_list));
//        Double diffsum = 0.0D;
//        for (Double diff:diffs_list) diffsum += diff;
//        Double meandiff = diffsum / diffs_list.size();
//        System.out.println("Mean diff = " + meandiff);
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<Double> get_data_basic(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            ArrayList<Double> data = new ArrayList<Double>();
            ArrayList<Object> obj = (ArrayList<Object>)jsonObject.get("data");
            for (int i=0; i < obj.size(); i++)
                data.add(Double.valueOf(obj.get(i).toString())); // if scaling, need to adjust hyperparameters
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static class DataPoint {
        public String rt;
        public Double value;
        public DataPoint(String this_rt, Double this_value) {
            this.rt = this_rt;
            this.value = this_value;
        }
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<DataPoint> get_data_rt(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            ArrayList<DataPoint> data = new ArrayList<DataPoint>();
            ArrayList<JSONObject> obj = (ArrayList<JSONObject>)jsonObject.get("data");
            for (int i=0; i < obj.size(); i++) {
                JSONObject o = obj.get(i);
                String rt = o.get("rt").toString();
                Double ttc = new Double(o.get("ttc").toString());

                DataPoint dp = new DataPoint(rt, ttc);
                data.add(dp); // if scaling, need to adjust hyperparameters
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
