package com.mist.sandbox;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import java.awt.*;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

public class ONS_with_stats_bak1 {
    public static void main(String[] args) throws Exception {
        System.out.println("Getting data ...");
        String filename = "ttc_site_10m_office_stage_5_4.json";
        String X_axis_title = "1 hour";
        ArrayList<DataPoint> data = get_data_advanced(filename);
        System.out.println("Got data: " + data);
        int len = data.size();

        DescriptiveStatistics stats_long = new DescriptiveStatistics();
        DescriptiveStatistics stats_short = new DescriptiveStatistics();
        int long_winsize = 48;
        int short_winsize = 4;
        double sigma_mult = 3.0;
        ArrayList<Double> std_dev_high_list = new ArrayList<Double>();
        ArrayList<Double> anomaliesY = new ArrayList<Double>();
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
        ArrayList<Date> dataX = new ArrayList<Date>();
        ArrayList<Double> dataY = new ArrayList<Double>();
        ArrayList<Double> predictions = new ArrayList<Double>();
        ArrayList<Double> medians = new ArrayList<Double>();

        for (int i=0; i < len; i++) {
            dataX.add(data.get(i).rt);
            dataY.add(data.get(i).median_ttc_ms);
        }

        ArrayList<Double> diffs_list = new ArrayList();
        Double SE = 0.0D;

        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
        A_trans.multiply(epsilon);

        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);

        for (int i=0; i < mk; i++) { // initialize to fill the first window
            predictions.add(0.0D);
            medians.add(0.0D);
            std_dev_high_list.add(0.0D);
            anomaliesY.add(null);
        }

        for (int i=mk; i < len; i++) {
            int window_idx = 0;
            for (int j=i-mk; j < i; j++) {
                window.set(window_idx++, data.get(j).median_ttc_ms); // load the window
            }

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
            predictions.add(prediction);

            // diff = prediction - data(i); # scalar: absolute error
            Double diff = prediction - data.get(i).median_ttc_ms;

            // grad = 2 * data(i-mk:i-1) * diff; # row vector: gradient?
            MatrixStore grad = window.multiply(2.0D).multiply(diff); // row vector: gradient?

            // A_trans = A_trans - A_trans * grad' * grad * A_trans/(1 + grad * A_trans * grad')
            PrimitiveDenseStore A_trans_div = A_trans.copy(); // copy because we modify A_trans in place for A_trans_div
            // A_trans_div = A_trans/(1 + grad * A_trans * grad') from the equation above
            A_trans_div.modifyAll(DIVIDE.second(1.0D + grad.multiply(A_trans).multiply(grad.transpose()).doubleValue(0, 0)));
            A_trans = (PrimitiveDenseStore)A_trans.subtract(A_trans.multiply(grad.transpose()).multiply(grad).multiply(A_trans_div));

            // w = w - lrate * grad * A_trans ; # update weights
            w = (PrimitiveDenseStore)w.subtract(A_trans.multiply(grad).multiply(lrate));

            stats_short.addValue(data.get(i).median_ttc_ms);
            double median = stats_short.getPercentile(50.0);
            medians.add(median);

            stats_long.addValue(data.get(i).median_ttc_ms);
            double conf_interval = stats_long.getStandardDeviation() * sigma_mult;
            std_dev_high_list.add(Math.max(prediction, median) + conf_interval);

            if (data.get(i).median_ttc_ms > std_dev_high_list.get(i)) {
                anomaliesY.add(data.get(i).median_ttc_ms);
            } else {
                anomaliesY.add(null);
            }
        }

        // Create Chart
        XYChart chart = new XYChartBuilder().title("OARIMA Results: " + filename).xAxisTitle(X_axis_title).yAxisTitle("Median TTC (ms)").build();
        XYSeries actual_series = chart.addSeries("Actual", dataX, dataY);
        actual_series.setLineColor(Color.GREEN);
        actual_series.setMarker(SeriesMarkers.NONE);
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
        XYSeries anomaly_series = chart.addSeries("Anomalies", dataX, anomaliesY);
        anomaly_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        anomaly_series.setMarker(SeriesMarkers.CIRCLE);
        anomaly_series.setMarkerColor(Color.red);
        chart.getStyler().setMarkerSize(16);
        chart.getStyler().setDatePattern("hh:mm");

        // Show it
        new SwingWrapper<XYChart>(chart).displayChart();
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<Double> get_data_basic(String filename) {
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

    // rt	dataval	bad	good	base_bad	base_good	variance
    // 2018-05-03 20:10:00.000+0000	378.0	0	2	26	585	0.040809
    public static class DataPoint {
        public Date rt;
        public Double median_ttc_ms;
        public Integer ttc_above_thresh;
        public Integer ttc_below_thresh;
        public Integer base_ttc_above;
        public Integer base_ttc_below;
        public Double ttc_variance;
        public DataPoint(Date _rt, Double _ttc, Integer _above, Integer _below,
                         Integer _base_above, Integer _base_below, Double _variance) {
            this.rt = _rt;
            this.median_ttc_ms = _ttc;
            this.ttc_above_thresh = _above;
            this.ttc_below_thresh = _below;
            this.base_ttc_above = _base_above;
            this.base_ttc_below = _base_below;
            this.ttc_variance = _variance;
        }
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<DataPoint> get_data_advanced(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            ArrayList<DataPoint> data = new ArrayList<DataPoint>();
            ArrayList<JSONObject> obj = (ArrayList<JSONObject>)jsonObject.get("data");
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (int i=0; i < obj.size(); i++) {
                JSONObject j = obj.get(i);
                Date rt = formatter.parse(j.get("rt").toString());
                Double median_ttc_ms = new Double(j.get("dataval").toString());
                Integer ttc_above_thresh = new Integer(j.get("bad").toString());
                Integer ttc_below_thresh = new Integer(j.get("good").toString());
                Integer base_ttc_above = new Integer(j.get("base_bad").toString());
                Integer base_ttc_below = new Integer(j.get("base_good").toString());
                Double ttc_variance = new Double(j.get("variance").toString());

                data.add(new DataPoint(rt, median_ttc_ms, ttc_above_thresh, ttc_below_thresh,
                        base_ttc_above, base_ttc_below, ttc_variance)); // if scaling, need to adjust hyperparameters
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
