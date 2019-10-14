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

public class ONS_with_stats_bak2 {
    public static enum AnomalyDirection {UP, DOWN, UPDOWN};

    public static void main(String[] args) throws Exception {
        int std_dev_winsize = 48; // 48
        int median_winsize = 4; // 4
        int mk = 7;
        double oarima_sigma_mult = 3.0;
        Float lift_sigma_mult = 1.0F;

        // initialize the oarima hyperparams
        Double lrate = 0.1;
        Double epsilon = Math.pow(1, -7);

        System.out.println("Getting data ...");
        String filename = "site_10m_office_stage_5_8.json";
        String X_axis_title = "10 minutes";

        ArrayList<DataPoint> data = new ArrayList<DataPoint>();
        AnomalyDirection direction = AnomalyDirection.UP;
        String mode = "TTC";

        if (mode.equals("TTC")) {
            data = get_data_ttc(filename);
            direction = AnomalyDirection.UP;
        } else if (mode.equals("STC")) {
            data = get_data_stc(filename);
            direction = AnomalyDirection.DOWN;
        }

        // init the data arrays for actuals
        ArrayList<Date> dataX = new ArrayList<Date>();
        ArrayList<Double> dataY = new ArrayList<Double>();
        for (int i=0; i < data.size(); i++) {
            dataX.add(data.get(i).rt);
            dataY.add(data.get(i).dataval);
        }

        ArrayList<Double> medians = get_medians(data, median_winsize);
        ArrayList<Double> predictions = get_oarima_predictions(data, mk, lrate, epsilon);

        ArrayList<Double> confidence_intervals_up = get_confidence_interval(data, std_dev_winsize, predictions,
                medians, oarima_sigma_mult, AnomalyDirection.UP);
        ArrayList<Double> confidence_intervals_down = get_confidence_interval(data, std_dev_winsize, predictions,
                medians, oarima_sigma_mult, AnomalyDirection.DOWN);

        Boolean require_min_samples = false;
        int min_samples = 5;
        ArrayList<Double> oarima_anomalies_all = get_oarima_anomalies(data, confidence_intervals_up, confidence_intervals_down,
                require_min_samples, min_samples, direction);

        require_min_samples = true;
        min_samples = 5;
        ArrayList<Double> oarima_anomalies_min_samples = get_oarima_anomalies(data, confidence_intervals_up, confidence_intervals_down,
                require_min_samples, min_samples, direction);

        require_min_samples = false;
        min_samples = 5;
        ArrayList<Double> lift_anomalies_all = get_lift_anomalies(data, lift_sigma_mult, require_min_samples, min_samples);

        require_min_samples = true;
        min_samples = 5;
        ArrayList<Double> lift_anomalies_min_samples = get_lift_anomalies(data, lift_sigma_mult, require_min_samples, min_samples);

        ArrayList<Double> conf_int = direction.equals(AnomalyDirection.UP) ? confidence_intervals_up : confidence_intervals_down;

        draw_chart(filename, X_axis_title, oarima_anomalies_all, oarima_anomalies_min_samples,
                lift_anomalies_all, lift_anomalies_min_samples,
                dataX, dataY, medians, conf_int, predictions, min_samples);
    }

    public static ArrayList<Double> get_oarima_anomalies(ArrayList<DataPoint> data,
                                                         ArrayList<Double> conf_int_up, ArrayList<Double> conf_int_down,
                                                         Boolean require_min_samples, int min_samples,
                                                         AnomalyDirection direction) {
        ArrayList<Double> anomalies = new ArrayList<Double>();
        for (int i=0; i < data.size(); i++) {
            Boolean added_one = false;
            int attempts = data.get(i).attempts;
            if (direction.equals(AnomalyDirection.UP) || direction.equals(AnomalyDirection.UPDOWN)) {
                Double conf_thresh_up = conf_int_up.get(i);
                if (data.get(i).dataval > conf_thresh_up) {
                    if ((require_min_samples && (attempts > min_samples)) || (require_min_samples == false)) {
                        anomalies.add(data.get(i).dataval);
                        added_one = true;
                    }
                }
            }
            if (direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UPDOWN)) {
                Double conf_thresh_down = conf_int_down.get(i);
                if (data.get(i).dataval < conf_thresh_down) {
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

    public static ArrayList<Double> get_oarima_predictions(ArrayList<DataPoint> data, int mk, Double lrate, Double epsilon) {
        // initialize the ojalgo matrices
        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        PrimitiveDenseStore w = storeFactory.makeZero(1, mk);
        for (int i=0; i < mk; i++) w.set(i, 0.1D);

        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
        A_trans.multiply(epsilon);

        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);

        ArrayList<Double> predictions = new ArrayList<Double>();
        for (int i=0; i < mk; i++) { // initialize to fill the first window
            predictions.add(0.0D);
        }

        for (int i=mk; i < data.size(); i++) {
            int window_idx = 0;
            for (int j=i-mk; j < i; j++) { // TODO can't do it this way in production
                window.set(window_idx++, data.get(j).dataval); // load the window
            }

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
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

    public static ArrayList<Double> get_medians(ArrayList<DataPoint> data, int winsize) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        stats.setWindowSize(winsize);
        ArrayList<Double> medians = new ArrayList<Double>();

        for (int i=0; i < data.size(); i++) {
            stats.addValue(data.get(i).dataval);
            double median = stats.getPercentile(50.0);
            medians.add(median);
        }
        return medians;
    }

    public static ArrayList<Double> get_confidence_interval(ArrayList<DataPoint> data, int winsize,
                                                            ArrayList<Double> predictions, ArrayList<Double> medians,
                                                            double sigma_mult, AnomalyDirection direction) {
        assert direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UP);

        DescriptiveStatistics stats = new DescriptiveStatistics();
        stats.setWindowSize(winsize);
        ArrayList<Double> confidence_interval = new ArrayList<Double>();

        double conf_int = 0.0;
        for (int i=0; i < data.size(); i++) {
            stats.addValue(data.get(i).dataval);
            double interval = stats.getStandardDeviation() * sigma_mult;
//            confidence_interval.add(stats.getStandardDeviation()); // For getting the std dev only
            if (direction.equals(AnomalyDirection.UP)) {
                conf_int = Math.max(predictions.get(i), medians.get(i)) + interval;
                confidence_interval.add(conf_int);
            } else if (direction.equals(AnomalyDirection.DOWN)) {
                conf_int = Math.min(predictions.get(i), medians.get(i)) - interval;
                confidence_interval.add(conf_int);
            } // For UPDOWN generate two different lists
        }
        return confidence_interval;
    }

    public static void draw_chart(String filename, String x_axis_title,
                                  ArrayList<Double> anomalies_all, ArrayList<Double> anomalies_min,
                                  ArrayList<Double> lift_anomalies_all, ArrayList<Double> lift_anomalies_min,
                                  ArrayList<Date> dataX, ArrayList<Double> dataY,
                                  ArrayList<Double> medians, ArrayList<Double> conf_intervals, ArrayList<Double> predictions,
                                  int min_samples) {
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
        chart.getStyler().setMarkerSize(16);
        chart.getStyler().setDatePattern("hh:mm");

        // Show it
        new SwingWrapper<XYChart>(chart).displayChart();
    }

    // rt	dataval	bad	good	base_bad	base_good	variance
    // 2018-05-03 20:10:00.000+0000	378.0	0	2	26	585	0.040809
    public static class DataPoint {
        public Date rt;
        public Double dataval;
        public Integer attempts;
        public Integer bad;
        public Integer good;
        public Integer base_bad;
        public Integer base_good;
        public Float variance;
        public DataPoint(Date _rt, Double _val, Integer _bad, Integer _good,
                         Integer _base_bad, Integer _base_good, Float _variance) {
            this.rt = _rt;
            this.dataval = _val;
            this.bad = _bad;
            this.good = _good;
            this.attempts = _bad + _good;
            this.base_bad = _base_bad;
            this.base_good = _base_good;
            this.variance = _variance;
        }
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<DataPoint> get_data_ttc(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            ArrayList<DataPoint> data = new ArrayList<DataPoint>();
            ArrayList<JSONObject> obj = (ArrayList<JSONObject>)jsonObject.get("data");
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (int i=0; i < obj.size(); i++) {
                JSONObject j = obj.get(i);
                Date rt = formatter.parse(j.get("rt").toString().replace(".000+0000", ""));
                Double dataval = new Double(j.get("median_ttc_ms").toString());
                Integer bad = new Integer(j.get("ttc_above_thresh").toString());
                Integer good = new Integer(j.get("ttc_below_thresh").toString());
                Integer base_bad = new Integer(j.get("base_ttc_above").toString());
                Integer base_good = new Integer(j.get("base_ttc_below").toString());
                Float variance = new Float(j.get("ttc_variance").toString());

                data.add(new DataPoint(rt, dataval, bad, good, base_bad, base_good, variance));
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<DataPoint> get_data_stc(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            ArrayList<DataPoint> data = new ArrayList<DataPoint>();
            ArrayList<JSONObject> obj = (ArrayList<JSONObject>)jsonObject.get("data");
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (int i=0; i < obj.size(); i++) {
                JSONObject j = obj.get(i);
                Date rt = formatter.parse(j.get("rt").toString().replace(".000+0000", ""));
                Double connects = new Double(j.get("connects").toString());
                Double not_connects = new Double(j.get("not_connects").toString());
                Double attempts = connects + not_connects;
                Double dataval = connects / attempts;
                Integer bad = not_connects.intValue();
                Integer good = connects.intValue();
                Integer base_bad = new Integer(j.get("base_not_connects").toString());
                Integer base_good = new Integer(j.get("base_connects").toString());
                Float variance = new Float(j.get("stc_variance").toString());

                data.add(new DataPoint(rt, dataval, bad, good, base_bad, base_good, variance));
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
