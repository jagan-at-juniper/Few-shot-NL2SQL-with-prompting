//package com.mist.sandbox;
//
//import com.mist.sandbox.Data.DataPoint;
//import org.apache.commons.math3.stat.StatUtils;
//import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
//import org.knowm.xchart.SwingWrapper;
//import org.knowm.xchart.XYChart;
//import org.knowm.xchart.XYChartBuilder;
//import org.knowm.xchart.XYSeries;
//import org.knowm.xchart.style.markers.SeriesMarkers;
//import org.ojalgo.matrix.store.MatrixStore;
//import org.ojalgo.matrix.store.PhysicalStore;
//import org.ojalgo.matrix.store.PrimitiveDenseStore;
//
//import java.awt.*;
//import java.util.ArrayList;
//import java.util.Date;
//
//import static org.ojalgo.function.PrimitiveFunction.DIVIDE;
//
//public class ONS_with_stats_bak3 {
//    public static enum AnomalyDirection {UP, DOWN, UPDOWN};
//
//    public static void main(String[] args) throws Exception {
//        int std_dev_winsize = 30; // 24; // 48
//        int median_winsize = 5; // 4
//        double oarima_sigma_mult = 3.0;
//        Float lift_sigma_mult = 1.0F;
//
//        // initialize the oarima hyperparams
//        int mk = 8;
//        Double lrate = 0.1;
//        Double epsilon = Math.pow(1, -7);
//
//        System.out.println("Getting data ...");
//        String train_filename = "amazon_sle_ttc_by_site_1h_430_506.json";
//        String test_filename = "amazon_sle_ttc_by_site_1h_430_506.json";
//        String X_axis_title = "10 minutes";
//        int minutes_in_interval = 60;
//        int min_samples_10_minutes = 5;
//        int min_samples = minutes_in_interval / 10 * min_samples_10_minutes;
//
//        ArrayList<DataPoint> train_data = new ArrayList<DataPoint>();
//        ArrayList<DataPoint> test_data = new ArrayList<DataPoint>();
//
//        AnomalyDirection direction = AnomalyDirection.UP;
//        Boolean has_ceiling = false;
//        Boolean has_floor = false;
//        Double ceiling_value = 0.0;
//        Double floor_value = 0.0;
//        String mode = "STC";
//        if (mode.equals("TTC")) {
//            train_data = Data.get_data_ttc(train_filename);
//            test_data = Data.get_data_ttc(test_filename);
//            direction = AnomalyDirection.UP;
//            has_ceiling = true;
//            ceiling_value = 20000.0D;
//            mk = 9;
//            lrate = 0.1;
//            epsilon = Math.pow(1, -7);
//            std_dev_winsize = 100;
//        } else if (mode.equals("STC")) {
//            train_data = Data.get_data_stc(train_filename);
//            test_data = Data.get_data_stc(test_filename);
//            direction = AnomalyDirection.DOWN;
//            has_floor = true;
//            floor_value = 0.0D;
//            mk = 12;
//            lrate = 0.1;
//            epsilon = Math.pow(1, -7);
//            std_dev_winsize = 30;
//        }
//
//        DescriptiveStatistics median_stats = new DescriptiveStatistics();
//        median_stats.setWindowSize(median_winsize);
//        DescriptiveStatistics std_dev_stats = new DescriptiveStatistics();
//        std_dev_stats.setWindowSize(std_dev_winsize);
//
//        ArrayList<Double> medians = get_medians(median_stats, train_data); // train the stats
//
//        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
//        PrimitiveDenseStore w = storeFactory.makeZero(1, mk);
//        for (int i=0; i < mk; i++) w.set(i, 0.1D);
//
//        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);
//
//        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
//        ArrayList<Double> predictions = get_oarima_predictions(train_data, mk, lrate, epsilon, A_trans, w, window, direction); // train oarima
//
//        medians = get_medians(median_stats, test_data); // generate current stats
//        predictions = get_oarima_predictions(test_data, mk, lrate, epsilon, A_trans, w, window, direction); // generate current predictions
//
//        double[] whole_series = new double[test_data.size()];
//        for (int i=0; i < test_data.size(); i++)
//            whole_series[i] = test_data.get(i).dataval;
//        double mean = StatUtils.mean(whole_series);
//        double std_dev = Math.sqrt(StatUtils.variance(whole_series));
//        ArrayList<Double> mean_series = new ArrayList<Double>();
//        ArrayList<Double> std_dev_series = new ArrayList<Double>();
//        for (int i=0; i < test_data.size(); i++) {
//            mean_series.add(mean);
//            if (direction.equals(AnomalyDirection.UP))
//                std_dev_series.add((std_dev * oarima_sigma_mult) + mean);
//            else
//                std_dev_series.add((std_dev * oarima_sigma_mult) - mean);
//        }
//
//        // init the data arrays for actuals
//        ArrayList<Date> dataX = new ArrayList<Date>();
//        ArrayList<Double> dataY = new ArrayList<Double>();
//        for (int i=0; i < test_data.size(); i++) {
//            dataX.add(test_data.get(i).rt);
//            dataY.add(test_data.get(i).dataval);
//        }
//
//        ArrayList<Double> weighted = get_sliding_weighted_mean(test_data, 3);
//
//        ArrayList<Double> confidence_intervals_up = get_confidence_interval_std_dev(test_data, std_dev_stats, predictions,
//                oarima_sigma_mult, AnomalyDirection.UP, has_ceiling, has_floor, ceiling_value, floor_value);
//        ArrayList<Double> confidence_intervals_down = get_confidence_interval_std_dev(test_data, std_dev_stats, predictions,
//                oarima_sigma_mult, AnomalyDirection.DOWN, has_ceiling, has_floor, ceiling_value, floor_value);
//
//        Boolean require_min_samples = false;
//        ArrayList<Double> oarima_anomalies_all = get_oarima_anomalies(test_data, confidence_intervals_up, confidence_intervals_down,
//                require_min_samples, min_samples, direction);
//
//        require_min_samples = true;
//        ArrayList<Double> oarima_anomalies_min_samples = get_oarima_anomalies(test_data, confidence_intervals_up, confidence_intervals_down,
//                require_min_samples, min_samples, direction);
//
//        require_min_samples = false;
//        ArrayList<Double> lift_anomalies_all = get_lift_anomalies(test_data, lift_sigma_mult, require_min_samples, min_samples);
//
//        require_min_samples = true;
//        ArrayList<Double> lift_anomalies_min_samples = get_lift_anomalies(test_data, lift_sigma_mult, require_min_samples, min_samples);
//
//        ArrayList<Double> conf_int = direction.equals(AnomalyDirection.UP) ? confidence_intervals_up : confidence_intervals_down;
//
//        draw_chart(test_filename, X_axis_title, oarima_anomalies_all, oarima_anomalies_min_samples,
//                lift_anomalies_all, lift_anomalies_min_samples,
//                dataX, dataY, medians, conf_int, predictions, min_samples, mean_series, std_dev_series);
//    }
//
//    public static ArrayList<Double> get_sliding_weighted_mean(ArrayList<DataPoint> data, int winsize) {
//        ArrayList<Double> result = new ArrayList<Double>();
//        for (int i=0; i < winsize; i++)
//            result.add(data.get(i).dataval);
//
//        for (int i=winsize; i < data.size(); i++) {
//            Double val = 0.0D;
//            int n = 0;
//            for (int j=0; j < winsize; j++) {
//                DataPoint dp = data.get(i - j);
//                val += dp.dataval * dp.attempts;
//                n += dp.attempts;
//            }
//            result.add(val / n);
//        }
//        return result;
//    }
//
//    public static ArrayList<Double> get_oarima_anomalies(ArrayList<DataPoint> data,
//                                                         ArrayList<Double> conf_int_up, ArrayList<Double> conf_int_down,
//                                                         Boolean require_min_samples, int min_samples,
//                                                         AnomalyDirection direction) {
//        ArrayList<Double> anomalies = new ArrayList<Double>();
//        for (int i=0; i < data.size(); i++) {
//            Boolean added_one = false;
//            int attempts = data.get(i).attempts;
//            if (direction.equals(AnomalyDirection.UP) || direction.equals(AnomalyDirection.UPDOWN)) {
//                Double conf_thresh_up = conf_int_up.get(i);
//                if (data.get(i).dataval >= conf_thresh_up) {
//                    if ((require_min_samples && (attempts > min_samples)) || (require_min_samples == false)) {
//                        anomalies.add(data.get(i).dataval);
//                        added_one = true;
//                    }
//                }
//            }
//            if (direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UPDOWN)) {
//                Double conf_thresh_down = conf_int_down.get(i);
//                if (data.get(i).dataval <= conf_thresh_down) {
//                    if ((require_min_samples && (attempts > min_samples)) || (require_min_samples == false)) {
//                        anomalies.add(data.get(i).dataval);
//                        added_one = true;
//                    }
//                }
//            }
//            if (added_one == false)
//                anomalies.add(null);
//        }
//        return anomalies;
//    }
//
//    public static ArrayList<Double> get_lift_anomalies(ArrayList<DataPoint> data, Float lift_sigma_mult,
//                                                       Boolean require_min_samples, int min_samples) {
//        ArrayList<Double> lift_anomalies = new ArrayList<Double>();
//        AnomalyDetectLift detector = new AnomalyDetectLift();
//        for (int i=0; i < data.size(); i++) {
//            Float variance = data.get(i).variance;
//            Integer base_attempts = data.get(i).base_bad + data.get(i).base_good;
//            ArrayList<Float> baseline_series = detector.CreateBaseline(data.get(i).base_bad, base_attempts, variance);
//
//            Float current_value = detector.CreateCurrentValue(data.get(i).bad, data.get(i).attempts);
//
//            detector.Prepare(baseline_series, current_value, lift_sigma_mult);
//
//            if (detector.IsAnomaly()) {
//                if ((require_min_samples && (data.get(i).attempts > min_samples)) || (require_min_samples == false))
//                    lift_anomalies.add(data.get(i).dataval);
//                else
//                    lift_anomalies.add(null);
//            } else {
//                lift_anomalies.add(null);
//            }
//        }
//        return lift_anomalies;
//    }
//
//    public static ArrayList<Double> get_oarima_predictions(ArrayList<DataPoint> data, int mk, Double lrate, Double epsilon,
//                                                           PrimitiveDenseStore A_trans, PrimitiveDenseStore w, PrimitiveDenseStore window,
//                                                           AnomalyDirection direction) {
//        // initialize the ojalgo matrices
//        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
//        A_trans.multiply(epsilon);
//
//        ArrayList<Double> predictions = new ArrayList<Double>();
//        for (int i=0; i < data.size(); i++) {
//            window.set(i % mk, data.get(i).dataval); // load the window
//
//            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
//            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
//            prediction = direction.equals(AnomalyDirection.UP) ? Math.max(prediction, 0.0) : Math.min(prediction, 1.0);
//            predictions.add(prediction);
//
//            // diff = prediction - data(i); # scalar: absolute error
//            Double diff = prediction - data.get(i).dataval;
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
//        }
//
//        return predictions;
//    }
//
//    public static ArrayList<Double> get_medians(DescriptiveStatistics stats, ArrayList<DataPoint> data) {
//        ArrayList<Double> medians = new ArrayList<Double>();
//
//        for (int i=0; i < data.size(); i++) {
//            stats.addValue(data.get(i).dataval);
//            double median = stats.getPercentile(50.0);
//            medians.add(median);
//        }
//        return medians;
//    }
//
//    public static ArrayList<Double> get_confidence_interval_std_dev(ArrayList<DataPoint> data, DescriptiveStatistics stats,
//                                                            ArrayList<Double> predictions,
//                                                            double sigma_mult, AnomalyDirection direction,
//                                                            Boolean ceiling, Boolean floor, Double ceiling_value, Double floor_value) {
//        assert direction.equals(AnomalyDirection.DOWN) || direction.equals(AnomalyDirection.UP);
//
//        ArrayList<Double> confidence_interval = new ArrayList<Double>();
//
//        double conf_int = 0.0;
//        for (int i=0; i < data.size(); i++) {
//            stats.addValue(data.get(i).dataval);
//            double interval = stats.getStandardDeviation() * sigma_mult;
//
//            if (direction.equals(AnomalyDirection.UP)) {
//                conf_int = predictions.get(i) + interval;
//                conf_int = ceiling ? Math.min(ceiling_value, conf_int) : conf_int;
//                confidence_interval.add(conf_int);
//            } else if (direction.equals(AnomalyDirection.DOWN)) {
//                conf_int = predictions.get(i) - interval;
//                conf_int = floor ? Math.max(floor_value, conf_int) : conf_int;
//                confidence_interval.add(conf_int);
//            }
//        }
//        return confidence_interval;
//    }
//
//    public static void draw_chart(String filename, String x_axis_title,
//                                  ArrayList<Double> anomalies_all, ArrayList<Double> anomalies_min,
//                                  ArrayList<Double> lift_anomalies_all, ArrayList<Double> lift_anomalies_min,
//                                  ArrayList<Date> dataX, ArrayList<Double> dataY,
//                                  ArrayList<Double> medians, ArrayList<Double> conf_intervals, ArrayList<Double> predictions,
//                                  int min_samples, ArrayList<Double> means, ArrayList<Double> std_devs) {
//        // Create Chart
//        XYChart chart = new XYChartBuilder().title("OARIMA Results: " + filename).xAxisTitle(x_axis_title).yAxisTitle("Median TTC (ms)").build();
//        XYSeries actual_series = chart.addSeries("Actual", dataX, dataY);
//        actual_series.setLineColor(Color.GREEN);
//        actual_series.setMarker(SeriesMarkers.NONE);
//        XYSeries predict_series = chart.addSeries("Predicted", dataX, predictions);
//        predict_series.setLineColor(Color.RED);
//        predict_series.setMarker(SeriesMarkers.NONE);
//        XYSeries high_series = chart.addSeries("Confidence Interval", dataX, conf_intervals);
//        XYSeries median_series = chart.addSeries("Median", dataX, medians);
//        median_series.setLineColor(Color.BLUE);
//        median_series.setMarker(SeriesMarkers.NONE);
//        high_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Area);
//        high_series.setLineColor(Color.BLACK);
//        high_series.setLineWidth(2.0F);
//        high_series.setMarker(SeriesMarkers.NONE);
//        XYSeries anomaly_all_series = chart.addSeries("OARIMA Anomalies (All)", dataX, anomalies_all);
//        anomaly_all_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        anomaly_all_series.setMarker(SeriesMarkers.CIRCLE);
//        anomaly_all_series.setMarkerColor(Color.LIGHT_GRAY);
//        XYSeries anomaly_min_series = chart.addSeries("OARIMA Anomalies (Min " + min_samples + " Samples)", dataX, anomalies_min);
//        anomaly_min_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        anomaly_min_series.setMarker(SeriesMarkers.CIRCLE);
//        anomaly_min_series.setMarkerColor(Color.RED);
//        XYSeries lift_anomaly_all_series = chart.addSeries("Lift Anomalies (All)", dataX, lift_anomalies_all);
//        lift_anomaly_all_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        lift_anomaly_all_series.setMarker(SeriesMarkers.TRIANGLE_UP);
//        lift_anomaly_all_series.setMarkerColor(Color.GRAY);
//        XYSeries lift_anomaly_min_series = chart.addSeries("Lift Anomalies(Min " + min_samples + " Samples)", dataX, lift_anomalies_min);
//        lift_anomaly_min_series.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        lift_anomaly_min_series.setMarker(SeriesMarkers.TRIANGLE_UP);
//        lift_anomaly_min_series.setMarkerColor(Color.GREEN);
//        XYSeries mean_series = chart.addSeries("Mean of Whole Series", dataX, means);
//        mean_series.setLineColor(Color.pink);
//        mean_series.setMarker(SeriesMarkers.NONE);
//        XYSeries std_dev_series = chart.addSeries("Sigma of Whole Series", dataX, std_devs);
//        std_dev_series.setLineColor(Color.CYAN);
//        std_dev_series.setMarker(SeriesMarkers.NONE);
//
//        chart.getStyler().setMarkerSize(16);
//        chart.getStyler().setDatePattern("hh:mm");
//
//        // Show it
//        new SwingWrapper<XYChart>(chart).displayChart();
//    }
//}
