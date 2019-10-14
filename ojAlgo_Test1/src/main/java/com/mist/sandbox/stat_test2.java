package com.mist.sandbox;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.ArrayList;

import org.knowm.xchart.*;

public class stat_test2 {
    public static void main(String[] args) throws Exception {
        System.out.println("Getting data ...");
        ArrayList<Double> data = get_data("sle_ttc_anomaly_by_site_1h.json");
        System.out.println("Got data: " + data + "\n\n");

        // Create a DescriptiveStats instance and set the window size to 100
        DescriptiveStatistics roll_stats = new DescriptiveStatistics();
        int winsize = 100;
        double roll_mean = 0.0D;
        double roll_std = 0.0D;
        double roll_min = 0.0D;
        double roll_max = 0.0D;
        long len = 0L;
        roll_stats.setWindowSize(winsize);
        for (int i=0; i < data.size(); i += winsize) {
            for (int j=i; (j < i+winsize) && (j < data.size()); j++) {
                roll_stats.addValue(data.get(j));
                roll_mean = roll_stats.getMean();
                roll_std = roll_stats.getStandardDeviation();
                roll_min = roll_stats.getMin();
                roll_max = roll_stats.getMax();
                len = roll_stats.getN();
            }

            System.out.println("Apache, rolling: len = " + len + ", roll_min = " + roll_min + ", roll_max = " + roll_max + ", roll_mean = "
                    + roll_mean + " roll_std = " + roll_std);
        }
        double[] dataX = new double[data.size()];
        double[] dataY = new double[data.size()];
        for (int i=0; i < data.size(); i++) {
            dataX[i] = i;
            dataY[i] = data.get(i);
        }

        // Create Chart
        XYChart chart = QuickChart.getChart("OARIMA Results", "Time", "Value", "Actual (ms)", dataX, dataY);
//        XYSeries series = chart.addSeries("Predicted", dataX, predictions);
//        series.setLineColor(Color.green);
//        series.setMarker(SeriesMarkers.NONE);

        // Show it
        new SwingWrapper(chart).displayChart();
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<Double> get_data(String filename) {
        try {
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(
                    "C:/Users/rcrowe/Documents/repos/ojAlgo_Test1/src/main/java/com/mist/sandbox/" + filename));
            ArrayList<Double> data = new ArrayList<Double>();
            ArrayList<Object> obj = (ArrayList<Object>)jsonObject.get("data");
            for (int i=0; i < obj.size(); i++)
                data.add(Double.valueOf(obj.get(i).toString()));
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
