package com.mist.sandbox;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import io.praesid.livestats.*;
import org.apache.commons.math3.stat.descriptive.*;

import java.io.FileReader;
import java.util.ArrayList;

public class stat_test1 {
    public static void main(String[] args) throws Exception {
        System.out.println("Getting data ...");
        ArrayList<Double> data = get_data("setting1_seq_d0.json");
        System.out.println("Got data: " + data + "\n\n");

        // Get a DescriptiveStatistics instance
        DescriptiveStatistics descr_stats1 = new DescriptiveStatistics();

        // Get a SummaryStatistics instance
        SummaryStatistics summary_stats = new SummaryStatistics();

        // Add the data from the array
        for(int j=1; j < data.size(); j++) {
            descr_stats1.addValue(data.get(j));
            summary_stats.addValue(data.get(j));
        }

        // Compute some statistics on the whole series
        double descr_mean1 = descr_stats1.getMean();
        double descr_std1 = descr_stats1.getStandardDeviation();
        double descr_median1 = descr_stats1.getPercentile(50);
        double descr_min1 = descr_stats1.getMin();
        double descr_max1 = descr_stats1.getMax();

        System.out.println("Apache, whole series: descr_min1 = " + descr_min1 + ", descr_max1 = " + descr_max1 + ", descr_mean1 = "
                + descr_mean1 + " descr_std1 = " + descr_std1 + ", descr_median1 = " + descr_median1);
        
        // Compute some statistics on the whole series
        double sum_mean1 = summary_stats.getMean();
        double sum_std1 = summary_stats.getStandardDeviation();
        double sum_min1 = summary_stats.getMin();
        double sum_max1 = summary_stats.getMax();

        System.out.println("Apache, whole series: sum_min1 = " + sum_min1 + ", sum_max1 = " + sum_max1 + ", sum_mean1 = "
                + sum_mean1 + " sum_std1 = " + sum_std1 + "\n\n");

        // Create a DescriptiveStats instance and set the window size to 100
        DescriptiveStatistics roll_stats = new DescriptiveStatistics();
        LiveStats live = new LiveStats();
        int winsize = 100;
        double roll_mean1 = 0.0D;
        double roll_std1 = 0.0D;
        double roll_min1 = 0.0D;
        double roll_max1 = 0.0D;
        double live_mean = 0.0D;
        double live_std = 0.0D;
        double live_min = 0.0D;
        double live_max = 0.0D;
        long len = 0L;
        roll_stats.setWindowSize(winsize);
        for (int i=0; i < data.size(); i += winsize) {
            for (int j=i; (j < i+winsize) && (j < data.size()); j++) {
                roll_stats.addValue(data.get(j));
                roll_mean1 = roll_stats.getMean();
                roll_std1 = roll_stats.getStandardDeviation();
                roll_min1 = roll_stats.getMin();
                roll_max1 = roll_stats.getMax();
                len = roll_stats.getN();
                
                live.add(data.get(j));
                live_mean = live.mean();
                live_std = Math.sqrt(live.variance());
                live_min = live.minimum();
                live_max = live.maximum();
                System.out.println("Apache, rolling: len = " + len);
            }

            System.out.println("Apache, rolling: len = " + len + ", roll_min1 = " + roll_min1 + ", roll_max1 = " + roll_max1 + ", roll_mean1 = "
                    + roll_mean1 + " roll_std1 = " + roll_std1);
            System.out.println("LiveStats, rolling: live_min = " + live_min + ", live_max = " + live_max + ", live_mean = "
                    + live_mean + " live_std = " + live_std);

            SummaryStatistics summary_stats2 = new SummaryStatistics();
            for (int j=i; (j < i+winsize) && (j < data.size()); j++) {
                summary_stats2.addValue(data.get(j));
            }
            double sum_mean2 = summary_stats2.getMean();
            double sum_std2 = summary_stats2.getStandardDeviation();
            double sum_min2 = summary_stats2.getMin();
            double sum_max2 = summary_stats2.getMax();

            System.out.println("Apache, whole series: sum_min2 = " + sum_min2 + ", sum_max2 = " + sum_max2 + ", sum_mean2 = "
                    + sum_mean2 + " sum_std2 = " + sum_std2 + "\n\n");
        }
        
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
