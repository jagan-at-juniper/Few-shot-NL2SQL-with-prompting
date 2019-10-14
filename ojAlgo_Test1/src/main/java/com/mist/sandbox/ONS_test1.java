package com.mist.sandbox;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import io.praesid.livestats.*;
import org.apache.commons.math3.stat.descriptive.*;

import java.awt.*;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

import org.knowm.xchart.*;

public class ONS_test1 {
    public static void main(String[] args) throws Exception {
        System.out.println("Getting data ...");
        ArrayList<Double> data = get_data("sle_ttc_anomaly_by_site_1h_jons.json");
        System.out.println("Got data: " + data);
        int len = data.size();

        // initialize the oarima hyperparams
        // Office Staging - Best: Median RMSE = 174.64390437800841, mk: 9, lrate: 60.940000000000005, epsilon: 3.162277660168379E-6
        // Office Staging - Best: Mean RMSE = 240.73461917645778, mk: 10, lrate: 70.93, epsilon: 3.162277660168379E-6
        // Jon's VNA Best: Mean RMSE = 397.3392089175923, mk: 1, lrate: 990.0100000000007, epsilon: 3.162277660168379E-6
        // Jon's VNA Best: Median RMSE = 126.13751173557213, mk: 1, lrate: 860.1400000000006, epsilon: 3.162277660168379E-6
        int mk = 20;
        Double lrate = 70.93;
        Double epsilon = Math.pow(3.16, -6);

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
        Double SE = 0.0D;

        PrimitiveDenseStore A_trans = storeFactory.makeEye(mk, mk);
        A_trans.multiply(epsilon);

        PrimitiveDenseStore window = storeFactory.makeZero(1, mk);

        for (int i=mk; i < len; i++) {
            int window_idx = 0;
            for (int j=i-mk; j < i; j++) window.set(window_idx++, data.get(j)); // load the window

            // prediction = w * data(i-mk:i-1)' # scalar: next prediction
            Double prediction = w.multiply(window.transpose()).doubleValue(0, 0);
            predictions[i] = prediction;

            // diff = prediction - data(i); # scalar: absolute error
            Double diff = prediction - data.get(i);

            // grad = 2 * data(i-mk:i-1) * diff; # row vector: gradient?
            MatrixStore grad = window.multiply(2.0D).multiply(diff); // row vector: gradient?

            // A_trans = A_trans - A_trans * grad' * grad * A_trans/(1 + grad * A_trans * grad')
            PrimitiveDenseStore A_trans_div = A_trans.copy(); // copy because we modify A_trans in place for A_trans_div
            // A_trans_div = A_trans/(1 + grad * A_trans * grad') from the equation above
            A_trans_div.modifyAll(DIVIDE.second(1.0D + grad.multiply(A_trans).multiply(grad.transpose()).doubleValue(0, 0)));
            A_trans = (PrimitiveDenseStore)A_trans.subtract(A_trans.multiply(grad.transpose()).multiply(grad).multiply(A_trans_div));

            // w = w - lrate * grad * A_trans ; # update weights
            w = (PrimitiveDenseStore)w.subtract(A_trans.multiply(grad).multiply(lrate));

            diffs_list.add(diff);

            // SE = SE + diff^2; # squared error
            SE += Math.pow(diff, 2);

            // MSE = SE / i
            Double MSE = SE / diffs_list.size();

            // rMSE = sqrt(MSE)
            Double RMSE = Math.sqrt(MSE);

            Double window_SE = 0.0D;
            Double window_RMSE = 0.0D;
            if (diffs_list.size() > mk) {
                for (int wi = diffs_list.size() - mk; wi < diffs_list.size(); wi++) {
                    window_SE += Math.pow(diffs_list.get(wi), 2);
                }
                window_RMSE = Math.sqrt(window_SE / mk);
            }

            System.out.println("real = " + data.get(i) + ", prediction = " + prediction + ", diff = " + diff + ", whole RMSE = " + RMSE + ", window RMSE = " + window_RMSE);
        }

        // Create Chart
        XYChart chart = QuickChart.getChart("OARIMA Results", "Time", "Value", "Actual", dataX, dataY);
        XYSeries series = chart.addSeries("Predicted", dataX, predictions);
        series.setLineColor(Color.green);
        series.setMarker(SeriesMarkers.NONE);

        // Show it
        new SwingWrapper(chart).displayChart();

        System.out.println("Max diff = " + Collections.max(diffs_list));
        Double diffsum = 0.0D;
        for (Double diff:diffs_list) diffsum += diff;
        Double meandiff = diffsum / diffs_list.size();
        System.out.println("Mean diff = " + meandiff);
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<Double> get_data(String filename) {
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            JSONParser parser = new JSONParser();
//            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(
//                    "C:/Users/rcrowe/Documents/repos/ojAlgo_Test1/src/main/java/com/mist/sandbox/" + filename));
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
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
