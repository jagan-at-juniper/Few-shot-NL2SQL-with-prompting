package com.mist.sandbox;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Data {
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

    public static ArrayList<Double> get_doubles_list(ArrayList<DataPoint> data) {
        ArrayList<Double> result = new ArrayList<Double>();
        for (DataPoint dp : data)
            result.add(dp.dataval);
        return result;
    }
}
