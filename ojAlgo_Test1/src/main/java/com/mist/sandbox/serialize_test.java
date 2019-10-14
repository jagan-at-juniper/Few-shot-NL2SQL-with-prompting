package com.mist.sandbox;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mist.sandbox.Data.DataPoint;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
//import org.json.simple.JSONObject;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.markers.SeriesMarkers;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;
import com.alibaba.fastjson.JSON;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Date;

import com.mist.sandbox.AnomalyOarima;

import javax.xml.bind.DatatypeConverter;

import static org.ojalgo.function.PrimitiveFunction.DIVIDE;

public class serialize_test {
    public static String anySerialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return DatatypeConverter.printBase64Binary(baos.toByteArray());
    }

    public static Object anyDeserialize(String s) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(s));
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    public static String PrimitiveDenseStoreToJsonString(PrimitiveDenseStore store) {
        assert store != null : "PrimitiveDenseStoreToJsonString: PrimitiveDenseStore was null";
        String str = store.toString();
        return str.substring(str.indexOf("{"))
                .replace("\n", "")
                .replace("\t", "")
                .replace("{", "[")
                .replace("}", "]");
    }

    public static PrimitiveDenseStore PrimitiveDenseStoreFromJsonString(String json_string) {
        assert json_string != null : "PrimitiveDenseStoreFromJsonString: JSON string was null";
        assert json_string.length() > 0 : "PrimitiveDenseStoreFromJsonString: JSON string was empty";

        JSONArray store_array = JSON.parseObject(json_string, JSONArray.class);
        assert store_array != null : "PrimitiveDenseStoreFromJsonString: JSONArray was null";

        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory = PrimitiveDenseStore.FACTORY;
        long rows = store_array.size();
        long cols = store_array.getJSONArray(0).size();
        PrimitiveDenseStore store = storeFactory.makeZero(rows, cols);
        for (int i=0; i < rows; i++) {
            for (int j=0; j < cols; j++) {
                store.set(i, j, new Double(store_array.getJSONArray(i).get(j).toString()).doubleValue());
            }
        }
        return store;
    }

    public static void main(String[] args) throws Exception {
        AnomalyOarima whole_oarima = new AnomalyOarima("id", 10, 0.1, 0.0001, 10);
        AnomalyOarima.OarimaModel model = whole_oarima.model;

        String stat_str = anySerialize(model.stats);

        DescriptiveStatistics stats = (DescriptiveStatistics) anyDeserialize(stat_str);

        String A_trans_str = PrimitiveDenseStoreToJsonString(model.A_trans);
        PrimitiveDenseStore A_trans = PrimitiveDenseStoreFromJsonString(A_trans_str);

        if (A_trans.equals(model.A_trans))
            System.out.println("It even equals!");
        else
            System.out.println("Nope");
    }
}
