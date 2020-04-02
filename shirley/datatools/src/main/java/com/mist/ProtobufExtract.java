package com.mist;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.mist.mobius.common.MistUtils;
import com.mist.mobius.generated.ApstatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalytics;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;
import scala.Tuple2;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mist.ClientDataIterator.mac2wcid;

public class ProtobufExtract {
    private static final long serialVersionUID = 1L;
    private transient AmazonS3 s3client;

    //spark-submit  --master yarn --jars data-tools-1.0-SNAPSHOT.jar --class com.mist.ProtobufExtract ./data-tools-1.0-SNAPSHOT.jar
    //


    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App");

        sparkConf.set("spark.serializer", KryoSerializer.class.getName());

        List<Class<?>> clses = Arrays.<Class<?>>asList(ApstatsAnalyticsRaw.APStats.class,
                ApstatsAnalyticsRaw.APStats.class,
                ClientDataIterator.class);
        sparkConf.registerKryoClasses((Class<?>[]) clses.toArray());


        SparkSession spark = SparkSession
                .builder()
                .appName("Back Fill Data")
                .config(sparkConf)
                .getOrCreate();


        Dataset<Row> clientDs = spark.sqlContext().read().parquet(
                "s3://mist-secorapp-production/client-stats-analytics/client-stats-analytics-production/dt=2020-03-15/hr=01/1_0_00000000013347518454.parquet");
        final StructType ss = clientDs.toDF().schema();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaPairRDD<org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.BytesWritable> rawRDD =
                jsc.sequenceFile(
                        "s3://mist-production-kafka-reseed/ap-stats-full-production/20200301/19/026c6021-8ac4-4712-bb6a-2a0abd5491ab/9a675073-23f9-49c9-bb4d-78d750fef1f3",
                      org.apache.hadoop.io.BytesWritable.class, org.apache.hadoop.io.BytesWritable.class);


        JavaRDD<ClientStatsAnalytics.ClientStatsAnalyticsOut> newRdd = rawRDD.map(
                new Function<Tuple2<BytesWritable, BytesWritable>, ApstatsAnalyticsRaw.APStats>() {
                    public ApstatsAnalyticsRaw.APStats call(Tuple2<BytesWritable, BytesWritable> v1) throws Exception {
                        // convert each record in sequence file to protobuf java class
                        BytesWritable bytesWritable = v1._2;
                        ByteString bs = ByteString.copyFrom(bytesWritable.getBytes(), 0, bytesWritable.getLength());

                        ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw = ApstatsAnalyticsRaw.APStats.parseFrom(bs);

                        return clientStatsAnalyticsRaw;
                    }
                }
        ).filter(
                new Function<ApstatsAnalyticsRaw.APStats, Boolean>() {
                    // Remove the data without clients
                    public Boolean call(ApstatsAnalyticsRaw.APStats apStats) throws Exception {
                        return apStats.getClientsCount() > 0;
                    }
                }
        ).flatMap(
                new FlatMapFunction<ApstatsAnalyticsRaw.APStats, ClientStatsAnalytics.ClientStatsAnalyticsOut>() {

                    // For each client emit one record
                    public Iterator<ClientStatsAnalytics.ClientStatsAnalyticsOut> call(ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw) throws Exception {
                        return new ClientDataIterator(clientStatsAnalyticsRaw, "dummy");
                    }
                }
        );

        JavaRDD<Row> finalRdd = newRdd.groupBy(

                new Function<ClientStatsAnalytics.ClientStatsAnalyticsOut, String>() {
                    public String call(ClientStatsAnalytics.ClientStatsAnalyticsOut v1) throws Exception {
                        return v1.getSiteId() + ":" + v1.getApId() + ":" + v1.getClientWcid();
                    }
                }
        ).flatMap(

                // Sort the client stats based on Terminator timestamp and does the delta calculation
                // Return Row object
                new FlatMapFunction<Tuple2<String, Iterable<ClientStatsAnalytics.ClientStatsAnalyticsOut>>, Row>() {
                    public Iterator<Row> call(Tuple2<String, Iterable<ClientStatsAnalytics.ClientStatsAnalyticsOut>> stringIterableTuple2) throws Exception {
                        return new ClientRowIterator(stringIterableTuple2._2, ss);
                    }
                }
        );

        // convert to DF and save as parquet
        Dataset<Row> ds = spark.sqlContext().createDataFrame(finalRdd, ss);

        System.out.println("Number of lines in file = " + ds.count());
        System.out.println("First Record in file = " + ds.first());

        // TODO the ds needs to repartition to x number of partitions and save the final files in a path with dt=xxxx/hr=xx format
//        ds.write().parquet("s3://mist-test-bucket-production/test_run/");
    }



}