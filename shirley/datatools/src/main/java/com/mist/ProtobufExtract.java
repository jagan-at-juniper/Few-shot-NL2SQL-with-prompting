package com.mist;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.mist.mobius.common.MistUtils;
import com.mist.mobius.generated.ApstatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalytics;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import static org.apache.spark.sql.functions.callUDF;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.mist.ClientDataIterator.mac2wcid;

public class ProtobufExtract {
    private static final long serialVersionUID = 1L;
    private transient AmazonS3 s3client;
    // TODO read from cmdline
    private static final String FeedBasePath = "s3://mist-production-kafka-reseed/ap-stats-full-production";
    private static final String OutPutPath = "s3://mist-test-bucket-production/shirley_test_run";
    private static final String OrgDimPath = "s3://mist-secorapp-production/dimension/org/part-*.parquet";

    //spark-submit  --master yarn --jars data-tools-1.0-SNAPSHOT.jar --class com.mist.ProtobufExtract ./data-tools-1.0-SNAPSHOT.jar
    //

    private static StructType getOutputSchema(SparkSession spark) {

        Dataset<Row> clientDs = spark.sqlContext().read().parquet(
                "s3://mist-secorapp-production/client-stats-analytics/client-stats-analytics-production/dt=2020-03-15/hr=01/1_0_00000000013347518454.parquet");
        return clientDs.toDF().schema();
    }

    private static Map<String, String> getOrgKeys(SparkSession spark) {
        Dataset<Row> keyDs = spark.sqlContext().read().parquet("s3://mist-data-science-dev/shirley/org_key");
        List<Row> data = keyDs.collectAsList();
        Map<String, String> orgKeys = new HashMap<String, String>();
        for (Row r: data) {
            int inx = r.fieldIndex("key");
            int org_inx = r.fieldIndex("org_id");
            orgKeys.put(r.getString(org_inx), r.getString(inx));
        }
        return orgKeys;
    }

    private static UDF1 addDate = new UDF1<Long, String>() {
        public String call(final Long epoch) throws Exception {
            DateTime dt = new DateTime(epoch / 1000);
            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
            return fmt.print(dt);
        }
    };
    private static UDF1 addHour = new UDF1<Long, Integer>() {
        public Integer call(final Long epoch) throws Exception {
            DateTime dt = new DateTime(epoch / 1000);
            return dt.getHourOfDay();
        }
    };


    public static CommandLine parseArgs(String[] args) {

        // Option parsing
        Options options = new Options();
        Option runDate = new Option("d", "date", true, "Date to run in yyyymmdd format");
        Option env = new Option("env", "env", true, "staging or production");
        env.setRequired(true);
        runDate.setRequired(true);
        options.addOption(runDate);
        options.addOption(env);

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cmdLine = parser.parse(options, args);
            return cmdLine;
        } catch (ParseException exp) {
            System.out.println(exp.getMessage());
            System.exit(1);
        }
        return null;
    }

    public static void main(String[] args) throws IOException, java.text.ParseException {

        CommandLine cmdLine = parseArgs(args);
        if (cmdLine == null)
            System.exit(1);

        String dateToRun = cmdLine.getOptionValue("date");
        String env = cmdLine.getOptionValue("env");
        System.out.println(" Running date = " + dateToRun + " env = " + env);

        Date inputDate =new SimpleDateFormat("yyyyMMdd").parse(dateToRun);
        String outputDateStr = new SimpleDateFormat("yyyy-MM-dd").format(inputDate);

        final String dateForLambda = dateToRun;
        int hourToRun = Integer.valueOf("1");
        String outputHourStr = String.format("ht=%02d", hourToRun);


        String inputPath = FeedBasePath + "/" + dateToRun + "/" + hourToRun + "/0e0303dc-a46e-44c1-ba50-68227dd91b25/*";

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
        spark.udf().register("addDate", addDate, DataTypes.StringType);
        spark.udf().register("addHour", addHour, DataTypes.IntegerType);

        Dataset<Row> orgDimData = spark.read().parquet(OrgDimPath).select("id", "secret");
        List<Row> orgs = orgDimData.collectAsList();
//        List<String> paths = orgs.stream().map(r -> FeedBasePath + "/" + dateForLambda + "/*/" + r.getString(0) + "/*/*").collect(Collectors.toList());
//        List<List<String>> pathBatch = Lists.partition(paths, 100);

        final StructType ss = getOutputSchema(spark);
        final Map<String, String> orgKeys = getOrgKeys(spark);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaPairRDD<org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.BytesWritable> rawRDD =
                jsc.sequenceFile(
                        inputPath,
                        org.apache.hadoop.io.BytesWritable.class, org.apache.hadoop.io.BytesWritable.class);


        JavaRDD<ClientStatsAnalytics.ClientStatsAnalyticsOut> newRdd = rawRDD.map(
                new Function<Tuple2<BytesWritable, BytesWritable>, ApstatsAnalyticsRaw.APStats>() {
                    public ApstatsAnalyticsRaw.APStats call(Tuple2<BytesWritable, BytesWritable> v1) throws Exception {
                        // convert each record in sequence file to protobuf java class
                        BytesWritable bytesWritable = v1._2;
                        ByteString bs = ByteString.copyFrom(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                        try {
                            ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw = ApstatsAnalyticsRaw.APStats.parseFrom(bs);

                            return clientStatsAnalyticsRaw;
                        } catch (Exception e) {
                            return null;
                        }
                    }
                }
        ).filter(
                new Function<ApstatsAnalyticsRaw.APStats, Boolean>() {
                    // Remove the data without clients
                    public Boolean call(ApstatsAnalyticsRaw.APStats apStats) throws Exception {
                        return apStats != null && apStats.getClientsCount() > 0;
                    }
                }
        ).flatMap(
                new FlatMapFunction<ApstatsAnalyticsRaw.APStats, ClientStatsAnalytics.ClientStatsAnalyticsOut>() {


                    // For each client emit one record
                    public Iterator<ClientStatsAnalytics.ClientStatsAnalyticsOut> call(ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw) throws Exception {
                        String org_id =  MistUtils.getUUID(clientStatsAnalyticsRaw.getOrgId());
                        String key = orgKeys.get(org_id);
                        return new ClientDataIterator(clientStatsAnalyticsRaw, key);
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


        String outPath = OutPutPath + "/dt=" + outputDateStr + "/" + outputHourStr;
        System.out.println("Save data to " + outPath);
        // format output path
        ds.repartition(5).write().parquet(outPath);
        
//        Dataset<Row> data = ds.withColumn("dt", callUDF("addDate", ds.col("terminator_timestamp")))
//                .withColumn("hr", callUDF("addHour", ds.col("terminator_timestamp")));
//        System.out.println("Number of lines in file = " + data.count());
//        System.out.println("First Record in file = " + data.first());
//
//
//        // TODO the ds needs to repartition to x number of partitions and save the final files in a path with dt=xxxx/hr=xx format
//        data.repartition(data.col("dt"), data.col("hr")).write().partitionBy("dt", "hr").parquet("s3://mist-test-bucket/shirley_test_run/");
//
    }


}