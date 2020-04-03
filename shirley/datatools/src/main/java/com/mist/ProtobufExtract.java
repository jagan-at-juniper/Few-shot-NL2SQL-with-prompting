package com.mist;


import com.google.protobuf.ByteString;
import com.mist.mobius.common.MistUtils;
import com.mist.mobius.generated.ApstatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalytics;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
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
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProtobufExtract implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(ProtobufExtract.class);

    // TODO read from cmdline
    public static final String FeedBasePath = "s3://mist-production-kafka-reseed/ap-stats-full-production";
    public static final String OutPutPath = "s3://mist-test-bucket-production/shirley_test_run";
    public static final String OrgDimPath = "s3://mist-secorapp-production/dimension/org/part-*.parquet";
    public static final String SourcePath = "s3://mist-test-bucket-production/backfill-ipaths/date=";

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

    private static Map<Integer, List<String>> getInputPaths(SparkSession spark, String inputDate) {

        Map<Integer, List<String>> pathKeys = new HashMap<>();
        Dataset<Row> keyDs = spark.sqlContext().read().parquet("s3://mist-test-bucket-production/backfill-ipaths/date=" + inputDate);
        List<Row> data = keyDs.collectAsList();

        for (Row r: data) {
            int inx = r.fieldIndex("key");
            String p = r.getString(inx);
            String[] parts = p.split("/");

            Integer hour = Integer.valueOf(parts[2]);
            if (pathKeys.containsKey(hour))
                pathKeys.get(hour).add(p);
            else {
                List<String> paths = new ArrayList<>();
                paths.add(p);
                pathKeys.put(hour, paths);
            }
        }
        return pathKeys;
    }

    public static CommandLine parseArgs(String[] args) {

        // Option parsing
        Options options = new Options();
        Option runDate = new Option("d", "date", true, "Date to run in yyyymmdd format");
        Option env = new Option("env", "env", true, "staging or production");
        Option batchSize = new Option("batch", "batch", true, "staging or production");

        env.setRequired(true);
        runDate.setRequired(true);
        batchSize.setRequired(true);
        options.addOption(runDate);
        options.addOption(env);
        options.addOption(batchSize);

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

    public static void processData(SparkSession spark, JavaSparkContext jsc,
                                   String inputPath,
                                   final Map<String, String>orgKeys, final StructType ss,
                                   String outputDateStr, String outputHourStr, int batchId) {
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

        String outPath = OutPutPath + "/dt=" + outputDateStr + "/" + outputHourStr + "/" + "batch=" + batchId;
        System.out.println("Save data to " + outPath);
        // format output path
        ds.repartition(3).write().parquet(outPath);
    }

    public static String composeInputPathString(List<String>inputPath, int startInx, int endInx) {

        StringBuffer buffer = new StringBuffer();
        for (int i = startInx; i < endInx && i < inputPath.size(); i++) {
            buffer.append("s3://mist-production-kafka-reseed/" + inputPath.get(i) + ",");
        }
        buffer.deleteCharAt(buffer.length() - 1);
        return buffer.toString();
    }

    public static void main(String[] args) throws IOException, java.text.ParseException {

        CommandLine cmdLine = parseArgs(args);
        if (cmdLine == null)
            System.exit(1);

        String dateToRun = cmdLine.getOptionValue("date");
        String env = cmdLine.getOptionValue("env");
        int batchSize = Integer.valueOf(cmdLine.getOptionValue("batch"));

        logger.info(" Running date = " + dateToRun + " env = " + env + " batch_size = " + batchSize);

        Date inputDate =new SimpleDateFormat("yyyyMMdd").parse(dateToRun);
        String outputDateStr = new SimpleDateFormat("yyyy-MM-dd").format(inputDate);

        final String dateForLambda = dateToRun;

        SparkConf sparkConf = new SparkConf()
                .set("spark.serializer", KryoSerializer.class.getName());

        List<Class<?>> clses = Arrays.<Class<?>>asList(
                ApstatsAnalyticsRaw.APStats.class,
                ApstatsAnalyticsRaw.APStats.class,
                ClientDataIterator.class);
        sparkConf.registerKryoClasses((Class<?>[]) clses.toArray());

        final SparkSession spark = SparkSession
                .builder()
                .appName("Back Fill date=" + inputDate)
                .config(sparkConf)
                .getOrCreate();

        final StructType ss = getOutputSchema(spark);
        final Map<String, String> orgKeys = getOrgKeys(spark);
        final Map<Integer, List<String>> inputPathsHourMap = getInputPaths(spark, dateToRun);
        final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

//        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);

        for(int hh=0; hh < 24; hh++) {


            List<String> inputPaths = inputPathsHourMap.get(hh);
            inputPaths.sort( Comparator.comparing( String::toString ) );
            String outputHourStr = String.format("ht=%02d", hh);
            System.out.println("Starting process hour " + hh + " with file count " + inputPaths.size());

            for (int i = 0; i < inputPaths.size(); i += batchSize) {
                String pathStr = composeInputPathString(inputPaths, i, i + batchSize);
                processData(spark, jsc, pathStr, orgKeys, ss, outputDateStr, outputHourStr, i);
                logger.info("Successfully submit hour " + hh + " batch " + i );
            }
        }

    }

}