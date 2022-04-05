import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime,timedelta
import os

spark = SparkSession \
    .builder \
    .appName("ap-capacity-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

myhost = os.uname()[1]
env = "production"
provider = "AWS" if "dataproc" not in myhost else "GCP"
fs = "s3" if provider == "AWS" else "gs"


date_now = datetime.now() - timedelta(hours=3)
write_date_day = date_now.strftime("%Y-%m-%d")
write_date_hour = date_now.strftime("%H")
print(write_date_day, write_date_hour)


def ap_id_reformat(ap):
    return ap.replace("-", "")
ap_id_reformat_f = F.udf(ap_id_reformat, StringType())


def hour_diff(t0, t1):
    if not t1 or t1<1:
        return -1.0
    else:
        return (t0/1_000-t1/1_000)/3600
    return -1
get_hour_diff = F.udf(hour_diff, FloatType())


def get_ap_coverage_candidates(last_days=3, dt=""):
    s3_bucket= '{fs}://mist-aggregated-stats-production/aggregated-stats/ap_coverage_candidates/'.format(fs=fs)
    files = []
    if not dt:
        date_now = datetime.now()  - timedelta(hours=3)
        for i in range(last_days):
            date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")

            #         s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt={}/hr=*/*.seq'.format(date_day)
            s3_bucket_files = s3_bucket + 'dt={}/hr=*/*.csv'.format(date_day)
            files.append(s3_bucket_files)
        files = ",".join(files)
    else:
        s3_bucket_files = s3_bucket + 'dt={dt}/hr=*/*.csv'.format(dt=dt)
        files.append(s3_bucket_files)
    print(files)

    df_events = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(files)

    df_events = df_events\
        .withColumn("date_hour", F.from_unixtime(F.col('timestamp')/1000, format='yyyy-MM-dd HH'))

    window = Window.partitionBy(F.col('ap_id'), F.col('band')).orderBy(F.col('date_hour').asc())
    df_events = df_events.withColumn("prev_timestamp",  F.lag(F.col("timestamp"), 1, 0).over(window)) \
        .withColumn("hours_since_last", get_hour_diff(F.col("timestamp"), F.col("prev_timestamp"))) \
        .withColumn("prev_date",  F.lag(F.col("date_hour"), 1, 0).over(window))

    #     df_coverage = rdd.map(lambda x: json.loads(x[1])). \
    #                     filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
    #                     .map(lambda x: x.get("source")) \
    #                     .toDF()

    return df_events


def check_coverage_anomaly_from_ap_events(last_days=3):
    files = []
    date_now = datetime.now() - timedelta(hours=2)
    for i in range(last_days):
        date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")
        s3_bucket= '{fs}://mist-secorapp-production/ap-events/ap-events-production/dt={date_day}/hr=*/*.seq'.format(fs=fs, date_day=date_day)
        files.append(s3_bucket)
    files = ",".join(files)
    rdd = spark.sparkContext.sequenceFile(files)
    df_coverage = rdd.map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    return df_coverage



# def check_impact_aps(impacted_aps):
#
#     df_coverage = check_coverage_anomaly_from_ap_events(7)
#     df_coverage.printSchema()
#
#
#     df_coverage = df_coverage.withColumn("ap", ap_id_reformat_f(F.col("ap")))
#
#     coverage_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
#                      "interference_type", "sle_coverage", "util_ap", "util_all_mean"]
#
#     df_coverage.select(coverage_cols).show()
#
#     df_coverage_aps = df_coverage.filter(df_coverage.ap.isin(impacted_aps))
#     df_coverage_aps.count()
#
#     # df_coverage.show(2)
#     df_coverage_aps.select(coverage_cols).show()
#     return df_coverage_aps


# def check_coverage_anomaly_stats_impact_aps(impacted_aps):
#     df_coverage_anomaly_stats = get_coverage_anomaly_agg_stats(7)
#     df_coverage_anomaly_stats.printSchema()
#
#     # coverage_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
#     #                  "interference_type", "sle_coverage", "util_ap", "util_all_mean"]
#
#     # df_coverage_anomaly_stats.select(coverage_cols).show()
#
#     df_coverage_anomaly_stats_aps = df_coverage_anomaly_stats.filter(df_coverage.ap.isin(impacted_aps))
#     df_coverage_anomaly_stats_aps.count()
#
#     df_coverage_anomaly_stats_aps.show(2)
#     # df_coverage_aps.select(coverage_cols).show()
#     return df_coverage_anomaly_stats_aps


def get_coverage_anomaly_agg_stats(last_days=3, dt=""):
    s3_bucket_files = []
    if not dt:
        date_now = datetime.now() - timedelta(hours=2)
        for i in range(last_days):
            date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")
            s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats_parquet/' \
                       f'dt={date_day}/hr=*/last_1_day/*.parquet'.format(fs=fs, date_day=date_day)
            s3_bucket_files.append(s3_bucket)
        print(s3_bucket_files)
        # s3_bucket_files = ",".join(s3_bucket_files)
        df_coverage_anomaly_stats = spark.read.parquet(*s3_bucket_files)
    else:
        s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats_parquet/' \
                   f'dt={dt}/hr=*/last_1_day/*.parquet'.format(fs=fs, date_day=dt)
        df_coverage_anomaly_stats = spark.read.parquet(s3_bucket)
    df_coverage_anomaly_stats.printSchema()
    return df_coverage_anomaly_stats


def get_coverage_anomaly_enriched_stats(last_days=3, dt=""):
    s3_bucket_files = []
    if not dt:
        date_now = datetime.now() - timedelta(hours=2)
        for i in range(last_days):
            date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")
            s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/' \
                       f'dt={date_day}/hr=*/*.parquet'.format(fs=fs, date_day=date_day)
            s3_bucket_files.append(s3_bucket)
        print(s3_bucket_files)
        # s3_bucket_files = ",".join(s3_bucket_files)

        df_coverage_enriched = spark.read.parquet(*s3_bucket_files)
    else:
        s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/' \
                   f'dt={dt}/hr=*/*.parquet'.format(fs=fs, date_day=dt)
        df_coverage_enriched = spark.read.parquet(s3_bucket)
    df_coverage_enriched.printSchema()
    return df_coverage_enriched


def get_coverage_anomaly_enriched_stats_test(last_days=3, dt=''):
    s3_bucket_files = []
    if not dt:
        date_now = datetime.now() - timedelta(hours=2)
        for i in range(last_days):
            date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")
            s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/' \
                       f'dt={date_day}/hr=*/*.parquet'.format(fs=fs, date_day=date_day)
            s3_bucket_files.append(s3_bucket)
        print(s3_bucket_files)
        # s3_bucket_files = ",".join(s3_bucket_files)

        df_coverage_enriched = spark.read.parquet(*s3_bucket_files)
    else:
        s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/' \
                   f'dt={dt}/hr=*/*.parquet'.format(fs=fs, date_day=dt)
        df_coverage_enriched = spark.read.parquet(s3_bucket)
    df_coverage_enriched.printSchema()
    return df_coverage_enriched

# if __name__ == "__main__":
if __name__ == "__main__":
    """
    """
    data_date_day = "2022-03-1*"
    write_date_day = "2022-03"  # date_now.strftime("%Y-%m-%d")
    write_date_hour = "00"   # date_now.strftime("%H")
    print(write_date_day, write_date_hour)

    last_days = 7
    df_events = get_ap_coverage_candidates(last_days , data_date_day)
    df_events.printSchema()
    # df_events = get_ap_coverage_candidates(1)
    # df_events.printSchema()
    df_events.show(2, truncate=False)
    df_events.count()

    df_events.select("ap_id", "band").groupBy("ap_id", "band").count().orderBy(F.col("count").desc()).show()

    df_events.select("band").groupBy("band").count().show()
    # df_events.filter(candidate_emit_filter).select("band").groupBy("band").count().show()

    ##
    impact_aps = list(df_events.select('ap_id').toPandas()['ap_id'])
    print(impact_aps)
    impact_aps = set( x.replace("-", "") for x in impact_aps )



    #     return df_coverage
    df_coverage = check_coverage_anomaly_from_ap_events(last_days)
    df_coverage.printSchema()
    # df_coverage.count()

    coverage_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
                     "interference_type", "sle_coverage", "util_ap", "util_all_mean",
                     "sle_coverage_base", "util_all_mean_base"]

    df_coverage = df_coverage.withColumn("ap", ap_id_reformat_f(F.col("ap")))
    df_coverage_aps = df_coverage.filter(df_coverage.ap.isin(impact_aps[:]))
    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_aps.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    N_rows = df_coverage_aps.count()
    print(N_rows)
    N_rows = 100
    # df_coverage_aps.saving
    df_coverage_aps.select(coverage_cols).orderBy("ap").show(N_rows, truncate=False)

    # df_test = spark.read.parquet(s3_path)
    # df_test.count()
    # df_test.select(coverage_cols).orderBy("ap").show(200)

    #     return df_coverage_agg
    df_coverage_anomaly_stats = get_coverage_anomaly_agg_stats(last_days, data_date_day)
    df_coverage_anomaly_stats.printSchema()
    df_coverage_anomaly_stats_aps = df_coverage_anomaly_stats.filter(df_coverage_anomaly_stats.ap_id.isin(impact_aps[:]))
    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-agg/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_anomaly_stats_aps.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    N_rows = df_coverage_anomaly_stats_aps.count()
    print(N_rows)
    df_coverage_anomaly_stats_aps.orderBy("ap_id").show(N_rows, truncate=False)

    cols = ['avg_nclients', 'util_ap',
             'rssi_mean', 'rssi_mean_base',
            'sle_coverage', 'sle_coverage_base']
    df_coverage_anomaly_stats.select(cols).show()


    # df_test = spark.read.parquet(s3_path)
    # df_test.count()
    #
    # df_test.select(coverage_cols).orderBy("ap").show(200)

    #     return df_coverage_agg
    df_coverage_enriched_test= get_coverage_anomaly_enriched_stats_test(last_days, data_date_day)
    df_coverage_enriched_test.printSchema()
    df_coverage_enriched_test_aps = df_coverage_enriched_test.filter(df_coverage_enriched_test.ap_id.isin(impact_aps[:]))
    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-enriched-test/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_enriched_test_aps.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    N_rows = df_coverage_enriched_test_aps.count()
    print(N_rows)
    df_coverage_enriched_test_aps.orderBy("ap_id").show(N_rows, truncate=False)
    cols = ['avg_nclients', 'util_ap', 'util_all', 'util_all_base', 'rssi_mean', 'rssi_mean_base',
            'sle_coverage', 'sle_coverage_base',
            'sle_capacity', 'sle_capacity_base',
            'capacity_util_all', 'capacity_util_all_base']
    df_coverage_enriched_test.filter('avg_nclients>0').select(cols).show()

    #
    df_coverage_enriched = get_coverage_anomaly_enriched_stats(last_days, data_date_day)
    df_coverage_enriched.printSchema()
    df_coverage_enriched_aps = df_coverage_enriched.filter(df_coverage_enriched.ap1.isin(impact_aps[:]))
    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-enriched/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_enriched_aps.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    N_rows = df_coverage_enriched_aps.count()
    print(N_rows)
    df_coverage_aps.saving
    df_coverage_enriched_aps.orderBy("ap1").show(N_rows, truncate=False)
    cols = ['ap1_avg_nclients', 'ap1_util_ap',
            'ap1_util_all', 'ap1_util_all_base', 'ap1_rssi_mean', 'ap1_rssi_mean_base',
            'ap1_sle_coverage', 'ap1_sle_coverage_base',
            'ap1_sle_capacity', 'ap1_sle_capacity_base',
            'ap1_capacity_util_all', 'ap1_capacity_util_all_base']
    df_coverage_enriched.filter('ap1_avg_nclients>0').select(cols).show()


def get_data():
    """

    :return:
    """

    data_date_day = "2022-03-*"
    write_date_day = "2022-03"  # date_now.strftime("%Y-%m-%d")
    write_date_hour = "00"   # date_now.strftime("%H")
    print(write_date_day, write_date_hour)

    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_aps =  spark.read.parquet(s3_path)
    df_coverage_aps.printSchema()
    df_coverage_aps.count()


    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-agg/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_anomaly_stats_aps = spark.read.parquet(s3_path)
    df_coverage_anomaly_stats_aps.printSchema()
    df_coverage_anomaly_stats_aps.count()

    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-enriched/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_enriched_test_aps = spark.read.parquet(s3_path)
    df_coverage_enriched_test_aps.printSchema()
    df_coverage_enriched_test_aps.count()

    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-coverage-aps-enriched/dt={dt}/hr={hr}".format(fs=fs, dt=write_date_day, hr=write_date_hour)
    print(s3_path)
    df_coverage_enriched_aps = spark.read.parquet(s3_path)
    df_coverage_enriched_aps.printSchema()
    df_coverage_enriched_aps.count()
