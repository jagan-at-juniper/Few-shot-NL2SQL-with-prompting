import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime,timedelta


env = "production"
provider = "AWS"

spark = SparkSession \
    .builder \
    .appName("ap-capacity-events") \
    .getOrCreate()

fs = "s3" if provider=="AWS" else "gs"

date_now = datetime.now() - timedelta(hours=3)
date_day = date_now.strftime("%Y-%m-%d")
date_hour = date_now.strftime("%H")
print(date_day, date_hour)


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


def get_ap_capacity_candidates(last_days=3, dt=""):
    s3_bucket= '{fs}://mist-aggregated-stats-production/aggregated-stats/ap_capacity_candidates/'.format(fs=fs)
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

    #     df_capacity = rdd.map(lambda x: json.loads(x[1])). \
    #                     filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
    #                     .map(lambda x: x.get("source")) \
    #                     .toDF()

    return df_events


def check_capacity_anomaly_from_ap_events(last_days=3):
    files = []
    date_now = datetime.now()  - timedelta(hours=2)
    for i in range(7):
        date_day= (date_now  - timedelta(days=i)).strftime("%Y-%m-%d")

        s3_bucket= '{fs}://mist-secorapp-production/ap-events/ap-events-production/dt={date_day}/hr=*/*.seq'.format(fs=fs, date_day=date_day)
        files.append(s3_bucket)
    # files = "dir1, dir2, dir3,"
    files = ",".join(files)
    rdd = spark.sparkContext.sequenceFile(files)
    df_capacity = rdd.map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    return df_capacity



def check_impact_aps(impacted_aps):
    #     impacted_aps = ['ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'd4dc09e4d38c', 'd4dc09e4d38c',
    #                     'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'ac23160dbbeb',
    #                     'ac23160dbbeb', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
    #                     'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
    #                     'd4dc092b8263', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc092b8263']


    df_capacity = check_capacity_anomaly_from_ap_events(7)
    df_capacity.printSchema()


    df_capacity = df_capacity.withColumn("ap", ap_id_reformat_f(F.col("ap")))

    capacity_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
                     "interference_type", "sle_capacity", "util_ap", "util_all_mean"]

    df_capacity.select(capacity_cols).show()

    df_capacity_aps = df_capacity.filter(df_capacity.ap.isin(impacted_aps))
    df_capacity_aps.count()

    # df_capacity.show(2)
    df_capacity_aps.select(capacity_cols).show()
    return df_capacity_aps





df_events = get_ap_capacity_candidates(1, "2022-02-2*")
df_events.printSchema()
# df_events = get_ap_capacity_candidates(1)
# df_events.printSchema()

df_events.show(2, truncate=False)
df_events.count()

df_events.select("ap_id", "band").groupBy("ap_id", "band").count().orderBy(F.col("count").desc()).show()

df_events.select("band").groupBy("band").count().show()
# df_events.filter(candidate_emit_filter).select("band").groupBy("band").count().show()



impact_aps = list(df_events.select('ap_id').toPandas()['ap_id'])
print(impact_aps)
impact_aps = [ x.replace("-", "") for x in impact_aps ]



#     return df_capacity
df_capacity = check_capacity_anomaly_from_ap_events(7)
df_capacity.printSchema()
# df_capacity.count()

capacity_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
                 "interference_type", "sle_capacity", "util_ap", "util_all_mean",
                 "sle_capacity_base", "util_all_mean_base"]

df_capacity = df_capacity.withColumn("ap", ap_id_reformat_f(F.col("ap")))
# df_capacity.select(capacity_cols).show()




df_capacity_aps = df_capacity.filter(df_capacity.ap.isin(impact_aps[:]))

s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-capacity-aps/dt={dt}/hr={hr}".format(fs=fs, dt=date_day, hr=date_hour)
print(s3_path)
df_capacity_aps.write.save(s3_path, format='parquet', mode='overwrite', header=True)

df_capacity_aps.count()



# df_capacity_aps.saving
df_capacity_aps.select(capacity_cols).orderBy("ap").show(200)


df_test = spark.read.parquet(s3_path)
df_test.count()

df_test.select(capacity_cols).orderBy("ap").show(200)
