
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta
from pyspark.sql.types import *

app_name = "zero-clients"
spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()

spark.sparkContext.setLogLevel("warn")


# @F.udf(IntegerType)
def bcn_per_wlan(bcn, num_wlans, model=""):
    bcn_norm = bcn if bcn != None else -1.0
    if model.find("AP41")>-1:
        bcn_norm= bcn_norm if bcn_norm <610 else 700
    else:
        bcn_norm = bcn_norm/num_wlans if num_wlans>0 else 0
    bcn_norm = bcn_norm if bcn_norm < 610 else 700.0
    return float(bcn_norm)

bcn_per_wlan_norm = F.udf(bcn_per_wlan, FloatType())


def save_df_to_fs(df, date_day, date_hour, app_name="aps-no-client-all"):
    date_hour = "000" if date_hour=="*" else date_hour
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name= app_name, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path,format='parquet',   mode='overwrite', header=True)


env = "production"
provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

detect_time = datetime.now() - timedelta(hours=2)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

# date_day = "2022-04-22"
# date_hour = "1*"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)

df= spark.read.parquet(s3_bucket)
df.printSchema()

# Radio
df_radio = df.filter("uptime>3600") \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "uptime",
            F.col("when").alias("timestamp"),
            F.explode("radios").alias("radio")
            ) \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "uptime",  "timestamp",  "radio.*")\
    .withColumn("num_wlans", F.size("wlans")) \
    .filter("dev != 'r2' and bandwidth>0 and not radio_missing and band==5 and num_wlans>0") \
    .withColumn("bcn_per_wlan1", bcn_per_wlan_norm(F.col("interrupt_stats_tx_bcn_succ"), F.col("num_wlans"), F.col("model"))) \
    .withColumn("bcn_per_wlan", F.when(F.col("model").rlike("AP41"), F.col("interrupt_stats_tx_bcn_succ")) \
                .otherwise(F.col("interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))) \
    .withColumn("bcn_per_wlan_drop", F.col("bcn_per_wlan")<500.0)\
    .withColumn("noise_floor_high", F.col("noise_floor")>-60.0) \
    .withColumn("tx_phy_err_high", F.col("tx_phy_err")>1.0) \
    .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err")>1.0) \
    .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err")>1.0) \
    .withColumn("utilization_non_wifi_high", F.col("utilization_non_wifi")>0.30)


def get_bcn_df(df_radio):
    df_radio_with_bcn_drop = df_radio.filter("bcn_per_wlan_drop") \
        .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band").agg(
        F.min("timestamp").alias("start_timestamp"),
        F.max("timestamp").alias("end_timestamp")
    ).withColumn("bcn_drop_time", (F.col("end_timestamp") - F.col("start_timestamp")/1_000_000))
    return df_radio_with_bcn_drop

df_radio_with_bcn_drop = get_bcn_df(df_radio)
save_df_to_fs(df_radio_with_bcn_drop, date_day, date_hour, "aps-bcn-drop")

df_radio.filter("bcn_per_wlan>=700").show()

df_radio.filter("bcn_per_wlan<0").count()

#
# df_g = df_radio.select("id", "has_client", "tx_phy_err_high", "mac_stats_tx_phy_err_high")\
#     .groupBy("has_client", "tx_phy_err_high", "mac_stats_tx_phy_err_high")\
#     .agg(F.count("id").alias("count"),
#          F.countDistinct("id").alias("aps"))
# df_g.orderBy("has_client", "tx_phy_err_high", "mac_stats_tx_phy_err_high").show()


# df_radio.filter("bcn_per_wlan < 500 and radio.tx_phy_err>0")
# df_radio.printSchema()

df_radio_nf_g = df_radio\
    .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
    .agg(
    F.min("uptime").alias("min_uptime"),
    F.max("re_init").alias("re_init"),
    F.count("id").alias("counts"),
    F.collect_set("channel").alias("channels"),
    F.max("num_clients").alias("max_num_clients"),
    F.max(F.col('rx_errors')/F.col("rx_pkts")).alias("rx_errors_max"),
    F.max(F.col('tx_errors')/F.col("tx_pkts")).alias("tx_errors_max"),
    F.max('tx_phy_err').alias("tx_phy_err_max"),
    F.min('tx_phy_err').alias("tx_phy_err_min"),
    F.avg('tx_phy_err').alias("tx_phy_err_avg"),
    F.max('mac_stats_tx_phy_err').alias("mac_stats_tx_phy_err_max"),
    F.min('mac_stats_tx_phy_err').alias("mac_stats_tx_phy_err_min"),
    F.avg('mac_stats_tx_phy_err').alias("mac_stats_tx_phy_err_avg"),
    F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
    F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
    F.max("bcn_per_wlan").alias("bcn_per_wlan_max"),
    F.stddev("bcn_per_wlan").alias("bcn_per_wlan_std"),
    F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
    F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
    F.max("num_wlans").alias("num_wlans"),
    F.max("noise_floor").alias("noise_floor_max"),
    F.avg("noise_floor").alias("noise_floor_avg"),
    F.min("noise_floor").alias("noise_floor_min"),
    F.max("utilization_non_wifi").alias("utilization_non_wifi_max"),
    F.avg("utilization_non_wifi").alias("utilization_non_wifi_avg"),
    F.min("utilization_non_wifi").alias("utilization_non_wifi_min"),
    F.sum(F.when(F.col("bcn_per_wlan_drop")==True, 1).otherwise(0)).alias("bcn_per_wlan_drop_count"),
    F.sum(F.when(F.col("noise_floor_high")==True, 1).otherwise(0)).alias("noise_floor_high_count"),
    F.sum(F.when(F.col("tx_phy_err_high")==True, 1).otherwise(0)).alias("tx_phy_err_high_count"),
    F.sum(F.when(F.col("mac_stats_tx_phy_err_high")==True, 1).otherwise(0)).alias("mac_stats_tx_phy_err_high_count"),
    F.sum(F.when(F.col("utilization_non_wifi_high")==True, 1).otherwise(0)).alias("utilization_non_wifi_high_count"),
    F.min(F.when(F.col("bcn_per_wlan_drop")==True, F.col("timestamp")).otherwise(0)).alias("start_timestamp"),
    F.max(F.when(F.col("bcn_per_wlan_drop")==True, F.col("timestamp")).otherwise(0)).alias("end_timestamp")
).withColumn("bcn_drop_time", F.col("end_timestamp") - F.col("start_timestamp"))


#


# df_radio_with_bcn_drop = df_radio_nf_g.filter("bcn_drop_time>0") #.select("start_timestamp", "end_timestamp", "bcn_drop_time").show()
# w = Window.partitionBy("userid").orderBy("eventtime")
# DF = DF.withColumn("indicator", (DF.timeDiff > 300).cast("int"))
# DF = DF.withColumn("subgroup", func.sum("indicator").over(w) - func.col("indicator"))
# DF = DF.groupBy("subgroup").agg(
#     func.min("eventtime").alias("start_time"),
#     func.max("eventtime").alias("end_time"),
#     func.count("*").alias("events")
# )
# df_radio_bcn = df_radio_nf_g.filter("bcn_per_wlan_drop_count > 0").
#     .withColumn("bcn_drop_start", F.when(F.col("bcn_per_wlan")<500, F.col("timestamp")).otherwise(F.col("end_timestamp"))) \
#     .withcolumn("bcn_drop_end", F.when(F.col("bcn_per_wlan")>500, F.col("timestamp")).otherwise(F.col("end_timestamp")))

df_radio_nf_g = df_radio_nf_g.withColumn("has_client", F.col("max_num_clients") > 0) \
    .withColumn("bcn_drop", F.col("bcn_per_wlan_drop_count")/F.col("counts") > 0.5) \
    .withColumn("reinited", F.col("re_init") > 0) \
    .withColumn("channel_switched", F.size("channels") > 1)\
    .drop("channels")\
    .withColumn("bcn_per_wlan_drop", F.col("bcn_per_wlan_drop_count") > 600) \
    .withColumn("noise_floor_high", F.col("noise_floor_high_count") > 600) \
    .withColumn("tx_phy_err_high", F.col("tx_phy_err_high_count") > 600) \
    .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err_high_count") > 600) \
    .withColumn("utilization_non_wifi_high", F.col("utilization_non_wifi_high_count") > 600)

df_radio_nf_g.select("counts", "bcn_per_wlan_drop_count", "noise_floor_high_count",
                     "tx_phy_err_high_count", "mac_stats_tx_phy_err_high_count",
                     "utilization_non_wifi_high_count").show()

""" only APs without clients"""
df_radio_nf_g0 = df_radio_nf_g.filter("max_num_clients<1 ")

df_radio_nf_g.select("has_client", "bcn_drop").groupBy("has_client", "bcn_drop").count().show()




save_df_to_fs(df_radio_nf_g0, date_day, date_hour, "zero-clients")

s3_path_1 = "{fs}://mist-data-science-dev/wenfeng/zero-clients/dt={date_day}/hr={date_hour}".format(fs=fs, date_day=date_day, date_hour=date_hour)
print(s3_path_1)
df_radio_nf_g0 = spark.read.parquet(s3_path_1)
problematic_aps = df_radio_nf_g0.select("id").collect()
print(len(problematic_aps))



df_radio_nf_g0.select("model").groupBy("model").count().show()

# df_radio_nf_g0= df_radio_nf_g0.withColumn("bcn_per_wlan_drop", F.col("bcn_per_wlan_drop_count")>600) \
#     .withColumn("noise_floor_high", F.col("noise_floor_high_count")>600) \
#     .withColumn("tx_phy_err_high", F.col("tx_phy_err_high_count")>600) \
#     .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err_high_count")>600) \
#     .withColumn("utilization_non_wifi_high", F.col("utilization_non_wifi_high_count")>600)

df_radio_nf_g0.select("bcn_per_wlan_drop", "noise_floor_high", "tx_phy_err_high", "mac_stats_tx_phy_err_high", "utilization_non_wifi_high") \
    .groupBy("bcn_per_wlan_drop", "noise_floor_high", "tx_phy_err_high", "mac_stats_tx_phy_err_high", "utilization_non_wifi_high").count().show()



cols = ["count", "min", "50%", "75%", "90%", "95%", "99%", "max"]
df_radio_nf_g0.summary(cols).show()



df_t = df_radio_nf_g.filter("bcn_per_wlan < 500 and (tx_phy_err_max>0 or mac_stats_tx_phy_err_max>0)")
df_t.select("max_num_clients", "bcn_per_wlan", "tx_phy_err_max",  "mac_stats_tx_phy_err_max", "interrupt_stats_tx_bcn_succ_max").summary().show()


# s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-radios/dt={dt}/hr={hr}".format(fs=fs, dt=date_day, hr=date_hour)
# df_radio_nf_g.write.save(s3_path, format='parquet', mode='overwrite', header=True)

# df_radio_nf_g.filter("bcn_per_wlan < 500 and max_tx_phy_err>0")
df_t = df_radio_nf_g.filter("bcn_per_wlan < 500 and (tx_phy_err_max>0 or mac_stats_tx_phy_err_max > 0)")
df_t.select("max_num_clients", "bcn_per_wlan", "tx_phy_err_max", "mac_stats_tx_phy_err_max",
            "interrupt_stats_tx_bcn_succ_max").summary().show()

# s3_path = "{fs}://mist-data-science-dev/wenfeng/test/dt={dt}/hr={hr}" \
#     .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)

# df_radio_nf_g.coalesce(1).write.save(s3_path,
#                                                format='csv',
#                                                mode='overwrite',

Filter_query_1 = "band==5 and max_num_clients <1 and (tx_phy_err_max > 0 or mac_stats_tx_phy_err_max>0)" \
                 " and num_wlans>0 and bcn_per_wlan < 500"
df_radio_nf_problematic = df_radio_nf_g.filter(Filter_query_1)


# def save_df_to_fs(df_radio_nf_problematic, date_day, date_hour):
#     date_hour = "000" if date_hour == "*" else date_hour
#     s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-all/dt={dt}/hr={hr}" \
#         .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
#
#     df_radio_nf_problematic.coalesce(1).write.save(s3_path,
#                                                    format='csv',
#                                                    mode='overwrite',
#                                                    header=True)

save_df_to_fs(df_radio_nf_problematic, date_day, date_hour, app_name+"_problematic")


df_radio_nf_problematic.count()

df_radio_nf_problematic.show()

def check_org_and_model():
    """

    :return:
    """
    df_radio_nf_problematic_g1 = df_radio_nf_problematic.groupBy("org_id").agg(
        F.countDistinct("id").alias("aps"))
    df_radio_nf_problematic_g1.show(truncate=False)

    df_radio_nf_problematic_g2 = df_radio_nf_problematic.groupBy("org_id", "site_id").agg(
        F.countDistinct("id").alias("aps")).orderBy(F.col("aps").desc())
    df_radio_nf_problematic_g2.show(truncate=False)

    df_radio_nf_problematic.groupby("firmware_version" , "model") \
        .count().orderBy(F.col("count").desc()) \
        .show(truncate=False)

check_org_and_model()
# df_radio_nf_problematic.persist()

#
#
# df_new = spark.read.option("header",True).csv(s3_path + "/*.csv")
# df_new.show()
#
# df_new = df_new.alias('df_new')
# df_old = df_old.alias('df_old')
# df_join= df_new.join(df_old, df_new.id == df_old.id)
# df_join.select("df_new.*").show()
#
# from pyspark.sql.types import StringType
# def get_ap_names(hostname):
#     return "".join([hostname[-3], "00"])
# ap_names_family = F.udf(get_ap_names, StringType())
#
# df_radio_nf_problematic_2 = df_radio_nf_problematic\
#     .filter(F.col("hostname").like("APUS-%-%"))\
#     .withColumn("ap_series", ap_names_family(F.col("hostname")))
#
# df_radio_nf_problematic_2.select("ap_series").groupBy("ap_series").count().show()

#
# def test():
#     from pyspark.sql import functions as F
#     import time
#     # sc.addPyFile("gs://mist-data-science-dev/wenfeng/spark_jobs_test.zip")
#     from analytics.jobs.utils import *
#     from analytics.event_generator.beacon_partial_stuck_event import *
#
#     current_epoch_seconds = int(time.time()/3600)*3600
#     start_time = current_epoch_seconds - 7200
#     end_time = start_time + 3600
#     # start_time, end_time = 1642856400, 1642860000
#     #job = start_debug_job( 'ap-stats-analytics', 1596099000, 1596099600, test_env='staging') #7.30 2am PST
#     job = start_debug_job( 'ap-stats-analytics', start_time, end_time, test_env='production') #7.31 1pm PST
#     data_rdd = run_category_transform(job, 'enabled_radio').persist()
#     data_rdd.count()
#
#     gen = get_event_generator(job, 'enabled_radio', 'BeaconPartialStuckEvent')
#     event_rdd = gen.generate_event(data_rdd, spark)
#     event_rdd.count()
#
#     #
#     # gen = get_event_generator(job, 'enabled_radio', 'BeaconPartialStuckEvent')
#     # data_rdd = data_rdd \
#     #     .filter(lambda radio_stat_tuple: not any([x['radio']['radio_missing'] for x in radio_stat_tuple[1]])) \
#     #     .mapValues(lambda stats_list: [rec for rec in stats_list if filter_inactive_radio(rec)]) \
#     #     .filter(lambda key_tuple: len(key_tuple[0]) > 0) \
#     #     .filter(lambda key_tuple: len(set([x['radio'].get('channel') for x in key_tuple[1]])) == 1 and
#     #                               check_firmware_version(list(set([x['firmware_version'] for x in key_tuple[1]]))))
#     #
#     # counter_add(gen.stats_accu, 'BeaconPartialStuckEvent.total_radios', data_rdd.count())
#     #
#     # feature_rdd = gen.compose_entity_features(data_rdd)
#     #
#     # gen.logger.info("there are %d %s left for %s %s after filtering channel switch" % (
#     #     feature_rdd.count(), gen.config['entity_type'], gen.config['event_name'], gen.config['event_type']))
#     #
#     # # filter out candidate radios for cross-batch aggregation
#     # feature_rdd = feature_rdd.filter(lambda kv_tuple:
#     #                                  kv_tuple[1]['features']['sum_wlan'] > 0 and
#     #                                  kv_tuple[1]['features']['band'] == '5' and
#     #                                  kv_tuple[1]['features']['tot_cnt'] >= MIN_BATCH_SIZE and
#     #                                  kv_tuple[1]['features']['sum_client'] <= MAX_CLIENT and
#     #                                  kv_tuple[1]['features']['sum_txphyerr'] >= MIN_PHY_ERR and
#     #                                  kv_tuple[1]['features']['avg_bcn_per_wlan'] < MAX_BCN_PER_WLAN)
#     #
#     # # feature_rdd.map(lambda x: x[0]).collect()
#     # # feature_rdd.map(lambda x: x[0]).collect()
#     # ap1 = ['5c5b350e8788',   # phy_err =2    but cut 6
#     #        '5c5b350e8f08'
#     #        ]
#     # feature_rdd.filter(lambda x: x in ap1)
#     #
#     # gen.logger.info("there are %d %s left for %s %s after filtering features" % (
#     #     feature_rdd.count(), gen.config['entity_type'], gen.config['event_name'], gen.config['event_type']))
#     #
#     # event_rdd = gen.gen_intra_batch_event(feature_rdd)
#     #
#     # event_rdd = gen.cross_batch_event_correlation(event_rdd)
#     #
#     # # d = event_rdd.map(lambda x: x[0]).collect()
#     ap_1 = event_rdd.map(lambda x: x.get("ap_id")).collect()
#     #
#     # #============
#
#     # df= spark.read.parquet(s3_bucket)
#     df = spark.read.parquet(*job.input_files)
#     # df = data_rdd.toDF()
#     df.printSchema()
#
#     # Radio
#     df_radio = df.filter("uptime>86400") \
#         .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
#                 F.col("when").alias("timestamp"),
#                 F.explode("radios").alias("radio")
#                 ) \
#         .filter("radio.dev != 'r2' and radio.bandwidth>0 and not radio.radio_missing and radio.band==5") \
#         .withColumn("num_wlans", F.size("radio.wlans")) \
#         .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))
#
#
#     df_radio.filter("bcn_per_wlan < 500 and (radio.tx_phy_err>0 or radio.mac_stats_tx_phy_err>0)")
#     # df_radio.printSchema()
#
#     df_radio_nf_g = df_radio \
#         .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans", "bcn_per_wlan") \
#         .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
#         .agg(
#         F.max("num_clients").alias("max_num_clients"),
#         F.max('tx_phy_err').alias("tx_phy_err_max"),
#         F.max('mac_stats_tx_phy_err').alias("mac_stats_tx_phy_err_max"),
#         F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
#         F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
#         F.max("bcn_per_wlan").alias("bcn_per_wlan_max"),
#         F.stddev("bcn_per_wlan").alias("bcn_per_wlan_std"),
#         F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
#         F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
#         F.max("num_wlans").alias("num_wlans")
#     )
#
#     # df_radio_nf_g.filter("bcn_per_wlan < 500 and tx_phy_err_max>0")
#     df_t = df_radio_nf_g.filter("bcn_per_wlan < 500 and (tx_phy_err_max>0 or mac_stats_tx_phy_err > 0)")
#
#     df_t.select("max_num_clients", "bcn_per_wlan", "tx_phy_err_max",  "mac_stats_tx_phy_err_max", "interrupt_stats_tx_bcn_succ_max").summary().show()
#
#     # s3_path = "{fs}://mist-data-science-dev/wenfeng/test/dt={dt}/hr={hr}" \
#     #     .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
#
#     # df_radio_nf_g.coalesce(1).write.save(s3_path,
#     #                                                format='csv',
#     #                                                mode='overwrite',
#
#     Filter_query_1 = "band==5 and max_num_clients <1 and (tx_phy_err_max > 0 or mac_stats_tx_phy_err > 0)" \
#                      " and num_wlans>0 and bcn_per_wlan < 500"
#     df_radio_nf_problematic_2 = df_radio_nf_g.filter(Filter_query_1)
#     df_radio_nf_problematic_2.select("org_id", "site_id", "id", "hostname", "model").show(truncate=False)
#
#     ap_2 = list(df_radio_nf_problematic_2.select("id").toPandas()['id'])
#     ap_2 = [ x.replace("-", "") for x in ap_2]
#
#     print([x for x in ap_1 if x in ap_2])
#     print([x for x in ap_1 if x not in ap_2])
#     print([x for x in ap_2 if x not in ap_1])
#     #===========
#     # data from
#     date_day,date_hour = '2022-01-22', '13'
#     fs_test = "gs://mist-data-science-dev/shirley/aps-no-client-all-2/dt={dt}/hr={hr}".format(dt=date_day, hr=date_hour)
#
#     df = spark.read.option("header",True).csv(fs_test)
#     df.select("org_id", "site_id", "id", "hostname", "model", "recovery_time").show(truncate=False)
#

def read_data():

    from datetime import datetime,timedelta
    # dt_now = datetime.now() - timedelta(hours=3)
    # dt_now = datetime.fromtimestamp(start_time)
    # date_day = dt_now.strftime("%Y-%m-%d")
    # date_hour = dt_now.strftime("%H")
    date_day,date_hour = '2022-01-30', '*'
    fs_test = "gs://mist-data-science-dev/shirley/aps-no-client-all-2/dt={dt}/hr={hr}".format(dt=date_day, hr=date_hour)

    df = spark.read.option("header",True).csv(fs_test)
    df.select("org_id", "site_id", "id", "hostname", "model", "recovery_time").show(truncate=False)
