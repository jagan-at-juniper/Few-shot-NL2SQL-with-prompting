
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

date_day = "2022-08-30"
date_hour = "*"


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
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "uptime",  "timestamp",  "radio.*") \
    .withColumn("num_wlans", F.size("wlans")) \
    .filter("dev != 'r2' and bandwidth>0 and not radio_missing and band==5 and num_wlans>0") \
    .withColumn("bcn_per_wlan1", bcn_per_wlan_norm(F.col("interrupt_stats_tx_bcn_succ"), F.col("num_wlans"), F.col("model"))) \
    .withColumn("bcn_per_wlan", F.when(F.col("model").rlike("AP41"), F.col("interrupt_stats_tx_bcn_succ")) \
                .otherwise(F.col("interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))) \
    .withColumn("bcn_per_wlan_drop", F.col("bcn_per_wlan")<500.0) \
    .withColumn("noise_floor_high", F.col("noise_floor")>-60.0) \
    .withColumn("tx_phy_err_high", F.col("tx_phy_err")>1.0) \
    .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err")>1.0) \
    .withColumn("mac_stats_tx_phy_err_high", F.col("mac_stats_tx_phy_err")>1.0) \
    .withColumn("utilization_non_wifi_high", F.col("utilization_non_wifi")>0.30)


def get_bcn_df(df_radio):
    df_radio_with_bcn_drop = df_radio.filter("bcn_per_wlan_drop") \
        .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band").agg(
        F.min("uptime").alias("min_uptime"),
        F.sum("re_init").alias("re_init"),
        F.collect_set("channel").alias("channels"),
        F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
        F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
        F.max("bcn_per_wlan").alias("bcn_per_wlan_max"),
        F.max("num_clients").alias("max_num_clients"),
        F.min("timestamp").alias("start_timestamp"),
        F.max("timestamp").alias("end_timestamp")
    ).withColumn("channel_switched", F.size("channels") > 1)\
        .withColumn("bcn_drop_time", (F.col("end_timestamp") - F.col("start_timestamp"))/1_000_000)
    return df_radio_with_bcn_drop

df_radio_with_bcn_drop = get_bcn_df(df_radio)

save_df_to_fs(df_radio_with_bcn_drop, date_day, date_hour, "aps-bcn-drop")
