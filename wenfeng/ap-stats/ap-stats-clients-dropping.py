from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime, timedelta
from pyspark.sql.types import *
import os
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.window import Window

env = "production"
provider = os.environ.get("CLOUD_PROVIDER", "aws")
# provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

spark = SparkSession \
    .builder \
    .appName("clients-dropping") \
    .getOrCreate()

spark.sparkContext.setLogLevel("warn")


# @F.udf(IntegerType)
def bcn_per_wlan(bcn, num_wlans, model=""):
    bcn = bcn if bcn != None else -1.0
    bcn_high = 700.0
    if model.find("AP41") > -1:
        bcn_norm = bcn if bcn < bcn_high else bcn_high
    else:
        bcn_norm = bcn / num_wlans if num_wlans > 0 else 0
    bcn_norm = bcn_norm if bcn_norm < bcn_high else bcn_high
    return float(bcn_norm)


bcn_per_wlan_norm = F.udf(bcn_per_wlan, FloatType())


def save_df_to_fs(df, date_day, date_hour, app_name="aps-no-client-all", band=""):
    date_hour = "000" if date_hour == "*" else date_hour
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}_{band}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name=app_name, band=band, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    return s3_path


detect_time = datetime.now() - timedelta(hours=1)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

date_day = "2022-05-25"
date_hour = "*"
band = "5"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)

df = spark.read.parquet(s3_bucket)
df.printSchema()


def get_df_radio(df, band):
    # Radio
    radio_filter = "dev != 'r2' and bandwidth>0 and not radio_missing  and band={band} and num_wlans>0".format(
        band=band)
    print(band, radio_filter)

    df_radios = df.filter("uptime>86400") \
        .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "terminator_timestamp",
                F.col("when").alias("timestamp"),
                F.explode("radios").alias("radio")
                ) \
        .withColumn("date_hour", F.from_unixtime(F.col('terminator_timestamp')/1_000_000, format='yyyy-MM-dd HH:mm:ss'))\
        .select("*",  "radio.*").drop("radio") \
        .withColumn("num_wlans", F.size("wlans")) \
        .filter(radio_filter) \
        .withColumn("bcn_per_wlan",
                    bcn_per_wlan_norm(F.col("interrupt_stats_tx_bcn_succ"), F.col("num_wlans"), F.col("model")))

    prev_cols = ["date_hour", "terminator_timestamp",  "channel", "bandwidth", "num_clients", "bcn_per_wlan"]
    shiftAmount = -1
    window = Window.partitionBy(F.col('id'), F.col('dev')).orderBy(F.col('terminator_timestamp').desc())
    df_radios = df_radios.select("*",
                                 *[F.lag(c, offset=shiftAmount).over(window).alias("prev_" + c) for c in prev_cols]) \
        .withColumn("time_diff", F.col("terminator_timestamp") - F.col("prev_terminator_timestamp")) \
        .withColumn("num_client_diff", (F.col("num_clients") - F.col("prev_num_clients")) / 1_000_000) \
        .withColumn("bcn_drop", (F.col("bcn_per_wlan") - F.col("prev_bcn_per_wlan")) ) \
        .withColumn("channel_updated", F.col("channel") != F.col("prev_channel")) \
        .withColumn("bandwidth_updated", F.col("bandwidth") != F.col("prev_bandwidth"))



    return df_radios


def get_df_name(fs, scope='device'):
    df_name = spark.read.parquet("{fs}://mist-secorapp-production/dimension/{scope}/{scope}.parquet".format(fs=fs, scope = scope)) \
        .select(F.col("id").alias("SiteID"),F.col("name").alias("site_name"),F.col("org_id").alias("OrgID")).join(df_org,["OrgID"]) \
        .select("org_name","site_name","OrgID","SiteID")

    t_org_1 = df_name.select(['OrgID','org_name']).withColumnRenamed('OrgID', 'org_id').dropDuplicates()
    t_org_2 = df_name.select(['SiteID','site_name']).withColumnRenamed('SiteID', 'site_id').dropDuplicates()
    return df_name

df_name = get_df_name(fs)



def ap_scope(df_query, df_name):
    if 'org_name' not in df_query.columns:
        df_query= df_query.join(df_name, df_query.site_id == df_name.SiteID, how='left')

    df_query.select('org_id', 'org_name').groupBy("org_id", 'org_name').count().orderBy(F.col("count").desc()).show(truncate=False)

    df_query.select('org_id','org_name', 'site_id', 'site_name') \
        .groupBy("org_id", 'org_name', 'site_id', 'site_name') \
        .count() \
        .orderBy(F.col("count").desc()).show(truncate=False)


    df_query.select('org_id','org_name', 'site_id', 'site_name', 'id').groupBy('org_id','org_name', 'site_id', 'site_name', 'id') \
        .count().orderBy(F.col("count").desc()) \
        .show(truncate=False)

    # %%pretty
    df_query.select('model').groupBy('model').count().orderBy(F.col("count").desc()).show(truncate=False)

    df_query.select('model', 'firmware_version').groupBy('model', 'firmware_version').count().orderBy(F.col("count").desc()).show(truncate=False)

    return df_query




df_radios = get_df_radio(df, band)
df_suspicious_radios = df_radios.filter(" not channel_updated and prev_num_clients>3 and num_clients<1")

prev_cols = ["date_hour", "terminator_timestamp", "channel", "bandwidth", "num_clients", "bcn_per_wlan"]
test_cols = ["id", "dev"] + prev_cols + [ "prev_" + c for c in prev_cols] + \
            ["time_diff", "num_client_diff", "bcn_drop", "channel_updated", "bandwidth_updated", "date_hour", "prev_date_hour"]

df_suspicious_radios.select(test_cols).show()

print("save to fs")
s3_path_1 = save_df_to_fs(df_suspicious_radios, date_day, date_hour, "clients-dropping", band)
print(s3_path_1)


df_suspicious_radios.select("num_clients", "prev_num_clients", "num_client_diff", "bcn_per_wlan", "prev_bcn_per_wlan", "bcn_drop").summary().show()

df_suspicious_radios= df_suspicious_radios \
    .withColumn("bcn_drop_large", F.col("bcn_drop")>100) \
    .withColumn("noise_floor_high", F.col("noise_floor") > -65.0) \
    .withColumn("tx_phy_err_high", F.col("tx_phy_err") > 1.0) \
    .withColumn("tx_error_high", (F.col("tx_failed") + F.col("tx_retried"))/ F.col("tx_pkts") >0.1) \
    .withColumn("rx_error_high", (F.col("rx_dups") + F.col("rx_errors"))/ F.col("rx_pkts")>0.1) \
    .withColumn("utilization_non_wifi_high", F.col("utilization_non_wifi") > 0.20)\


cols = ["bcn_drop_large", "channel_updated", "bandwidth_updated","noise_floor_high","utilization_non_wifi_high" ,
        "tx_phy_err_high", "tx_error_high", "rx_error_high"]
df_suspicious_radios_g = df_suspicious_radios.select(cols).groupBy(cols).count().orderBy(cols)

nrows = df_suspicious_radios_g.count()
df_suspicious_radios_g.show(nrows, truncate=False)

df_suspicious_radios=ap_scope(df_suspicious_radios, df_name)