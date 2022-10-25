# import matplotlib.pyplot as plt
# %matplotlib inline

import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta
env = "production"
provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

now = datetime.now() - timedelta(hours=1)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")

# date_day = "2021-10-2[78]"
# date_day = "2022-01-1[8]"
# date_hour = "21"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

print(s3_bucket)

df = spark.read.parquet(s3_bucket)
df.printSchema()

def get_aps_per_org():
    df_g = df.groupBy("org_id").agg(
        F.countDistinct("id").alias("aps")
    )
    df_g.orderBy(F.col("aps").desc()).show(truncate=False)
# get_aps_per_org()

# Radio
df_bad_radio = df.filter("uptime>86400") \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
            F.col("when").alias("timestamp"),
            F.explode("radios").alias("radio")
            ) \
    .withColumn("num_wlans", F.size("radio.wlans")) \
    .filter("num_wlans>0 and radio.channel==0") \
    .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))


df_radio_nf_g = df_bad_radio \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans", "bcn_per_wlan") \
    .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
    .agg(
    F.max("num_clients").alias("max_num_clients"),
    F.max('tx_phy_err').alias("max_tx_phy_err"),
    F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
    F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
    F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
    F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
    F.max("num_wlans").alias("num_wlans"),
    F.max("channel").alias("max_channel"),
    F.max("max_tx_power").alias("max_tx_power")
)

df_radio_nf_g.count()

# df_bad_radio_g = df_bad_radio.select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans") \
#     .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band", "dev", "num_wlans", "channel", "max_tx_power").count()

# df_bad_radio_g.count()

df_radio_nf_g.orderBy("org_id", "site_id", "band").show(truncate=False)

