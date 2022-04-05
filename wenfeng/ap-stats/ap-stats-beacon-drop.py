from pyspark.sql.functions import udf, size, avg, count, col,sum, explode

import matplotlib.pyplot as plt
%matplotlib inline

import pyspark.sql.functions as F

import json
from datetime import datetime,timedelta
env = "production"
# env = "staging"

# provider = "aws"
provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

now = datetime.now()  - timedelta(hours=3)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")

date_day = "2022-03-2*"
date_hour = "*"

def get_df_radio(date_day, date_hour):
    s3_bucket = "gs://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(env=env)
    s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

    print(s3_bucket)

    df= spark.read.parquet(s3_bucket)
    # df.printSchema()


    df_radio = df.select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
                         F.col("when").alias("timestamp"),
                         F.explode("radios").alias("radio")
                         ) \
        .filter("radio.dev != 'r2' and radio.bandwidth>0 and not radio.radio_missing and radio.band==5") \
        .withColumn("num_wlans", F.size("radio.wlans")) \
        .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans")) \
        .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "num_wlans",'bcn_per_wlan', "timestamp",  "radio.*")


    #         df_site = df_site.withColumn("date_time",F.from_unixtime(F.col("timestamp")/1_000_000))\
    #             .orderBy(F.col('timestamp').asc())

    return df_radio





site_id = 'ce7517e9-43dc-406d-9ec2-cf8f0d2c3767'
ap_id = 'd4-20-b0-40-03-f7'

site_id = '56f7b9b3-3653-4f9c-8da0-1909a3806868'  #  Site[1997] under Org[US - WALMART] of Admin[?],
ap_id = 'd4-20-b0-c2-20-b0'  # 56f7b9b3-3653-4f9c-8da0-1909a3806868

site_id = 'cac37d7b-c8f6-495d-94e3-b9f794cb887f'
ap_id = 'd4-dc-09-21-ae-46'

site_id = 'b3034bae-08ab-4751-ba69-dcb5c43251da'
ap_id = 'd4-20-b0-c0-47-bd'
new_data = True


s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-site-reinite/dt={dt}".format(fs=fs, dt=date_day)
print(s3_path)

if new_data:
    df_radio = get_df_radio(date_day, date_hour)
    df_site = df_radio.filter(F.col("site_id")== site_id)
    df_site.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    df_site = df_radio.filter(F.col("site_id")== site_id)
else:
    df_site = spark.read.parquet("gs://mist-data-science-dev/wenfeng/ap-site-reinite/dt=2022-03-10/*.parquet")

df_site.printSchema()
df_site = df_site.withColumn("date_time",F.from_unixtime(F.col("timestamp")/1_000_000)) \
    .orderBy(F.col('timestamp').asc())

cols = ["org_id", "site_id", "id", "hostname", "firmware_version", "model",
        "date_time",
        "num_clients",
        "dev", "band", "radio_missing", "bandwidth",
        "tx_phy_err",  "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan", "re_init", "re_init_throttle", "channel"]


cols = ["org_id", "site_id", "id", "hostname", "firmware_version", "model", "date_time", "num_clients",
        "tx_phy_err",  "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan", "re_init", "re_init_throttle", "channel"]


df_site.select(cols).show()

print(df_site.count())

df_ap = df_site.filter(F.col("id")== ap_id).select(cols)
df_ap.count()


import matplotlib.pyplot as plt
# import numpy as np
# if using a Jupyter notebook, includue:
%matplotlib inline

# plt.hist(df_radio_nf_g_43_pd["bcn_per_wlan"])

df_site_pd = df_site.toPandas()
df_ap_pd = df_ap.toPandas()


df_ap_pd.plot(x="date_time", y=['num_clients', 'interrupt_stats_tx_bcn_succ', 'tx_phy_err', 're_init'], figsize=(10, 15), subplots=True)
plt.xticks(rotation=90)
