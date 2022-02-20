# import matplotlib.pyplot as plt
# %matplotlib inline

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta


spark = SparkSession \
    .builder \
    .appName("ap-zero-clients") \
    .getOrCreate()


env = "production"
provider = "aws"
provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

now = datetime.now() - timedelta(hours=1)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")


org_sams = "dfdd2428-7627-49ed-9800-87f7c61972d3"  # Sam's club
org_walmart = "6d49ba5d-26f5-4d32-a787-abdaeb31a994"  # walmart

now = datetime.now()  - timedelta(hours=3)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")
date_hour = "*"
def get_df_disabled_radio():
    s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
    s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
    print(s3_bucket)

    df= spark.read.parquet(s3_bucket)
    # df.printSchema()

    #        .filter(F.col("model").rlike("AP41|AP61")) \
    df_radio = df.filter(F.col("org_id") != "")\
        .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
                F.col("when").alias("timestamp"),
                F.explode("radios").alias("radio")
                ) \
        .withColumn("num_wlans", F.size("radio.wlans")) \
        .filter("radio.dev != 'r2' and radio.radio_missing") \
        .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))

    df_radio.count()
    df_radio_1 = df_radio \
        .select("org_id", "site_id", "model", "id", "hostname",
                "radio.band", "radio.dev", "radio.bandwidth", "radio.radio_missing", "num_wlans" ) \
        .groupBy("org_id", "site_id", "model", "id", "hostname",  "band", "dev") \
        .agg(
             F.max("bandwidth").alias("bandwidth"),
             F.max("radio_missing").alias("radio_missing"),
             F.max("num_wlans").alias("num_wlans")
        )
    df_radio_1.count()
    # df_radio.filter("radio_missing")

    return df_radio_1

df_disabled_radio = get_df_disabled_radio()

s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-disabled-radios/".format(fs=fs)
df_disabled_radio.coalesce(1).write.save(s3_path,  format='csv',  mode='overwrite',   header=True)


# df_disabled_radio = spark.read.option("header", True).csv(s3_path)
print("df_disabled_radio = ", df_disabled_radio.count())


df_disabled_radio_model_radio = df_disabled_radio.select("model", "band", "id")\
        .groupBy("model", "band")\
        .agg(F.countDistinct("id").alias("aps"))
df_disabled_radio_model_radio.orderBy("model", "band").show(df_disabled_radio_model_radio.count(), truncate=False)

#
df_disabled_radio_org = df_disabled_radio \
    .select("org_id",  "model", "id", "band", "dev", "bandwidth", "radio_missing", "num_wlans" ) \
    .groupBy("org_id",  "model",  "band", "dev", "bandwidth", "radio_missing", "num_wlans" ) \
    .agg(F.countDistinct("id").alias("disabled_radios")) \
    .orderBy(F.col("disabled_radios").desc())

from pyspark.sql.types import *
import requests
def get_org_name(org):
    url = "http://papi-production.mist.pvt/internal/orgs/{org}".format(org=org)
    res = requests.get(url, timeout=10)
    return res.json().get("name")
get_org_name_f = F.udf(get_org_name, StringType())
df_disabled_radio_org = df_disabled_radio_org.withColumn("org_name", get_org_name_f(F.col("org_id")))

s3_org_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-disabled-radios-org/".format(fs=fs)
df_disabled_radio_org.coalesce(1).write.save(s3_org_path,  format='csv',  mode='overwrite',   header=True)

# df_disabled_radio_org = spark.read.option("header", True).csv(s3_org_path)
print("df_disabled_radio_org = ", df_disabled_radio_org.count())
# df_disabled_radio_org.orderBy("band", "model").orderBy("disabled_radios").show(100, truncate=False)

df_1 = df_disabled_radio_org.filter(F.col("model").rlike("AP41|AP61"))
df_1.orderBy("org_id", F.col("disabled_radios").desc(), "band", "model").show(200, truncate=False)

#
# df_2 = df_disabled_radio_org\
#     .filter(F.col("radio_missIng"))\
#     .filter(F.col("model").rlike("AP41|AP61"))
#
# df_2.select("band").groupBy("band").count().show()
#
# df_2.orderBy(F.col("disabled_radios").desc(), "org_id", "model").show(100, truncate=False)


# def save_fs(date_day, date_hr):
#     s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-disabled-radios/dt={dt}/hr={hr}" \
#         .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hr)
#
#     df_disabled_radio.coalesce(1).write.save(s3_path,
#                                                    format='csv',
#                                                    mode='overwrite',
#                                                    header=True)
