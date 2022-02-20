
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta

spark = SparkSession \
    .builder \
    .appName("ap-zero-clients") \
    .getOrCreate()

env = "production"
# provider = "aws"
provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

detect_time = datetime.now() - timedelta(hours=1)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)

df= spark.read.parquet(s3_bucket)
df.printSchema()

# Radio
df_radio = df.filter("uptime>86400") \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
            F.col("when").alias("timestamp"),
            F.explode("radios").alias("radio")
            ) \
    .filter("radio.dev != 'r2' and radio.bandwidth>0 and not radio.radio_missing and radio.band==5") \
    .withColumn("num_wlans", F.size("radio.wlans")) \
    .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))

# df_radio.printSchema()

df_radio_nf_g = df_radio \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans", "bcn_per_wlan") \
    .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
    .agg(
    F.max("num_clients").alias("max_num_clients"),
    F.max('tx_phy_err').alias("max_tx_phy_err"),
    F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
    F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
    F.max("bcn_per_wlan").alias("bcn_per_wlan_max"),
    F.stddev("bcn_per_wlan").alias("bcn_per_wlan_std"),
    F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
    F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
    F.max("num_wlans").alias("num_wlans")
)

# s3_path = "{fs}://mist-data-science-dev/wenfeng/test/dt={dt}/hr={hr}" \
#     .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)

# df_radio_nf_g.coalesce(1).write.save(s3_path,
#                                                format='csv',
#                                                mode='overwrite',

Filter_query_1 = "band==5 and max_num_clients <1 and max_tx_phy_err > 0 and num_wlans>0 and bcn_per_wlan < 500"
df_radio_nf_problematic_2 = df_radio_nf_g.filter(Filter_query_1)


def save_df_to_fs(df_radio_nf_problematic, date_day, date_hour):
    s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-all/dt={dt}/hr={hr}" \
        .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)

    df_radio_nf_problematic.coalesce(1).write.save(s3_path,
                                                   format='csv',
                                                   mode='overwrite',
                                                   header=True)

save_df_to_fs(df_radio_nf_problematic, date_day, date_hour)


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

