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
# date_hour = "1*"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

print(s3_bucket)

df= spark.read.parquet(s3_bucket)
df.printSchema()

def get_aps_per_org():
    df_g = df.groupBy("org_id").agg(
        F.countDistinct("id").alias("aps")
    )
    df_g.orderBy(F.col("aps").desc()).show(truncate=False)
# get_aps_per_org()

# Radio
df_radio = df.filter("uptime>86400")\
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
                     F.col("when").alias("timestamp"),
                     F.explode("radios").alias("radio")
                     )\
    .filter("radio.dev != 'r2' and radio.bandwidth>0 and not radio.radio_missing and radio.band==5")\
    .withColumn("num_wlans", F.size("radio.wlans"))\
    .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))

# df_radio.printSchema()

df_radio_nf_g = df_radio\
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans", "bcn_per_wlan") \
    .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
    .agg(
    F.max("num_clients").alias("max_num_clients"),
    F.max('tx_phy_err').alias("max_tx_phy_err"),
    F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
    F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
    F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
    F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
    F.max("num_wlans").alias("num_wlans")
)


Filter_query_1 = "band==5 and max_num_clients <1 and max_tx_phy_err>=0 and num_wlans>0 and bcn_per_wlan < 500"
df_radio_nf_problematic_1 = df_radio_nf_g.filter(Filter_query_1)


def save_fs(date_day, date_hr):
    s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-all/dt={dt}/hr={hr}" \
        .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hr)

    df_radio_nf_problematic.coalesce(1).write.save(s3_path,
                                                   format='csv',
                                                   mode='overwrite',
                                                   header=True)

# df_radio_nf = df_radio.select("org_id", "site_id", "id", "hostname", "firmware_version" , "model",
#                               "timestamp",  "radio.noise_floor", "radio.num_clients",
#                               "radio.band",
#                               "radio.tx_phy_err",
#                               "radio.interrupt_stats_tx_bcn_succ",
#                               F.col("radio.utilization_non_wifi").alias("chan_util_non_wifi"),
#                               F.col("radio.utilization_unknown_wifi").alias("chan_util_unknown_wifi"),
#                               F.size("radio.wlans").alias("num_wlans"),
#                               )\
#     .withColumn("bcn_per_wlan", F.col("interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))
#
# df_radio_nf.show()

df_radio_nf_g = df_radio_nf \
    .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "band","num_clients", "tx_phy_err",
            "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan") \
    .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
    .agg(
    F.max("num_clients").alias("max_num_clients"),
    F.max('tx_phy_err').alias("max_tx_phy_err"),
    F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
    F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
    F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
    F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
    F.max("num_wlans").alias("num_wlans")
)



Filter_query_1 = "band==5 and max_num_clients <1 and max_tx_phy_err>=0 and num_wlans>0 and bcn_per_wlan < 500"
df_radio_nf_problematic_1 = df_radio_nf_g.filter(Filter_query_1)


Filter_query = "band==5 and max_num_clients <1 and max_tx_phy_err>0 and num_wlans>0 and bcn_per_wlan < 500"
df_radio_nf_problematic = df_radio_nf_g.filter(Filter_query)

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

    df_radio_nf_problematic.groupby("firmware_version" , "model")\
        .count().orderBy(F.col("count").desc())\
        .show(truncate=False)

df_radio_nf_problematic.persist()

df_radio_nf_problematic.count()

s3_path = "{fs}://mist-data-science-dev/wenfeng/aps-no-client-all/dt={dt}"\
    .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""))

df_radio_nf_problematic.coalesce(1).write.save(s3_path,
                                     format='csv',
                                     mode='overwrite',
                                     header=True)

df_radio_nf_problematic.count()


df_new = spark.read.option("header",True).csv(s3_path + "/*.csv")
df_new.show()

df_new = df_new.alias('df_new')
df_old = df_old.alias('df_old')
df_join= df_new.join(df_old, df_new.id == df_old.id)
df_join.select("df_new.*").show()

from pyspark.sql.types import StringType
def get_ap_names(hostname):
    return "".join([hostname[-3], "00"])
ap_names_family = F.udf(get_ap_names, StringType())

df_radio_nf_problematic_2 = df_radio_nf_problematic\
    .filter(F.col("hostname").like("APUS-%-%"))\
    .withColumn("ap_series", ap_names_family(F.col("hostname")))

df_radio_nf_problematic_2.select("ap_series").groupBy("ap_series").count().show()


# df_radio_nf_problematic_remove_outdoor = df_radio_nf_problematic.filter(F.col("model").like("%E-%"))
# Filter_query = "band==5 and num_clients < 1 and tx_phy_err > 0 and num_wlans > 0 and bcn_per_wlan < 500"
# df_radio_nf_problematic = df_radio_nf.filter(Filter_query)
#
# df_radio_nf_problematic_g = df_radio_nf_problematic \
#     .select("org_id", "site_id", "id", "band","num_clients", "tx_phy_err",
#             "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan") \
#     .groupBy("org_id", "site_id", "id", "band") \
#     .agg(
#     F.avg("num_clients").alias("num_clients"),
#     F.avg('tx_phy_err').alias("tx_phy_err"),
#     F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
#     # F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
#     F.avg("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_avg"),
#     F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
#     F.max("num_wlans").alias("num_wlans")
# )
# df_radio_nf_problematic_g1 = df_radio_nf_problematic_g.groupBy("org_id").agg(
#     F.countDistinct("id").alias("aps")
# )
# df_radio_nf_problematic_g1.show()
#
# # df_walmart_problemaics_g.write()
# s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-no-client-all/".format(fs=fs)
# df_radio_nf_problematic_g.write.save(s3_path,
#                                     format='csv',
#                                     mode='overwrite',
#                                     header=True)


org_sams = "dfdd2428-7627-49ed-9800-87f7c61972d3"  # Sam's club
org_walmart = "6d49ba5d-26f5-4d32-a787-abdaeb31a994"  # walmart

df_walmart = df_radio_nf.filter(F.col("org_id") == org_walmart)  # Walmart
df_walmart.show()

# #
# df_walmart.select( "num_clients", "tx_phy_err","interrupt_stats_tx_bcn_succ","num_wlans").summary().show()
#
# df_walmart_problemaics = df_walmart.filter("num_clients <1 and tx_phy_err>0 and num_wlans>0")
# df_walmart_problemaics.select("num_clients", "tx_phy_err", "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan").summary().show()
#
#
# df_walmart_problemaics_g = df_walmart_problemaics\
#     .select("site_id", "id", "band","num_clients", "tx_phy_err",
#             "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan")\
#     .groupBy("site_id", "id", "band")\
#     .agg(
#     F.avg("num_clients").alias("num_clients"),
#     F.avg('tx_phy_err').alias("tx_phy_err"),
#     F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
#     # F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
#     F.avg("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_avg"),
#     F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
#     F.max("num_wlans").alias("num_wlans")
# )
#
# df_walmart_problemaics_g.count()
# df_walmart_problemaics_g.show()
# df_walmart_problemaics_g.summary().show()
#
# # df_walmart_problemaics_g.write()
# s3_path = "gs://mist-data-science-dev/wenfeng/ap-no-client-walmart/"
# df_walmart_problemaics_g.write.save(s3_path,
#                          format='csv',
#                          mode='overwrite',
#                          header=True)
#
# #
# df_sam = df_radio_nf.filter(F.col("org_id") == "dfdd2428-7627-49ed-9800-87f7c61972d3")  # Sam's club
# df_sam.show()
#
# df_sam.select( "radio.num_clients", "radio.tx_phy_err",
#                "radio.interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan").summary().show()
#
#
# df_sam_problemaics = df_sam.filter("radio.num_clients <1 and radio.tx_phy_err>0 and num_wlans>0")
# df_sam_problemaics.select("radio.num_clients", "radio.tx_phy_err",
#                           "radio.interrupt_stats_tx_bcn_succ", "num_wlans").summary().show()
#
#
# df_sam_problemaics_g = df_sam_problemaics \
#     .select("site_id", "id", "band","num_clients", "tx_phy_err",
#             "interrupt_stats_tx_bcn_succ", "num_wlans", "bcn_per_wlan") \
#     .groupBy("site_id", "id", "band") \
#     .agg(
#     F.avg("num_clients").alias("num_clients"),
#     F.avg('tx_phy_err').alias("tx_phy_err"),
#     F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
#     F.avg("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_avg"),
#     F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
#     F.max("num_wlans").alias("num_wlans")
# )
#
# df_sam_problemaics_g = df_sam_problemaics.select("site_id", "id", "radio.band").groupBy("site_id", "id", "radio.band").count()
# df_sam_problemaics_g.count()
# df_sam_problemaics_g.show()
# df_sam_problemaics_g.summary().show()
#
#
# s3_path = "gs://mist-data-science-dev/wenfeng/ap-no-client-sams/"
# df_sam_problemaics_g.write.save(s3_path,
#                                     format='csv',
#                                     mode='overwrite',
#                                     header=True)
