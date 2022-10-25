
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import json
from datetime import datetime, timedelta
from pyspark.sql.types import *
import os


env = "production"
provider = os.environ.get("CLOUD_PROVIDER", "aws")
# provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"
app_name = "ap-radios"

spark = SparkSession \
    .builder \
    .appName("") \
    .getOrCreate()

spark.sparkContext.setLogLevel("warn")

env = "production"
provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

now = datetime.now() - timedelta(hours=1)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")

date_day = "2022-10-24"
# date_day = "2022-10-24"
date_hour = "*"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)

df = spark.read.parquet(s3_bucket)
df.printSchema()

# only check AP41-* for now
# df = df.filter(F.col("model").startswith('AP41'))


def get_df_name(fs):
    """
      Get Org and site name
    :param fs:
    :return:
    """
    df_org = spark.read.parquet("{fs}://mist-secorapp-production/dimension/org".format(fs=fs)) \
        .select(F.col("id").alias("OrgID"),F.col("name").alias("org_name"))
    df_name = spark.read.parquet("{fs}://mist-secorapp-production/dimension/site/site.parquet".format(fs=fs)) \
        .select(F.col("id").alias("SiteID"),F.col("name").alias("site_name"),F.col("org_id").alias("OrgID")).join(df_org,["OrgID"]) \
        .select("org_name","site_name","OrgID","SiteID")

    t_org_1 = df_name.select(['OrgID','org_name']).withColumnRenamed('OrgID', 'org_id').dropDuplicates()
    t_org_2 = df_name.select(['SiteID','site_name']).withColumnRenamed('SiteID', 'site_id').dropDuplicates()
    return df_name

df_name = get_df_name(fs)

def flatten_radios(df):
    """
    flatten radios: r0/r1/r2
    :param df:
    :return:
    """
    df_radios = df.select("org_id", "site_id", "id", "terminator_timestamp",  "hostname", "firmware_version", "model",
                          "cpu_total_time", "cpu_user", "cpu_system",
                            *[F.col('radios')[i].alias(f'r{i}') for i in range(3)]
                          ) \
        .withColumn("date_hour", F.from_unixtime(F.col('terminator_timestamp')/1_000_000, format='yyyy-MM-dd HH:mm:ss')) \
        .select('*', F.col("r2.re_init").alias("r2_re_init"),
                F.col("r0.interrupt_stats_tx_bcn_succ").alias("r0_interrupt_stats_tx_bcn_succ"),
                F.col("r1.interrupt_stats_tx_bcn_succ").alias("r1_interrupt_stats_tx_bcn_succ")
                )

    prev_cols = ["date_hour", "r2_re_init",  "r0_interrupt_stats_tx_bcn_succ", "r1_interrupt_stats_tx_bcn_succ"]
    shiftAmount = -1
    window = Window.partitionBy(F.col('id')).orderBy(F.col('terminator_timestamp').desc())

    df_radios = df_radios.select("*",
                             *[F.lag(c, offset=shiftAmount).over(window).alias("prev_" + c) for c in prev_cols]) \
        .withColumn("time_diff", F.col("terminator_timestamp") - F.col("prev_terminator_timestamp")) \
        .withColumn("r2_re_init_diff", (F.col("r2_re_init") - F.col("pre_r2_re_init")) ) \
        .withColumn("r0_bcn_drop", (F.col("r0_interrupt_stats_tx_bcn_succ") - F.col("prev_r0_interrupt_stats_tx_bcn_succ")) ) \
        .withColumn("r1_bcn_drop", (F.col("r1_interrupt_stats_tx_bcn_succ") - F.col("prev_r1_interrupt_stats_tx_bcn_succ")) )

    return df_radios
df_ap_radios = flatten_radios(df)
df_ap_radios.printSchema()

# from pyspark.ml.stat import Correlation
# check_cols = ["r2_re_init",  "r0_interrupt_stats_tx_bcn_succ", "r1_interrupt_stats_tx_bcn_succ"]
# r1 = Correlation.corr(df_ap41_radios, "r2_re_init",  "r0_interrupt_stats_tx_bcn_succ", "r1_interrupt_stats_tx_bcn_succ").head()
# print("Pearson correlation matrix:\n" + str(r1[0]))
#
#
# df_ap41_radios = df_ap41_radios.na.drop()
# check_cols = ["r2_re_init",  "r0_interrupt_stats_tx_bcn_succ", "r1_interrupt_stats_tx_bcn_succ"]
# # df_ap41_radios = df_ap41_radios.filter(F.col)
# df_corr = get_correlation_matrix_from_df(df_ap41_radios, check_cols)


# a naive  selection
df_ap_radios_problematic = df_ap_radios.filter("r2_re_init_diff > 0 and r1_bcn_drop < -10")


df_ap_radios_problematic= df_ap_radios_problematic.join(df_name, df_ap_radios_problematic.site_id == df_name.SiteID, how='left')
# scope: org/site/aps


# impacted aps
df_ap41_radios_problematic_aps = df_ap_radios_problematic.select("org_id",'org_name', 'site_id', 'site_name',
                                                                 "id",  "hostname", "firmware_version", "model")\
        .groupBy("org_id", 'org_name', 'site_id', 'site_name',
                 "id",  "hostname", "firmware_version", "model").count()

df_ap41_radios_problematic_aps.persist()
n_aps = df_ap41_radios_problematic_aps.count()
print("problematic aps {}".format(n_aps))
df_ap41_radios_problematic_aps.orderBy(F.col("count").desc()).show(n_aps, truncate=False)


# only check high impacted
df_ap41_radios_problematic_aps = df_ap41_radios_problematic_aps.filter("count>50")

#impact scopes
print("df_impact_scopes")
df_impact_scopes = df_ap41_radios_problematic_aps.agg(
    F.countDistinct("org_id").alias("impacted_orgs"),
    F.countDistinct("site_id").alias("impacted_sites"),
    F.countDistinct("id").alias("impacted_aps"),
    F.countDistinct("model").alias("models")
)
df_impact_scopes.show()

# impacted models
print("impact models")
df_impact_models = df_ap41_radios_problematic_aps.groupBy("model").agg(
    F.countDistinct("org_id").alias("impacted_orgs"),
    F.countDistinct("site_id").alias("impacted_sites"),
    F.countDistinct("id").alias("impacted_aps"),
    F.countDistinct("model").alias("models")
)
df_impact_models.show(100, truncate=False)

#impact models
print("df_impact_orgs")
df_impact_orgs = df_ap41_radios_problematic_aps.groupBy("org_id", "org_name", "model").agg(
    F.countDistinct("org_id").alias("impacted_orgs"),
    F.countDistinct("site_id").alias("impacted_sites"),
    F.countDistinct("id").alias("impacted_aps"),
    F.countDistinct("model").alias("models")
).orderBy(F.col("impacted_aps").desc())
df_impact_orgs.show(20, truncate=False)



def save_df_to_fs(df, date_day, date_hour, app_name="selected-aps", band=""):
    date_day = date_day.replace("[", "").replace("]", "").replace("*", "000")
    date_hour = date_hour.replace("*", "000")
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}_{band}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name=app_name, band=band, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    return s3_path
save_df_to_fs(df_ap41_radios_problematic_aps, date_day, date_hour, app_name, "")

#