
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
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

# date_day = "2021-10-2[78]"
# date_day = "2022-10-24"
# date_hour = "*"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)

df = spark.read.parquet(s3_bucket)
df.printSchema()

# only check AP41-* for now
df_ap41 = df.filter(F.col("model").startswith('AP41'))


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
    return df_radios
df_ap41_radios = flatten_radios(df_ap41)
df_ap41_radios.printSchema()

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
df_ap41_radios_problematic = df_ap41_radios.filter("r2_re_init > 0 and r1_interrupt_stats_tx_bcn_succ < 500")

# scope: org/site/aps
df_impact_scopes = df_ap41_radios_problematic.agg(
    F.countDistinct("org_id").alias("impacted_orgs"),
    F.countDistinct("site_id").alias("impacted_sites"),
    F.countDistinct("id").alias("impacted_aps"),
)

df_impact_scopes.show()


# impacted aps
df_ap41_radios_problematic_aps = df_ap41_radios_problematic.select("org_id", "site_id", "id",  "hostname", "firmware_version", "model")\
        .groupBy("org_id", "site_id", "id",  "hostname", "firmware_version", "model").count()
df_ap41_radios_problematic_aps.orderBy("count").show()

