
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
app_name = "ap-select-aps"

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
date_day = "2022-10-19"
date_hour = "*"

s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

print(s3_bucket)

df = spark.read.parquet(s3_bucket)
df.printSchema()

aps = ["ac-23-16-e9-33-4b", "ac-23-16-e9-31-ca", "ac-23-16-e9-31-4d"]
aps = ["5c-5b-35-3e-9f-6f"]
df_aps = df.filter(df.id.isin(aps))

def save_df_to_fs(df, date_day, date_hour, app_name="selected-aps", band=""):
    date_day = date_day.replace("[", "").replace("]", "").replace("*", "000")
    date_hour = date_hour.replace("*", "000")
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}_{band}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name=app_name, band=band, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    return s3_path


save_df_to_fs(df_aps, date_day, date_hour, app_name)

print(df_aps.count())


def flatten_radios(df):
    df_radios = df.select("org_id", "site_id", "id", "terminator_timestamp",  "hostname", "firmware_version", "model","cpu_total_time", "cpu_user", "cpu_system",
                      *[F.col('radios')[i].alias(f'r{i}') for i in range(3)]
                          ) \
        .withColumn("date_hour", F.from_unixtime(F.col('terminator_timestamp')/1_000_000, format='yyyy-MM-dd HH:mm:ss')) \
        .orderBy(F.col("terminator_timestamp").asc())
    return df_radios

df_radios.show()