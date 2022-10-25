from pyspark.sql import functions as F
import os
from datetime import datetime,timedelta

app_name = "ap-capacity-events"

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR");


env = "production"
provider = os.environ.get("CLOUD_PROVIDER", "aws")
# provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"


sc.addPyFile("s3://mist-data-science-dev/wenfeng/spark_jobs_test.zip")
from analytics.event_generator.ap_coverage_event import *

# sc.addPyFile("s3://mist-data-science-dev/spark_jobs/spark_jobs_test.zip")
from analytics.event_generator.ap_coverage_event import *
from analytics.event_generator.ap_capacity_event import *


org_id = "cd5d9339-d5dc-4a8c-a733-c90ff9c8e893"  # Tesla
site_id = "8cb91905-1843-4574-b7de-d2d0ae353cab" # Tesla AUS07-GA 8cb91905-1843-4574-b7de-d2d0ae353cab

org_id = "ea506ec2-06b1-4530-a72d-7906d4d6918b"  # FAIRFAX COUNTY PUBLIC SCHOOLS
site_id = '796af540-75ec-4392-9c29-6f1cbbf919b6'  # Lake Braddock SS
# df_site = df.filter(F.col("site_id")==site_id)


detect_time = datetime.now() - timedelta(hours=4)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

date_day = "2022-09-1*"
date_hour = "*"
# date_hour = "000"
# band= "5"

print(org_id, site_id, date_day, date_hour)

# # check_capacity_anomaly_from_ap_events
print("## check_capacity_anomaly_from_ap_events")

s3_bucket= '{fs}://mist-secorapp-{env}/ap-events/ap-events-{env}/dt={dt}/hr={hr}/'.format(fs=fs, env=env, dt=date_day, hr=date_hour)
df_capacity = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
    filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
    .map(lambda x: x.get("source")) \
    .toDF()

df_capacity.printSchema()

df_capacity = df_capacity.withColumn("date_hour", F.from_unixtime(F.col('timestamp'), format='yyyy-MM-dd HH'))

# df_capacity.select("sle_capacity", "sle_capacity_anomaly_score").summary().show()
# df_capacity.count()
# summary_cols = ["count", "mean", "min", "50%", "75%", "90%", "95%", "99%", "max"]
# df_capacity.filter("band=='24'").select("band", "avg_nclients", "util_ap", "sle_capacity", "sle_capacity_anomaly_score", "error_rate").summary(summary_cols).show()
# df_capacity.filter("band=='5'").select("band", "avg_nclients", "util_ap", "sle_capacity", "sle_capacity_anomaly_score", "error_rate").summary(summary_cols).show()

df_capacity_site = df_capacity.filter(F.col("site")==site_id)
df_capacity_site.groupBy("band").count().show()

df_capacity_site.filter("band=='5'").select("band", "avg_nclients", "util_ap", "sle_capacity", "sle_capacity_anomaly_score", "error_rate").summary().show()
# df_capacity.count()




def save_df_to_fs(df, date_day, date_hour, app_name="ap-capacity", band=""):
    date_hour = "000" if date_hour == "*" else date_hour
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}_{band}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name=app_name, band=band, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    return s3_path


save_df_to_fs(df_capacity_site, date_day, date_hour, app_name, "")