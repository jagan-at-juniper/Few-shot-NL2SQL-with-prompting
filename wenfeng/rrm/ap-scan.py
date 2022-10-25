
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta
import os

env = "production"
provider = os.environ.get("CLOUD_PROVIDER", "aws")
# provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"


detect_time = datetime.now() - timedelta(hours=1)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

app_name = "ap-scan"
date_day = "2022-10-03"
date_hour = "20"

s3_bucket = "{fs}://mist-secorapp-{env}/cv-ap-scans-multipartition/cv-ap-scans-multipartition-{env}/dt={dt}/hr={hr}".format(fs=fs, env=env, dt=date_day, hr=date_hour)
print(s3_bucket)

df = spark.read.orc(s3_bucket)
# df.filter(col('ap').isNull() &
df.printSchema()


#
# # ap = "d420b0834ee5"
# # ap2 = "5c5b353e8ff7"
#
# df_ap = df.filter(F.col("ap")=ap2)

site_id = '7487264a-2214-42cf-ada7-f19bdb09c059'   # Saurabh Shukla
band = "6"

site_id = "825c05a6-0f21-4e88-b84d-fc8068ad292c"
bannd = "5"

site_id = "b4239d56-bf92-4791-bd86-717fb7d62943"

org_id = "56de201d-e63b-4312-9858-40f4cfe35c7f"  #  AMazon
site_id = "948b9b51-b56f-4c9d-87bf-5cc532ff0112"  #

# org_id = "cd5d9339-d5dc-4a8c-a733-c90ff9c8e893"  # Tesla
# site_id = "8cb91905-1843-4574-b7de-d2d0ae353cab" # Tesla AUS07-GA 8cb91905-1843-4574-b7de-d2d0ae353cab

band = "5"


df_site= df.filter("site=='{}'".format(site_id))

df_site_g = df_site.select("ap", "ap2", "band").groupBy("ap", "band").agg(F.countDistinct("ap2").alias("ap2s"))
df_site_g.show()

# count().show()
df_site.select("ap2", "band").groupBy("ap2", "band").count().show()


df_site = df_site.withColumn("date_hour", F.date_trunc('hour', F.to_timestamp("time","yyyy-MM-dd HH:mm:ss")))

df_site_max = df_site.select("date_hour", "ap", "ap2", "band",  "channel", "rssi") \
    .groupBy("date_hour", "ap", "ap2", "band", "channel" ) \
    .agg(F.max("rssi").alias("max_rssi"), F.count("ap2").alias("num_rec")
         )
# df_site_max.show(20)
df_site_max.orderBy("date_hour","ap", "ap2").show(100)
