from pyspark.sql import functions as F
import json
import os
from datetime import datetime,timedelta

spark = SparkSession \
    .builder \
    .appName("sticky-events") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

myhost = os.uname()[1]

env = "production"
provider = "AWS" if "dataproc" not in myhost else "GCP"
fs = "s3" if provider == "AWS" else "gs"


spark = SparkSession \
    .builder \
    .appName("ep-pace") \
    .getOrCreate()

spark.sparkContext.setLogLevel("warn")


detect_time = datetime.now() - timedelta(hours=1)
date_day = detect_time.strftime("%Y-%m-%d")
date_hour = detect_time.strftime("%H")

date_day = "2022-09-14"
date_hour = "*"

# dt = "2022-09-09"
# hr = "*"

s3_gs_bucket = 's3://mist-secorapp-{env}/ep-pace-event/ep-pace-event-{env}'.format(env=env)
s3_gs_bucket += '/dt={dt}/hr={hr}/*.seq'.format(dt=date_day, hr=date_hour)


site_id = '796af540-75ec-4392-9c29-6f1cbbf919b6'   # FAIRFAX COUNTY PUBLIC SCHOOLS
# site_id = "9291ba26-6e1e-11e5-9cdd-02e208b2d3"

rdd = sc.sequenceFile(s3_gs_bucket).map(lambda x: json.loads(x[1])).filter(lambda x: x.get("SiteID")==site_id)
df_pace = rdd.toDF()

rdd_site = sc.sequenceFile(s3_gs_bucket).map(lambda x: json.loads(x[1])).filter(lambda x: x.get("SiteID")==site_id)
df_pace_site = rdd_site.toDF()
print(df_pace_site.count())

# df_pace_site = df_pace.filter.filter(F.col("SiteID")==site_id)
# df_pace_site.count()

s3_path = "{fs}://mist-data-science-dev/wenfeng/ep-pace/dt={dt}/".format(fs=fs, dt=dt)
print(s3_path)
df_pace_site.write.save(s3_path, format='parquet', mode='overwrite', header=True)
