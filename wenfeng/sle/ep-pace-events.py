from pyspark.sql import functions as F
import json
import os

spark = SparkSession \
    .builder \
    .appName("sticky-events") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

myhost = os.uname()[1]
env = "production"
provider = "AWS" if "dataproc" not in myhost else "GCP"
fs = "s3" if provider == "AWS" else "gs"



env = "production"
dt = "2022-03-08"
hr = "*"

s3_gs_bucket = 's3://mist-secorapp-{env}/ep-pace-event/ep-pace-event-{env}'.format(env=env)
s3_gs_bucket += '/dt={dt}/hr={hr}/*.seq'.format(dt=dt, hr=hr)
rdd = sc.sequenceFile(s3_gs_bucket).map(lambda x: json.loads(x[1])).filter(lambda x: x.get("SiteID")==site_id)

df_pace = sc.sequenceFile(s3_gs_bucket).map(lambda x: json.loads(x[1])).filter(lambda x: x.get("SiteID")==site_id).toDF()


site_id = "9291ba26-6e1e-11e5-9cdd-02e208b2d3"

df_pace_site = df_pace.filter.filter(F.col("SiteID")==site_id)
df_pace_site.count()

s3_path = "{fs}://mist-data-science-dev/wenfeng/ep-pace/dt={dt}/".format(fs=fs, dt=dt)
print(s3_path)
df_pace_site.write.save(s3_path, format='parquet', mode='overwrite', header=True)
