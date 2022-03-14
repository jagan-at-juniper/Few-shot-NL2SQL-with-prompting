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
dt = "2022-03-11"
hr = "22"

def fetch_sticky_roaming_event(dt, hr, env):
    s3_gs_bucket = 's3://mist-secorapp-{env}/cv-sle-roaming-multipartition-v2/cv-sle-roaming-multipartition-v2-{env}'.format(env=env)
    s3_gs_bucket += '/dt={dt}/hr={hr}/*.seq'.format(dt=dt, hr=hr)
    df_roam = sc.sequenceFile(s3_gs_bucket).map(lambda x: json.loads(x[1])).toDF()

    # s3_bucket_files = []
    # date_now = datetime.now() - timedelta(hours=1)
    # last_hours = 1
    # for i in range(last_hours):
    #     dt = (date_now - timedelta(days=i)).strftime("%Y-%m-%d")
    #     hr = date_now.strftime("%H")
    #     s3_gs_bucket = '{fs}://mist-secorapp-{env}/cv-sle-roaming-multipartition-v2/cv-sle-roaming-multipartition-v2-{env}'.format(fs=fs, env=env)
    #     s3_gs_bucket += '/dt={dt}/hr={hr}/*.seq'.format(dt=dt, hr=hr)
    #
    #     # s3_bucket ='{fs}://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats_parquet/' \
    #     #            f'dt={date_day}/hr=*/last_1_day/*.parquet'.format(fs=fs, date_day=date_day)
    #     s3_bucket_files.append(s3_gs_bucket)
    # print(s3_bucket_files)
    # df = sc.sequenceFile(*s3_gs_bucket).map(lambda x: json.loads(x[1])).toDF()
    df_roam.printSchema()
    return df_roam

def fetch_client_stats(dt, hr, env):
    s3_gs_bucket = 's3://mist-secorapp-{env}/client-stats-analytics/client-stats-analytics-{env}/'.format(env=env)
    s3_gs_bucket += '/dt={dt}/hr={hr}/*.parquet'.format(dt=dt, hr=hr)

    print(s3_gs_bucket)
    df_client = spark.read.parquet(s3_gs_bucket)
    return df_client


df_roam = fetch_sticky_roaming_event(dt, hr, env)
df_classifiers = df_roam.select("classifier", "metric").groupBy("classifier", "metric").count()
df_classifiers.show()
df_sticky = df_roam.filter('classifier == "signal-sticky-client"')
df_sticky.show()

#
df_long_sticky_client = df_sticky.select("org_id", "site_id", "band", "client_wcid", "ap_mac", "band") \
    .groupBy("org_id", "site_id", "band", "client_wcid", "ap_mac", "band") \
    .count() \
    .orderBy(F.col("count").desc())

df_sticky_client_counts = df_long_sticky_client.withColumn("client_long_sticky", F.when(F.col("count")>1, 1).otherwise(0))

df_site_sticky = df_sticky_client_counts.select("org_id", "site_id", "band", "client_long_sticky", "count") \
    .groupBy("org_id", "site_id", "band") \
    .agg(F.sum("client_long_sticky").alias("long_sticky_clients"), F.sum("count").alias("total_sticky_events")) \
    .orderBy(F.col("sticky_clients").desc())

df_site_sticky.show(truncate=False)

site_id = "fdf1b743-a8cb-4227-a015-b3d587eb7caf" # Site[Main Campus] under Org[Massachusetts Institute of Technology]
client_wcid = 'b9016e41-ef2c-ef3d-e960-bc258bf9c6d3'


site_id = "fdf1b743-a8cb-4227-a015-b3d587eb7caf" # Site[Main Campus] under Org[Massachusetts Institute of Technology]

df_sticky_site = df_sticky_client_counts.filter(F.col("site_id")==site_id)
sticky_clients = df_sticky_site.filter(F.col("client_long_sticky")==True).select( "client_wcid").take(10)
sticky_clients_list = [x.client_wcid for x in sticky_clients]
sticky_clients_list


#client-stats
df_client= fetch_client_stats(dt, hr, env)
df_client.printSchema()

df_site_client_sticky = df_client.filter(F.col("site_id")==site_id) \
    .withColumn("date_time",F.from_unixtime(F.col("terminator_timestamp")/1_000_000)) \
    .filter(df_client.client_wcid.isin(sticky_clients_list))
# save intermediate data
s3_path = "{fs}://mist-data-science-dev/wenfeng/stick_client/dt={dt}/".format(fs=fs, dt=dt)
print(s3_path)
df_site_client_sticky.write.save(s3_path, format='parquet', mode='overwrite', header=True)

site_id = "fdf1b743-a8cb-4227-a015-b3d587eb7caf" # Site[Main Campus] under Org[Massachusetts Institute of Technology]

df_sticky_site = df_sticky_client_counts.filter(F.col("site_id")==site_id)


#
# client_wcid  = sticky_clients_list[0]

site_id = "fdf1b743-a8cb-4227-a015-b3d587eb7caf" # Site[Main Campus] under Org[Massachusetts Institute of Technology]
client_wcid = 'b9016e41-ef2c-ef3d-e960-bc258bf9c6d3'


df_client_sticky = df_client.filter(F.col("client_wcid")==client_wcid) \
    .withColumn("date_time",F.from_unixtime(F.col("terminator_timestamp")/1_000_000)) \
    .orderBy(F.col("terminator_timestamp").desc())

df_client_sticky.select('client_mac', "terminator_timestamp", "date_time", "ap_id", "client_band", "client_radio_index",  "client_rssi").show()

# save intermediate data
s3_path = "{fs}://mist-data-science-dev/wenfeng/stick_client/dt={dt}/".format(fs=fs, dt=write_date_day)
print(s3_path)
df_client_sticky.write.save(s3_path, format='parquet', mode='overwrite', header=True)

df_client_sticky = df_client_sticky.orderBy(F.col("terminator_timestamp").desc())

import matplotlib.pyplot as plt
%matplotlib inline


df_client_sticky.plot(x="date_time", y=['client_rssi', 'ap_id', 'client_band', 'client_connected_time_sec'], figsize=(10, 15), subplots=True)
plt.xticks(rotation=90)


