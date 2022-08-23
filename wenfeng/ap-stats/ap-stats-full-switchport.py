from pyspark.sql.functions import udf, size, avg, count, col,sum, explode

import pyspark.sql.functions as F

import json
from datetime import datetime,timedelta
env = "production"

now = datetime.now() - timedelta(hours=3)
date_day = now.strftime("%Y-%m-%d")
date_hour = now.strftime("%H")
date_day = "2022-03-21"
date_hour = "20"

s3_bucket = "s3://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)
df= spark.read.parquet(s3_bucket)
df.printSchema()


df_switchports = df.select("org_id", "site_id", "id", "firmware_version" , "model", "total_client_count", "active_client_count",
                           "cloud_last_rtt",
                           F.explode("switchports").alias("switchports"))
df_switchports_1 = df_switchports.select("org_id", "site_id", "id", "firmware_version", "model", "total_client_count", "active_client_count",
                                         "cloud_last_rtt", F.col("switchports.*"))


site='d4e9f17b-fc8e-4102-9deb-842fe99421b8'
df_switchports_site = df_switchports_1.filter(F.col("site_id")==site)
df_switchports_site.select("cloud_last_rtt").summary().show()

df_switchports_2 = df_switchports_1.withColumn("high_tx_bytes", F.col("tx_bytes")>10_000_000_000)\
    .withColumn("high_rx_bytes", F.col("rx_bytes") > 10_000_000_000) \
    .withColumn("high_tx_peakbps", F.col("tx_bytes") > 10_000_000_000) \
    .withColumn("high_rx_peakbps", F.col("rx_bytes") > 10_000_000_000) \
    .select("firmware_version", "model", "high_tx_bytes",  "high_rx_bytes", "high_tx_peakbps", "high_rx_peakbps")

df_switchports_3=df_switchports_2.select( "model", "high_tx_bytes",  "high_rx_bytes", "high_tx_peakbps", "high_rx_peakbps")\
    .groupBy( "model", "high_tx_bytes",  "high_rx_bytes", "high_tx_peakbps", "high_rx_peakbps").count()
df_switchports_3.show()


# University of Lynchburg
Site1 = "bbbbf5e0-72b3-4a66-b60c-4c5089be12ea"  # ("Schewel")
Site2 = "f3af5560-72ee-4941-a2cc-86b59143501e"  # ("Thompson")
Site3 = "69dc6756-6e3f-4d13-91b2-9d3bb1d077ea"  # ("Dillard")
sites = [Site1, Site2, Site3]

# site_KNH = "d7b17b58-bf13-4f73-a664-05f482dc51ea"   # KNH TP
# df_site = df.filter(F.col("site_id")==Site)
df_site = df.filter(df.site_id.isin(sites))
# df_site.show()

df_switchports = df_site.select("org_id", "site_id", "id", "firmware_version" , "model", "total_client_count", "active_client_count",  F.explode("switchports").alias("switchports"))
df_switchports_1 = df_switchports.select("org_id", "site_id", "id", "firmware_version", "model", "total_client_count", "active_client_count", F.col("switchports.*"))

# df_radio_1.show()
df_switchports_1.printSchema()
df_switchports_1.orderBy("model", "name").show(200)

df_switchports_1.select("total_client_count", "active_client_count", "rx_peakbps", "tx_peakbps", "tx_bytes", "rx_bytes", "switchport_counter_delta" ).summary().show()


filter_query = 'link and tx_peakpbs < 1_000_000_000 and rx_peakpbs < 1_000_000_000'
df_switchports_filter = df_switchports_1.filter(filter_query)
df_switchports_filter.select("total_client_count", "active_client_count", "rx_peakbps", "tx_peakbps", "tx_bytes", "rx_bytes", "switchport_counter_delta" ).summary().show()

s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-stats-switchport/dt={dt}/*".format(fs=fs, dt=date_day)
print(s3_path)
df_switchports_1.write.save(s3_path, format='parquet', mode='overwrite', header=True)
#
#
df_switchports_11 = spark.read.parquet(s3_path)

filter_query = 'link and tx_peakbps < 1000000000 and rx_peakbps < 1000000000'
df_switchports_filter = df_switchports_11.filter(filter_query)
df_switchports_filter.select("total_client_count", "active_client_count", "rx_peakbps", "tx_peakbps", "tx_bytes", "rx_bytes", "switchport_counter_delta" ).summary().show()


