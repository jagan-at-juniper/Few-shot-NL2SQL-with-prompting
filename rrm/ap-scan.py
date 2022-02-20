from pyspark.sql import functions as F
env = "production"
# env = "staging"


dt = "2022-02-20"
hr = "*"
s3_bucket = "s3://mist-secorapp-{env}/cv-ap-scans-multipartition/cv-ap-scans-multipartition-{env}/dt={dt}/hr={hr}".format(env=env, dt=dt, hr=hr)
print(s3_bucket)

df = spark.read.orc(s3_bucket)
# df.filter(col('ap').isNull() &
df.printSchema()

site_id = '7487264a-2214-42cf-ada7-f19bdb09c059'   # Saurabh Shukla
band = "6"

site_id = "825c05a6-0f21-4e88-b84d-fc8068ad292c"
bannd = "5"

df_site= df.filter("site=='{}'".format(site_id))
df_site.select("ap", "band").groupBy("ap", "band").count().show()

df_site = df_site.withColumn("date_hour", F.date_trunc('hour', F.to_timestamp("time","yyyy-MM-dd HH:mm:ss")))

df_site_max = df_site.select("date_hour", "ap", "ap2", "band",  "channel", "rssi") \
    .groupBy("date_hour", "ap", "ap2", "band", "channel" ) \
    .agg(F.max("rssi").alias("max_rssi"), F.count("ap2").alias("num_rec")
         )
# df_site_max.show(20)
df_site_max.orderBy("date_hour","ap", "ap2").show(100)
