

import pyspark.sql.functions as F
from datetime import datetime,timedelta
env = "production"

# now = datetime.now() - timedelta(hours=3)
# date_day = now.strftime("%Y-%m-%d")
# date_hour = now.strftime("%H")
date_day = "2022-03-31"
date_hour = "20"

s3_bucket = "s3://mist-secorapp-{env}/client-stats-analytics/client-stats-analytics-{env}/".format(env=env)
s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
print(s3_bucket)
df= spark.read.parquet(s3_bucket)
df.printSchema()

df.select(F.length("client_wcid").alias("client_wcid_length"), df.client_wcid.isNull().alias("client_wcid_null"))\
    .groupBy("client_wcid_length", "client_wcid_null").count().show()
