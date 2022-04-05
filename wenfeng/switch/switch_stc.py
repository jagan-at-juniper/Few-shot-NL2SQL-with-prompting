
from pyspark.sql.functions import lit, udf, size, avg, min as min_, max as max_, sum as sum_, count, countDistinct, col, mean, stddev, struct, explode, explode_outer, unix_timestamp, sum as sum_

env = "production"
DATE = '2020-01-2[78]/'
file_path = 's3://mist-secorapp-{env}/oc-stats-analytics/oc-stats-analytics-{env}/dt={date}/*'.format(env=env, date=DATE)
df = spark.read.parquet(file_path)
df.printSchema()


# to pandas
import pandas as pd

# df1_i = df1.select(col("id"), col("when"), col("org_id"), col("site_id"), explode(col("interfaces")).alias("interface")).select('id','when', 'org_id', 'site_id', 'interface.*')
# df1_i.take(1)

df1_i = df.select(col("id"), col("when"), col("org_id"), col("site_id"), explode(col("clients")).alias("client")).select('id','when', 'org_id', 'site_id', 'client.*')
df_pd = df1_i.toPandas()

