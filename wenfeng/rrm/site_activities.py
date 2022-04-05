
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, ArrayType, FloatType, DataType
import pyspark.sql.functions as fn
from pyspark.sql.functions import udf, size, avg, count, col,sum
from operator import itemgetter
import json, datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


DATE = 'dt=2019-11-13'
s3_path = 's3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/' + DATE + '/*'
df = spark.read.parquet(s3_path).filter(col("model").startswith('AP') & col("delta") == True)

df_site = df.groupBy(col("site_id")).count().show

df1 = df.select('site_id', 'active_client_count').groupBy(col('site_id'))
df1.take(10)

df2 = df.groupBy(col("site_id")).agg(sum("active_client_count"))
df2.show()


Walmart_store_0001= "4d65b639-a039-4382-9a98-b17659be39cc"
df1 = df.filter(col("site_id")==Walmart_store_0001)
# df1.first()

radio_clients = udf(lambda x: x[0].num_clients +  x[1].num_clients if len(x) > 1 else 0, IntegerType())

df1_counts = df1 \
    .filter(col("model").startswith('AP') & col("delta")==True) \
    .withColumn('total_clients', radio_clients(col('radios'))) \
    .select('id', 'site_id', 'active_client_count', 'total_clients' )


