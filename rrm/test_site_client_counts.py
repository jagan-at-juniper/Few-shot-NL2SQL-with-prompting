

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

file_site_timezone = "s3://mist-secorapp-staging/zones-sessions_rewrite/dimension/site/site.txt.gz"



s3_path = "s3://mist-aggregated-stats-production/aggregated-stats/sum_active_client_count_by_site_id/"

s3_path = "s3://mist-data-science-dev/rrm_scheduler_optimization/output/"
s3_path += "dt=2019-10-0*/hr=*/"

df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(s3_path)


site_id1 = "c7b6dc15-3878-4d68-80cf-d354a88b025f"  # STEM high school highland branch
df1 = df.filter(col("site_id")==site_id1)


# Walmart_store_0001= "4d65b639-a039-4382-9a98-b17659be39cc"
df1 = df.filter(col("site_id")==Walmart_store_0001)



s3_path = "s3://mist-data-science-dev/rrm_scheduler_optimization/output/"

s3_path = "s3://mist-data-science-dev/rrm_scheduler_optimization/output/a0936f4e-88a9-44d2-9efa-13a2abc32a78/"
df = sqlContext.read.format('csv').options(header='false', inferSchema='true').load(s3_path)
