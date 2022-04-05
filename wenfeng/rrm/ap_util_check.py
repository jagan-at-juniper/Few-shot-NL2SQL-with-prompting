
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, ArrayType, FloatType, DataType
import pyspark.sql.functions as fn
from pyspark.sql.functions import udf, size, avg, count, col,sum, explode
from operator import itemgetter
import json, datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


DATE = 'dt=2019-11-13'
s3_path = 's3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/' + DATE + '/*'
df = spark.read.parquet(s3_path).filter(col("model").startswith('AP') & col("delta") == True)

ap_id = "5c-5b-35-be-62-ac"
df_ap = df.filter(col("ap_id")==ap_id)

df_radio = df_ap.select("ap_id", "when", explode(col("radios")).alias("radio"))

df_util = df_radio.select("ap_id", "when", "radio.dev", "radio.num_active_clients", "radio.band", "radio.channel",  "radio.utilization_all",  "radio.utilization_rx_other_bss", "radio.utilization_non_wifi", "radio.utilization_unknown_wifi")

# df_util = df_radio.select( "when",   "radio.utilization_rx_other_bss", "radio.utilization_non_wifi", "radio.utilization_unknown_wifi")

df_util.coalesce(100).write.mode('overwrite').parquet('s3://mist-data-science-dev/wenfeng/rrm/'+DATE)

