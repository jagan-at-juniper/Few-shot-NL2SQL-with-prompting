from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('UpdateOSA') \
    .getOrCreate() 
#
from pyspark.sql.functions import udf, size, avg, count, col,sum, explode
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from functools import reduce
import json
from datetime import datetime,timedelta

# explicit functions
def union(*dfs):
    return reduce(DataFrame.union, dfs)

env = "production"
#env = "staging"
# 

past_day_timestamp = datetime.now() - timedelta(2) # previous day
past_date_YMD  = past_day_timestamp.strftime("%Y-%m-%d")
date_YMD = past_date_YMD
osp_schema = StructType([
    StructField('org_id', StringType(), True),
    StructField('site_id', StringType(), True),
    StructField('service_provider_asn', StringType(), True)
  ])

try:
    osp_previous = spark.read.parquet('s3://mist-data-science-dev/rjagannathan/orgid_siteid_asn.parquet')
except:
    osp_previous = spark.createDataFrame(data=[], schema=osp_schema)
    
osp_unique = spark.createDataFrame(data=[], schema=osp_schema)
s3path = \
    "s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt=" + \
    str(date_YMD) + "/hr=*/*.parquet"
s3data=spark.read.parquet(s3path)
s3data.createOrReplaceTempView("s3data")
this_osp = spark.sql("SELECT org_id, site_id, service_provider_asn FROM s3data WHERE service_provider_asn != '' ")
this_osp_unique = this_osp.distinct()
osp = union(*[osp_previous, this_osp_unique])
osp_unique = osp.distinct()

osp_unique.write.format("parquet")\
                .mode("overwrite")\
                .save("s3://mist-data-science-dev/rjagannathan/orgid_siteid_asn.parquet")

osp_count = osp_unique.count()

print( "Updated s3://mist-data-science-dev/rjagannathan/orgid_siteid_asn.parquet with previous day's data " + "(" + str(osp_count) + " entries)")
