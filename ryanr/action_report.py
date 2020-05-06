from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import json
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.functions import explode
from datetime import datetime, timedelta
import sys

date_string = sys.argv[0]
print (f"Running the action report for {date_string}")

# d = datetime.today() - timedelta(days=1)
# date_string = d.strftime('%Y-%m-%d')

def validate(date_string):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

conf = SparkConf().setAppName(f"Automated Action Entity Data Joins For {date_string}")

rdd_action_data = sc.sequenceFile(f"s3://mist-aggregated-stats-production/entity_action/entity_action-production/dt={date_string}/hr=*/AutoAction_*.seq") #1580773985, 1580776189
# rdd_action_data.map(lambda r: json.loads(r[1])['action'])

df_action_data = rdd_action_data.map(lambda r: json.loads(r[1]))\
.map(lambda r:r[0]).filter(lambda r:r['action']!= None)\
.filter(lambda r:r['action']!= "do_nothing")\
.toDF()

# 'action', 'ap_id', 'org_name', 'event_name', 'event_type'
df_action_data_cols = df_action_data.select(df_action_data['action'],
											df_action_data['display_entity_id'].alias('ap_id'), ### app_id
											df_action_data['org_id'], #--> org_name with org_id_mapping table
											explode(df_action_data['event_id']).alias('event_id'))
# df_action_data['event_name']
### Code below is joining the orginzation name
df_org_id_name_map  = spark.read.load("s3://mist-secorapp-production/dimension/org/part-*.parquet").withColumnRenamed("name","org_name")

df_action_w_org = df_action_data_cols.join(df_org_id_name_map, df_action_data_cols.org_id == df_org_id_name_map.id, 'inner')\
.drop('created_time','modified_time','msp_id','tzoffset','secret','loaded_date', 'id', 'org_id')

df_action_w_org.show()

df_action_w_org.coalesce(1)\
  .write\
  .option("header","true")\
  .option("sep",",")\
  .mode("overwrite")\
  .csv(f"s3://mist-data-science-dev/automated_action_report/{date_string}/")