from functools import partial
from datetime import datetime, timedelta
import json
import copy
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import *

"""
Utility function definitions
"""


def date_list(endDate, delta=14):
    temp = [endDate]
    for i in range(1, delta + 1):
        temp.append(endDate - timedelta(days=i))
    return '{' + ','.join([str(d.date()) for d in temp]) + '}'


def readParquet(prefix, dates, hour="{*}", fields=None, toPandas=False):
    path = prefix + "/dt=" + dates + "/hr=" + hour + "/"
    return spark.read.parquet(path)


def readSeq(prefix, date, hour="*", fields=None, toPandas=False):
    path = prefix + "/dt=" + date + "/hr=" + hour + "/"
    temp = sc.sequenceFile(path).values() \
        .map(bytearray.decode).map(json.loads)
    if isinstance(fields, list):
        temp = temp.flatMap(lambda x: Row([x[field] for field in fields]))
    temp = spark.createDataFrame(temp)
    if isinstance(fields, list):
        for idx, field in enumerate(fields):
            temp = temp.withColumnRenamed("_" + str(idx + 1), field)
    _schema = copy.deepcopy(temp.schema)
    if toPandas:
        return temp.toPandas(), _schema
    return temp, _schema


def roundDatetime(timestamp, interval=0):
    tm = datetime.fromtimestamp(timestamp)
    if interval > 0:
        tm = tm - timedelta(minutes=tm.minute % interval, seconds=tm.second)
    return tm


curried_roundDatetime = partial(roundDatetime, interval=0)
udf_roundDate = F.udf(curried_roundDatetime, TimestampType())


@F.pandas_udf("int", F.PandasUDFType.GROUPED_AGG)
def median_udf(v):
    return v.median()


@F.pandas_udf("int", F.PandasUDFType.GROUPED_AGG)
def iqr_udf(v):
    iqr = v.quantile(0.75) - v.quantile(0.25)
    return iqr


"""
Variables
"""
prefix = "s3://mist-aggregated-stats-staging/aggregated-stats/rrm_local_cmd_count/"

filter_commands = ['scan-anomaly', 'capacity-anomaly',
                   'rrm-radar', 'acs-request-ap',
                   'rrm-global', 'marvis-action-check']

# endDate = datetime.strptime("2021-02-08", '%Y-%m-%d')
END_DATE = datetime.today()
LAG = 14
WINDOW_SMOOTH = 5
STD_SMOOTH = 2

"""
Processing
"""
dates = date_list(END_DATE, delta=LAG)
df_stats = readParquet(prefix, dates)
# df_stats = df_stats.withColumn("org_id", regexp_replace(col("org_id"), "[-]", ""))
# df_stats = df_stats.withColumn("site_id", regexp_replace(col("site_id"), "[-]", ""))
df_stats = df_stats.withColumn("date", udf_roundDate(F.col("timestamp"))).drop('timestamp')
df_stats = df_stats.withColumn("time_of_day", F.date_format(F.col("date"), 'HH:mm'))
df_stats = df_stats.withColumn("command-reason", F.concat(F.col("command").cast(StringType()),
                                                          F.lit(" <> "),
                                                          F.col("reason").cast(StringType()))).drop('reason')

df_anomaly_summary_by_tod = df_stats.filter(F.col('command').isin(filter_commands)) \
    .groupBy('time_of_day', 'band', 'command-reason') \
    .agg(median_udf('rrm_local_cmd_count').alias('median_count'),
         iqr_udf('rrm_local_cmd_count').alias('iqr_count'))

df_anomaly_summary_pandas = df_anomaly_summary_by_tod.toPandas()

df_med = df_anomaly_summary_pandas.pivot_table(index=['time_of_day'], columns=['band', 'command-reason'],
                                               values='median_count')
df_iqr = df_anomaly_summary_pandas.pivot_table(index=['time_of_day'], columns=['band', 'command-reason'],
                                               values='iqr_count')

df_upper = df_med + 1.5 * df_iqr
df_lower = df_med - 1.5 * df_iqr

df_upper = df_upper.rolling(WINDOW_SMOOTH, win_type='gaussian', min_periods=1).mean(std=STD_SMOOTH)
df_lower = df_lower.rolling(WINDOW_SMOOTH, win_type='gaussian', min_periods=1).mean(std=STD_SMOOTH)

CLIP_UPPER = 20000.
CLIP_LOWER = 1.
df_upper = df_upper.applymap(lambda x: int(min(CLIP_UPPER, x)))
df_lower = df_lower.applymap(lambda x: int(max(CLIP_LOWER, x)))

# Long-format dataframe with upper and lower bounds
df_upper = pd.melt(df_upper.reset_index(), id_vars=['time_of_day'])
df_lower = pd.melt(df_lower.reset_index(), id_vars=['time_of_day'])
model = df_upper.merge(df_lower, on=['band', 'command-reason', 'time_of_day']).head()

# Serialize model to pkl file
# model.to_pickle("./model_rrm_local_anomaly_all.pkl")
