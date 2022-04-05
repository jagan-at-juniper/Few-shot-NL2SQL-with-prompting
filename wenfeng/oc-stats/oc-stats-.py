


import pyspark.sql.functions as F
import json

from datetime import datetime,timedelta

def test():
    env = "production"
    env = "staging"


    now = datetime.now()  - timedelta(hours=3)
    date_day = now.strftime("%Y-%m-%d")
    date_hour = now.strftime("%H")

    s3_bucket = "s3://mist-secorapp-{env}/oc-stats-analytics/oc-stats-analytics-{env}/".format(env=env)
    s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

    s3_bucket

    df= spark.read.parquet(s3_bucket)
    df.printSchema()

    df.filter(F.size("ssr_peer_path_stats")>0).select("ssr_peer_path_stats").show()
    df.filter(F.size("ssr_peer_path_stats")>0).select("ssr_peer_path_stats").show()


def test():
    env = "production"
    env = "staging"

    now = datetime.now()  - timedelta(hours=3)
    date_day = now.strftime("%Y-%m-%d")
    date_hour = now.strftime("%H")

    s3_bucket = "s3://mist-secorapp-{env}/oc-stats/oc-stats-{env}/".format(env=env)
    s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)

    df= spark.read.parquet(s3_bucket)
    df.printSchema()

    df_s =  df.filter(F.size("ssr_peer_path_stats")>0)
    df_s.select(F.explode("ssr_peer_path_stats")).show(truncate=False)
