#!/usr/bin/env python
# coding: utf-8

# # start with graphframe 
# 
# $ find venv/ -name *graphx*.jar
#   venv//spark-2.4.4-bin-without-hadoop/jars/spark-graphx_2.11-2.4.4.jar
# 
# $ ./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
# 
# 
# 
# ## data sources:
# * ap-last-seen
# * scan 
# * coverage-anomaly
# * sticky-clients
# 
# 


import pyspark.sql.functions as F
# from pyspark.sql.functions import udf, size, avg, count, col, sum, explode
import json
from pyspark.sql.types import StringType, FloatType
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *


spark.conf.set("spark.sql.session.timeZone", "PST")

def mac_format(mac):
    return mac.replace("-", "")
mac_format = F.udf(mac_format, StringType())


# spark.conf.set("spark.sql.session.timeZone", "PST")

env = "production"
# env = "staging"
s3_bucket = "s3://mist-aggregated-stats-{env}/aggregated-stats/".format(env=env)
date_day = "2020-08-26"
hr = '*'


#
# ap-last-seen,  all wireless- APs.   #AP per site,  ap-model
#
def get_ap_last_seen(spark, s3_bucket):
    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    ff = {'org_id': StringType,
          'site_id': StringType,
          'ap_id': StringType,
          'model': StringType,
          'firmware_version': StringType,
          'terminator_timestamp': LongType}
    fields = [StructField(k, v()) for k,v in ff.items()]
    schema = StructType(fields)

    df_ap = spark.createDataFrame(ap_rdd, schema)

    # filter models:
    #    AP21 no scan data
    #    AP41E/AP43E, out-door AP.  Complicated  which need
    skip_models = ["AP43E-US", "AP41E-US", "AP21-US"]
    df_ap = df_ap.filter(~df_ap.model.isin(skip_models))

    # filter ap last seen 1 day ago
    current_timestamp = time.time() * 1000
    df_ap = df_ap.filter( F.col("terminator_timestamp") < current_timestamp-86400).count()

    return df_ap

s3_ap_last_seen_bucket = 's3n://mist-aggregated-stats-{env}/event_generator/ap_last_seen/*'.format(env=env)
print(s3_ap_last_seen_bucket)
df_ap = get_ap_last_seen(spark, s3_ap_last_seen_bucket)


#
#  dataframe for coverage anomaly
#
import numpy as np
def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def coverage_score(nclients, sle_coverage, sle_coverage_anomaly_dev, coverage_anomaly_count):
    # score = 0.0
    score = (1.0 - sle_coverage) * \
            sigmoid(nclients-1.0) * \
            sigmoid(sle_coverage_anomaly_dev -2.0) * \
            sigmoid(coverage_anomaly_count - 2)
    return score
coverage_score = F.udf(coverage_score, FloatType())

def load_coverage_anomaly(spark, s3_bucket):
    rdd = spark.sparkContext.sequenceFile(s3_coverage_bucket).map(lambda x: json.loads(x[1]))
    df_coverage = rdd.filter(lambda x: x['event_type'] == "sle_coverage_anomaly")\
        .map(lambda x: x.get("source"))\
        .toDF()

    # groupBy AP
    df_coverage = df_coverage.filter(F.col("anomaly_type")!="aysmmetry_downlink")\
                            .select("ap", "avg_nclients", "sle_coverage",
                                     "sle_coverage_anomaly_score")\
                            .groupBy("ap")\
                            .agg(F.avg("avg_nclients").alias("avg_nclients"),
                                    F.avg("sle_coverage").alias("sle_coverage"),
                                    F.avg("sle_coverage_anomaly_score").alias("sle_coverage_anomaly_dev"),
                                    F.count("*").alias("coverage_anomaly_count")
                                 )\
                            .withColumn("ap", mac_format(F.col("ap")))\
                            .withColumnRenamed("ap", "cov_ap")

    # reduce coverage events with low
    filter_query= "avg_nclients > 2.0 and sle_coverage < 0.50 and coverage_anomaly_count>3.0"
    df_coverage = df_coverage.filter(filter_query)

    # get coverage_anomaly_score
    df_coverage = df_coverage.withColumn("coverage_anomaly_score",  coverage_score(F.col("avg_nclients"),
                                                                                   F.col("sle_coverage"),
                                                                                   F.col("sle_coverage_anomaly_dev"),
                                                                                   F.col("coverage_anomaly_count"))
                                         )

    # df_coverage_filter.count()
    return df_coverage

s3_coverage_bucket = 's3n://mist-secorapp-{env}/ap-events/ap-events-{env}/dt='.format(env=env) + date_day + '/*'
df_coverage = load_coverage_anomaly(spark, s3_coverage_bucket)
df_coverage.show()

#
# sticky client  (from client-events)?
#
def load_sticky_client(spark, s3_bucket):
    rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    df_sticky = rdd.filter(lambda x: x['event_type'] == "sticky-client") \
        .map(lambda x: x.get("source")) \
        .toDF()

    # rdd_sticky = spark.sparkContext.sequenceFile(s3_sticky_path)
    # df_sticky = rdd_sticky.map(lambda x: json.loads(x[1])).toDF()  # .map(lambda x: json.loads(x[1])).
    df_sticky = df_sticky.select(F.col("Assoc.OrgID").alias("org_id"),
                                 F.col("Assoc.SiteID").alias("site_id"),
                                 F.col("Assoc.BSSID").alias("bssid"),
                                 F.col("Assoc.WLAN").alias("wlan"),
                                 F.col("Assoc.BAND").alias("band"),
                                 F.col("Assoc.AP").alias("ap_sticky"),
                                 F.col("Assoc.SSID").alias("ssid"),
                                 "WC",
                                 "Sticky", "When", "version"
                                 )
    #
    df_sticky_ap = df_sticky.select("org_id", "site_id", "ap_sticky").filter("Sticky")\
        .groupBy("org_id", "site_id", "ap_sticky")\
        .agg(F.count("*").alias("sticky_count")) \
        .withColumn("ap_sticky", mac_format(F.col("ap_sticky")))

    return df_sticky_ap

s3_sticky_bucket = 's3n://mist-secorapp-{env}/client-events/client-events-{env}/dt={day}/*'.format(env=env, day= date_day)
# s3_sticky_bucket = "s3://mist-secorapp-{env}/sticky-client/sticky-client-{env}/dt={day}/hr={hr}/*.seq".format(env=env,
#                                                                                                               day=date_day, hr=hr)
print(s3_sticky_bucket)
df_sticky_ap = load_sticky_client(spark, s3_sticky_bucket)
df_sticky_ap.show()


# joining (ap_last_seen,  coverage_anomaly  and sticky_client) for Ap-node properties
df_ap_coverage = df_ap.join(df_coverage, [df_ap.ap_id == df_coverage.cov_ap], how='left')

df_ap_coverage_sticky = df_ap_coverage.join(df_sticky_ap.select("ap_sticky", "sticky_count"),
                                            [df_ap_coverage.ap_id == df_sticky_ap.ap_sticky],
                                            how='left')
df_ap_coverage_sticky.printSchema()
df_ap_coverage_sticky.show(2)





#  Using scan_data for ap-neighbor,  edges
#
hr = "23"
s3_bucket = "s3://mist-aggregated-stats-{env}/aggregated-stats/".format(env=env)
s3_ap_neighbors_path = s3_bucket + "top_1_time_epoch_by_site_ap_ap2_band/dt={day}/hr={hr}/*.csv".format(env=env, day=date_day, hr=hr)
print(s3_ap_neighbors_path)

#
def load_ap_scan_data(spark, s3_bucket):
    df_edges = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df_edges.createOrReplaceTempView("scan_data")
    df_edges = df_edges.withColumnRenamed("ap", "ap1")
    return df_edges

df_edges = load_ap_scan_data(spark, s3_ap_neighbors_path)
df_edges.count()

#
# join df_coverage_sticky with edge from df_edges
#
def join_df(df_ap_coverage_sticky, df_edges):
    df_joined_1 = df_ap_coverage_sticky.join(df_edges.select("ap1", "ap2", "rssi"),
                                        [df_ap_coverage_sticky.ap_id == df_edges.ap1],
                                        how="left")
    # df_joined_1.printSchema()
    # df_joined_1.show(2)

    df_ap_coverage_sticky_ap2 = df_ap_coverage_sticky.withColumnRenamed("ap_id", "ap_2") \
        .select("ap_2", F.col("coverage_anomaly_score").alias("coverage_anomaly_score_2")
                )

    df_joined = df_joined_1.join(df_ap_coverage_sticky_ap2,  [df_joined_1.ap2 == df_ap_coverage_sticky_ap2.ap_2], how="left")


    df_final = df_joined.select("org_id", "site_id", "ap_id", "model",
                                "avg_nclients", "sle_coverage", "coverage_anomaly_score", "coverage_anomaly_count",
                                "sticky_count",
                                "ap2", "rssi", "coverage_anomaly_score_2")


    return df_final

df_final = join_df(df_ap_coverage_sticky, df_edges)
df_final.show(2)

# final_stats


s3_out_bucket = "s3://mist-test-bucket/wenfeng/df-joined-new/"
df_final.write.parquet(s3_out_bucket)
df_final_new = spark.read.parquet(s3_out_bucket)


df_joined_g = df_final.select("org_id", "site_id", "ap_id", "coverage_anomaly_score", "ap2", "coverage_anomaly_score_2")\
        .groupBy("org_id", "site_id", "ap_id")\
        .agg( F.avg("coverage_anomaly_score").alias("coverage_anomaly_score"),
              F.max("sticky_count").alias("max_sticky_count"),
              F.countDistinct("ap2").alias("strong_neighbors"),
              F.max("coverage_anomaly_score_2").alias("neighbor_anomaly")
            )

df_joined_g.printSchema()
df_joined_g.show(2)

# TODO:  Testing purpose
def ap_coverage_score(sle_coverage_anomaly_score, strong_neighbors=0, neighbor_anomaly=None, tx_rx_utl= 1.0):
    score = 0.0
    if sle_coverage_anomaly_score and sle_coverage_anomaly_score>3:
        score = 0.5
    if strong_neighbors and strong_neighbors < 1:
        score += 0.2
    if neighbor_anomaly and neighbor_anomaly > 0:
        score += 0.3
    score = score * tx_rx_utl
    return score

ap_coverage_score = F.udf(ap_coverage_score, FloatType())
df_joined_g = df_joined_g.withColumn("ap_coverage_score", ap_coverage_score(F.col("coverage_anomaly_score"),
                                                                            F.col("strong_neighbors"),
                                                                            F.col("neighbor_anomaly"))
                                     )


df_joined_g.select("ap_coverage_score").describe().show()



# TODO:
# site_id = "5e8fe474-a9ee-4d01-a2b6-b022b0f9c869"  # GEG1 , AmazonOTFC-prod
site_id = "a7092875-257f-43f3-9514-ca1ab688bec0"  # Sam's club. 4989
# site_id = "d1ee1d22-4b55-4c97-97c4-9d757144f45b"



