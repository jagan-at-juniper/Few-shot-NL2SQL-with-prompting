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
"""
stats_aggregator_job

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    job = stats_aggregator_job(1599696000, 1599699600, "client-events",  spark, "production")
    job.execute()

    job = stats_aggregator_job(1599696000, 1599699600, "ap-events",  spark, "production")
    job.execute()

d_source= job.data_source_inst.load_data()
d = job.data_source_inst.get_data()

d2 =job.data_sources.get("ap_coverage_anomaly_stats")
d2.select('band', 'util_ap').groupBy('band').count().show()
+------------------+-----+
|              band|count|
+------------------+-----+
| 1.267076849937439|    1|
| 8.108983993530273|    3|
|        5.19140625|    2|
|3.1302733421325684|    1|
|3.2381770610809326|   18|
|1.2743749618530273|    1|
| 4.012259006500244|   11|
|3.0227603912353516|    8|
| 3.423802137374878|    3|
|2.6585025787353516|    2|
|1.3258333206176758|    8|
| 6.667239665985107|    1|
|1.8791667222976685|  141|
| 1.787500023841858|  449|
| 5.672500133514404|    5|
| 4.117578029632568|    2|
|1.8712760210037231|    2|
|2.9156250953674316|   28|
|1.1071614821751912|    1|
| 1.905429720878601|   22|
+------------------+-----+
only showing top 20 rows

from analytics.utils.time_util import current_epoch_seconds
from analytics.jobs.utils import *
end_time =  current_epoch_seconds()//86400 * 86400
start_time = end_time - 3600
job= data_enrichment_job("ap_coverage_enrichment",  start_time , end_time, spark=spark, test_env='production', debug_mode=False)

job.execute()
ds = job.data_sources



job = start_debug_job('ap_coverage_detection', 1599696000-1, 1599699600-1, spark=spark, test_env='production', debug_mode=False)
data = run_category_transform(job, 'all')


"""
import json
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark.conf.set("spark.sql.session.timeZone", "PST")


def mac_format(mac):
    """

    :param mac:
    :return:
    """
    return mac.replace("-", "")


mac_format = F.udf(mac_format, StringType())


def get_ap_last_seen(spark, s3_bucket):
    """
    Load Available APs
    :param spark:
    :param s3_bucket:
    :return:
    """
    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    ff = {'org_id': StringType,
          'site_id': StringType,
          'ap_id': StringType,
          'model': StringType,
          'firmware_version': StringType,
          'terminator_timestamp': LongType}
    fields = [StructField(k, v()) for k, v in ff.items()]
    schema = StructType(fields)

    df_ap = spark.createDataFrame(ap_rdd, schema)

    # filter models:
    #    AP21 no scan data
    #    AP41E/AP43E, out-door AP.  Complicated  which need
    skip_models = ["AP43E-US", "AP41E-US", "AP21-US"]
    df_ap = df_ap.filter(~df_ap.model.isin(skip_models))

    # filter ap last seen 1 day ago
    current_timestamp = time.time() * 1000
    df_ap = df_ap.filter(F.col("terminator_timestamp") < current_timestamp - 86400)

    return df_ap


#
#  dataframe for coverage anomaly
#
import numpy as np


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def coverage_score(nclients, sle_coverage, sle_coverage_anomaly_dev, coverage_anomaly_count):
    """
    AP's coverage score, combining sle_coverage, anomalies and connected clients
    :param nclients:
    :param sle_coverage:
    :param sle_coverage_anomaly_dev:
    :param coverage_anomaly_count:
    :return:
    """
    # score = 0.0
    # nclients = nclients or 1.0
    # sle_coverage
    if sle_coverage and nclients and sle_coverage_anomaly_dev and coverage_anomaly_count:
        score = (1.0 - sle_coverage) * \
                sigmoid(nclients - 1.0) * \
                sigmoid(sle_coverage_anomaly_dev - 2.0) * \
                sigmoid(coverage_anomaly_count - 2)
    else:
        score = 0.0
    return float(score)
coverage_score = F.udf(coverage_score, FloatType())


def load_coverage_anomaly(spark, s3_bucket):
    """
    load sle_coverage_anomaly events.
    :param spark:
    :param s3_bucket:
    :return:
    """
    rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    df_coverage = rdd.filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    # groupBy AP
    df_coverage = df_coverage.filter(F.col("anomaly_type") != "aysmmetry_downlink") \
        .filter("avg_nclients > 2.0 and sle_coverage < 0.50")\
        .select("ap", "band",
                "avg_nclients", "util_ap", "sle_coverage", "error_rate", "power",
                "sle_coverage_anomaly_score") \
        .groupBy("ap") \
        .agg(F.avg("avg_nclients").alias("avg_nclients"),
             F.avg("util_ap").alias("util_ap"),
             F.avg("sle_coverage").alias("sle_coverage"),
             F.avg("error_rate").alias("error_rate"),
             F.avg("sle_coverage_anomaly_score").alias("sle_coverage_anomaly_dev"),
             F.count("*").alias("coverage_anomaly_count")
             ) \
        .withColumn("ap", mac_format(F.col("ap"))) \
        .withColumnRenamed("ap", "cov_ap")

    # reduce coverage events with low
    filter_query = "coverage_anomaly_count>3.0"
    df_coverage = df_coverage.filter(filter_query)

    # get coverage_anomaly_score
    df_coverage = df_coverage.withColumn("coverage_anomaly_score", coverage_score(F.col("avg_nclients"),
                                                                                  F.col("sle_coverage"),
                                                                                  F.col("sle_coverage_anomaly_dev"),
                                                                                  F.col("coverage_anomaly_count"))
                                         )

    # df_coverage_filter.count()
    return df_coverage


#
# sticky client  (from client-events)?
#
def load_sticky_client(spark, s3_bucket):
    """

    :param spark:
    :param s3_bucket:
    :return:
    """
    rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    df_sticky = rdd.filter(lambda x: x['event_type'] == "sticky-client") \
        .map(lambda x: x.get("source")) \
        .toDF()

    # rdd_sticky = spark.sparkContext.sequenceFile(s3_sticky_path)
    # df_sticky = rdd_sticky.map(lambda x: json.loads(x[1])).toDF()  # .map(lambda x: json.loads(x[1])).
    df_sticky = df_sticky.filter(F.col("Sticky") == True)\
                         .select(F.col("Assoc.OrgID").alias("org_id"),
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
    df_sticky_ap = df_sticky.select("org_id", "site_id", "ap_sticky")\
        .filter("Sticky") \
        .groupBy("org_id", "site_id", "ap_sticky") \
        .agg(F.count("*").alias("sticky_count")) \
        .withColumn("ap_sticky", mac_format(F.col("ap_sticky")))

    return df_sticky_ap


def ap_properties_enrichment(df_ap, df_coverage, df_sticky_ap):
    # joining (ap_last_seen,  coverage_anomaly  and sticky_client) for Ap-node properties
    df_ap_coverage = df_ap.join(df_coverage, [df_ap.ap_id == df_coverage.cov_ap], how='left')

    df_ap_coverage_sticky = df_ap_coverage.join(df_sticky_ap.select("ap_sticky", "sticky_count"),
                                                [df_ap_coverage.ap_id == df_sticky_ap.ap_sticky],
                                                how='left')

    return df_ap_coverage_sticky


#
def load_ap_scan_data(spark, s3_bucket):
    """

    :param spark:
    :param s3_bucket:
    :return:
    """
    df_edges = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df_edges.createOrReplaceTempView("scan_data")
    df_edges = df_edges.withColumnRenamed("ap", "ap1")
    return df_edges


#
# join df_coverage_sticky with edge from df_edges
#
def join_nodes_edges_dfs(df_ap_coverage_sticky, df_edges):
    """

    :param df_ap_coverage_sticky:
    :param df_edges:
    :return:
    """
    df_joined_1 = df_ap_coverage_sticky.join(df_edges.select("ap1", "ap2", "rssi"),
                                             [df_ap_coverage_sticky.ap_id == df_edges.ap1],
                                             how="left")
    # df_joined_1.printSchema()
    # df_joined_1.show(2)

    df_ap_coverage_sticky_ap2 = df_ap_coverage_sticky.withColumnRenamed("ap_id", "ap_2") \
        .select("ap_2", F.col("coverage_anomaly_score").alias("coverage_anomaly_score_2")
                )

    df_joined = df_joined_1.join(df_ap_coverage_sticky_ap2, [df_joined_1.ap2 == df_ap_coverage_sticky_ap2.ap_2],
                                 how="left")

    df_final = df_joined.select("org_id", "site_id", "ap_id", "model",
                                "avg_nclients", "sle_coverage", "coverage_anomaly_score", "coverage_anomaly_count",
                                "sticky_count",
                                "ap2", "rssi", "coverage_anomaly_score_2")

    return df_final


def ap_coverage_severity_score(sle_coverage_anomaly_score, strong_neighbors=0, neighbor_anomaly=None, tx_rx_utl=1.0):
    """
    TODO: to be improved, use the simplied formula for testing.
     Coverage Severity score, combining neighbor's impacting.
    :param sle_coverage_anomaly_score:
    :param strong_neighbors:
    :param neighbor_anomaly:
    :param tx_rx_utl:
    :return:
    """

    score = 0.7 * sle_coverage_anomaly_score

    if neighbor_anomaly and neighbor_anomaly > 0:
        score += 0.3 * neighbor_anomaly
    else:
        if strong_neighbors and strong_neighbors < 1:
            score += 0.2
        else:
            score -= np.log(strong_neighbors)

    score = score * tx_rx_utl
    return float(score)

ap_coverage_severity_score = F.udf(ap_coverage_severity_score, FloatType())


def generate_events(df_final):
    df_joined_g = df_final.select("org_id", "site_id", "ap_id", "coverage_anomaly_score", "ap2",
                                  "coverage_anomaly_score_2") \
        .groupBy("org_id", "site_id", "ap_id") \
        .agg(F.avg("coverage_anomaly_score").alias("coverage_anomaly_score"),
             F.max("sticky_count").alias("max_sticky_count"),
             F.countDistinct("ap2").alias("strong_neighbors"),
             F.max("coverage_anomaly_score_2").alias("neighbor_anomaly")
             )

    # df_joined_g.printSchema()
    # df_joined_g.show(2)
    df_joined_g = df_joined_g.withColumn("ap_coverage_score",
                                         ap_coverage_severity_score(F.col("coverage_anomaly_score"),
                                                                    F.col("strong_neighbors"),
                                                                    F.col("neighbor_anomaly"))
                                         )

    return df_joined_g


def test():

    env = "production"
    # env = "staging"
    # s3_bucket = "s3://mist-aggregated-stats-{env}/aggregated-stats/".format(env=env)
    date_day = "2020-10-15"
    hr = '*'

    s3_ap_last_seen_bucket = 's3n://mist-aggregated-stats-{env}/event_generator/ap_last_seen/*'.format(env=env)
    print(s3_ap_last_seen_bucket)
    df_ap = get_ap_last_seen(spark, s3_ap_last_seen_bucket)

    s3_coverage_bucket = 's3n://mist-secorapp-{env}/ap-events/ap-events-{env}/dt='.format(env=env) + date_day + '/*'
    df_coverage = load_coverage_anomaly(spark, s3_coverage_bucket)
    df_coverage.show()

    # s3_sticky_bucket = "s3://mist-secorapp-{env}/sticky-client/sticky-client-{env}/dt={day}/hr={hr}/*.seq".format(env=env,
    # day=date_day, hr=hr)
    s3_sticky_bucket = 's3n://mist-secorapp-{env}/client-events/client-events-{env}/dt={day}/*'.format(env=env,
                                                                                                       day=date_day)
    print(s3_sticky_bucket)
    df_sticky_ap = load_sticky_client(spark, s3_sticky_bucket)
    df_sticky_ap.show()

    df_ap_coverage_sticky = ap_properties_enrichment(df_ap, df_coverage, df_sticky_ap)
    df_ap_coverage_sticky.printSchema()
    df_ap_coverage_sticky.show(2)

    #  Using scan_data for ap-neighbor,  edges
    #
    hr = "23"
    s3_ap_neighbors_bucket = "s3://mist-aggregated-stats-{env}/aggregated-stats/" \
                             "top_1_time_epoch_by_site_ap_ap2_band/dt={day}/hr={hr}/*.csv".format(env=env, day=date_day,
                                                                                                  hr=hr)
    print(s3_ap_neighbors_bucket)

    df_edges = load_ap_scan_data(spark, s3_ap_neighbors_bucket)
    df_edges.count()

    df_final = join_nodes_edges_dfs(df_ap_coverage_sticky, df_edges)
    df_final.show(2)

    s3_out_bucket = "s3://mist-test-bucket/wenfeng/df-joined-new/"
    df_final.write.parquet(s3_out_bucket)
    df_final_new = spark.read.parquet(s3_out_bucket)

    df_joined_g = generate_events(df_final)
    df_joined_g.select("ap_coverage_score").describe().show()


# TODO:
# site_id = "5e8fe474-a9ee-4d01-a2b6-b022b0f9c869"  # GEG1 , AmazonOTFC-prod
site_id = "a7092875-257f-43f3-9514-ca1ab688bec0"  # Sam's club. 4989
# site_id = "d1ee1d22-4b55-4c97-97c4-9d757144f45b"

import json
from pyspark.sql import functions as F

s3_bucket='s3://mist-data-science-dev/ap-coverage-test/event_data_production/dt=2020-10-04/'

df = spark.read.format("csv") \
             .option("header", "true").option("inferSchema", "true") \
             .load(s3_bucket)
df.count()

df.select("ap_id").agg(F.countDistinct("ap_id")).show()
df.select("ap_id").agg(F.count("ap_id").desc()).show()



s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2020-10-05/hr=*/'
df_coverage = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])).\
    filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
            .map(lambda x: x.get("source")) \
         .toDF()

df_coverage.count()


s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats/dt=2020-10-06/hr=21/last_1_day/'
s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats/dt=2020-10-06/hr=21/*.csv'
df_coverage_anomaly_stats = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true") \
    .load(s3_bucket)
df_coverage_anomaly_stats.count()




s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/sticky_client_stats/dt=2020-10-06/hr=21/*.csv'
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true") \
    .load(s3_bucket)
df.count()


#
#
s3_bucket='s3://mist-data-science-dev/ap-coverage-test/event_data_production/dt=2020-10-20/'
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true") \
    .load(s3_bucket)
df.count()

df.select("site_id", "ap_id").groupBy("ap_id").count().count()
df.select("ap1_avg_nclients",  "ap1_coverage_score", "ap1_combined_score").summary().show()

df_filter= df.filter('ap1_avg_nclients>2.0 and ap1_combined_score>0.5')
df_filter.count()


df_sites= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id").count().orderBy(F.col('count').desc())
df_sites.show(5, truncate=False)



df_orgs= df_filter.select("org_id", "site_id").groupBy("org_id").agg(
    F.count("site_id").alias("count"),
    F.countDistinct("site_id").alias("sites")) \
    .orderBy(F.col('sites').desc()
             )
df_orgs.show(5, truncate=False)

df_sites= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id").agg(
    F.count("ap_id").alias("count"),
    F.countDistinct("ap_id").alias("aps"))\
    .orderBy(F.col('aps').desc()
             )
df_sites.show(5, truncate=False)


df_selected_org= df_filter.filter(F.col("org_id")=="bbb101eb-b62d-4fb1-8c3d-030c6db7e208")
cols = ["org_id", "site_id", "ap_id", "band", 'ap1_sle_coverage', 'ap1_avg_nclients', 'ap1_coverage_score', "ap1_combined_score"]
df_selected_sites = df_selected_org.select(cols)\
    .groupBy("org_id", "site_id", "band").agg(
        F.count("ap_id").alias("anomalies"),
        F.collect_set("ap_id").alias("impacted_aps"),
        F.countDistinct("ap_id").alias("count_aps"),
        F.avg('ap1_sle_coverage').alias("avg_sle_coverage"),
        F.max("ap1_coverage_score").alias("worst_coverage_score"),
        F.max("ap1_combined_score").alias("worset_combined_score"),
        F.sum("ap1_avg_nclients").alias("sum_nclients")
        ) \
    .orderBy(F.col('count_aps').desc()
             )
df_selected_sites.show(10, truncate=False)


df_site_aps= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id", "ap_id").count().orderBy(F.col('count').desc())
df_site_aps.show(5, truncate=False)

def get_site_info(site_id):
    import requests
    res= requests.get("")
