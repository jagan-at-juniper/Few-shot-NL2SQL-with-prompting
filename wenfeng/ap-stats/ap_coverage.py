#!/usr/bin/env python


import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

import numpy as np

def sigmoid(x):
    """
    sigmoid function
    :param x:
    :return:
    """
    return 1 / (1 + np.exp(-x))

def ap_coverage_score(nclients, sle_coverage, sle_coverage_anomaly_dev, coverage_anomaly_count):
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
                sigmoid(nclients - 3.0) * \
                sigmoid(sle_coverage_anomaly_dev - 2.0) * \
                sigmoid(coverage_anomaly_count - 2)
    else:
        score = 0.0
    return float(score)
ap_coverage_score = F.udf(ap_coverage_score, FloatType())



def neigbhor_anomalies(ap2, ap2_sle_coverage, ap2_avg_nclients):
    """

    :param ap2:
    :param ap2_sle_coverage:
    :param ap2_avg_nclients:
    :return:
    """
    if ap2_avg_nclients and ap2_avg_nclients>2.0 and (ap2_sle_coverage and ap2_sle_coverage< 0.5):
        return ap2
    return
neigbhor_anomalies = F.udf(neigbhor_anomalies)


def combined_serverity_score(sle_coverage_anomaly_score, strong_neighbors=0, neighbor_anomaly=None, tx_rx_utl=1.0):
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
    #         else:
    #             score -= np.log(strong_neighbors)

    score = score * tx_rx_utl
    #     if score > 1.0:
    #         score = 1.0
    #     elif score < 0.0:
    #         score = 0.0

    return float(score)
combined_coverage_score= F.udf(combined_serverity_score)


def generate_events_df(df):
    window = Window.partitionBy(F.col('ap1')).orderBy(F.col('rssi').desc())

    filters = 'ap1_avg_nclients > 3.0 and ap1_sle_coverage < 0.50'

    df = df.filter(filters) \
        .select('*', F.rank().over(window).alias('neighbor_rank')) \
        .filter(F.col('neighbor_rank') <= 3)

    df_new = df.groupBy("org_id", "site_id", "ap1") \
        .agg(
        F.avg("ap1_avg_nclients").alias("ap1_avg_nclients"),
        F.avg("ap1_sle_coverage").alias("ap1_sle_coverage"),
        F.sum("ap1_coverage_anomaly_count").alias("ap1_coverage_anomaly_count"),
        F.avg("ap1_sle_coverage_anomaly_score").alias("ap1_sle_coverage_anomaly_score"),
        F.max("ap1_sticky_uniq_client_count").alias("ap1_sticky_uniq_client_count"),
        F.countDistinct("ap2").alias("strong_neighbors"),
        F.countDistinct(neigbhor_anomalies(F.col("ap2"),
                                           F.col("ap2_sle_coverage"),
                                           F.col("ap2_avg_nclients"))).alias("anomaly_neighbors")
    ).withColumn("ap1_coverage_score", ap_coverage_score(F.col("ap1_avg_nclients"),
                                                         F.col("ap1_sle_coverage"),
                                                         F.col("ap1_sle_coverage_anomaly_score"),
                                                         F.col("ap1_coverage_anomaly_count"))
                 )


    df_new= df_new.withColumn("serverity_score", combined_serverity_score(F.col("ap1_coverage_score"),
                                                                          F.col("strong_neighbors"),
                                                                          F.col("anomaly_neighbors")
                                                                          )
                              )

    # df_new.filter("anomaly_neighbors>0").count()

    return df_new



# get_last_ap_coverage_stats
def load_ap_coverage_stats(dt='2020-09-03', hr="*", spark):
    s3_bucket = 's3://mist-aggregated-stats-production/'
    file_path = 'aggregated-stats/ap_coverage_stats/dt={}/hr={}'.format(dt, hr)
    return spark.read.parquet(s3_bucket + file_path)


df_new = generate_events_df(df)

filters = 'ap1_avg_nclients > 3.0 and ap1_sle_coverage < 0.50'

df_identified = df_new.filter("ap1_coverage_score > 0.5 and strong_neighbors")
good_sites = ["5c7f5e8fe474-a9ee-4d01-a2b6-b022b0f9c869", # GEG1, Amazon
              "813724d1-0a77-4898-8d05-7f0e8bdc76ba", # DXW2
              "09df1500-f1b3-4501-a805-7af21a1fb90f"    # USNJPRS
              ]

problematic_sites = ["b751bbda-948b-4853-a025-006a6aa9180e",                # Target 2456
                     "a7092875-257f-43f3-9514-ca1ab688bec0"    # Sam's. 4989
                     ]


df_good_sites = df.filter(df.site_id.isin(good_sites))
df_prolematic_sites = df.filter(df.site_id.isin(problematic_sites))
df_good_filter = df_good_sites.filter(filters)
df_prolematic_filter = df_prolematic_sites.filter(filters)



