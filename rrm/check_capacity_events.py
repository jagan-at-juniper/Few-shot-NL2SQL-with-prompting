import json
from pyspark.sql import functions as F

def test_aggregator():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 5
    end_time = start_time + 3600
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job = stats_aggregator_job(start_time, end_time, "ap-events",  spark, "staging")
    job.execute()



def check_capacity_anomaly_stats():

    s3_bucket ='s3://mist-aggregated-stats-staging/aggregated-stats/capacity_anomaly_stats/dt=2021-02-24/hr=0[2]/*.csv'
    df_capacity_anomaly_stats = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)

    df_capacity_anomaly_stats.printSchema()

    df_capacity_anomaly_stats.count()

    from analytics.event_generator.ap_capacity_event import *
    features_df = df_ap_capacity_stats_test
    features_df = features_df.withColumn("ap1_capacity_score",
                                         ap_capacity_score(F.col("avg_nclients"),
                                                           F.col("sle_capacity"),
                                                           F.col("sle_capacity_anomaly_score"),
                                                           F.col("capacity_anomaly_count"),
                                                           F.col("sticky_uniq_client_count")
                                                           )
                                         )

    features_df.select("avg_nclients", "sle_capacity", "sle_capacity_anomaly_score",
                       "capacity_anomaly_count", "sticky_uniq_client_count",
                       "ap1_capacity_score").summary().show()


    return df_capacity_anomaly_stats

