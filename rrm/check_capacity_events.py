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


def test_enrichment():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1/4
    end_time = start_time + 3600 * 1
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job= data_enrichment_job("ap_capacity_enrichment",  start_time , end_time, spark=spark, test_env='production', debug_mode=True)

    job.execute()


def check_capacity_anomaly_from_ap_events():
    s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2021-03-1*/hr=*/'
    df_capacity = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    df_capacity.printSchema()

    df_capacity.count()
    return df_capacity


def check_capacity_anomaly_stats():

    s3_bucket ='s3://mist-aggregated-stats-staging/aggregated-stats/capacity_anomaly_stats/dt=2021-03-21/hr=17/*.csv'
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


def check_capacity_anomaly_stats_test():
    from pyspark.sql import functions as F

    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_capacity_stats_test/dt=2021-03-*/hr=*/'
    df_ap_capacity_stats_test = spark.read.parquet(s3_bucket)
    df_ap_capacity_stats_test.printSchema()

    df_ap_capacity_stats_test.select((F.col("max_tx_power")>0).alias("power_on"), "radio_missing") \
        .groupBy("power_on", "radio_missing").count().show()

    df_ap_capacity_stats_test.filter(F.col("org_id").isNull()).select("org_id", "site_id").groupBy("org_id", "site_id").count().show()

    df_ap_capacity_stats_test.count()
    df_ap_capacity_stats_test.select("avg_nclients", "util_ap", "error_rate", "max_power").summary().show()

    # test ap_capacity_score
    from analytics.event_generator.ap_capacity_event import *
    features_df = df_ap_capacity_stats_test
    features_df = features_df.withColumn("ap1_capacity_score",
                                         ap_capacity_score(F.col("avg_nclients"),
                                                           F.col("sle_capacity"),
                                                           F.col("sle_capacity_anomaly_score"),
                                                           F.col("capacity_anomaly_count")
                                                           )
                                         )

    features_df.select("avg_nclients", "sle_capacity", "sle_capacity_anomaly_score",
                       "capacity_anomaly_count", "sticky_uniq_client_count",
                       "ap1_capacity_score").summary().show()

    return df_ap_capacity_stats_test




