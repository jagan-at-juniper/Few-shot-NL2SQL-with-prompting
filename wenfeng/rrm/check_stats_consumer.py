
def test_stats_consumer():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds() - 3600* 24   #//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job = start_stats_consumer(start_time, end_time,  spark, "staging", 'stats_consumer')
    job.execute()

def test():
    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds() - 3600* 2   #//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600

    job = stats_aggregator_job(start_time, end_time, "cv-ap-scans-multipartition",  spark, "staging")
    job.execute()


    # s3 =
    import json
    from pyspark.sql import functions as F
    s3 = 's3://mist-secorapp-production/cv-ap-scans-multipartition/cv-ap-scans-multipartition-production/dt=2021-06-18/hr=02/'
    df = spark.read.format("orc") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3)
    df.printSchema()


def check_offline():
    from pyspark.sql import functions as F

    env = "staging"
    dt = "2021-06-18"
    s3_bucket = 's3://mist-aggregated-stats-{env}/aggregated-stats/ap_coverage_stats_test/dt={dt}/hr=*/'.format(env=env, dt=dt)
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats_test.printSchema()

    df_ap_coverage_stats_test.select("max_tx_power", "has_wlans").summary().show()
    df_ap_coverage_stats_test.filter("not has_wlans and max_tx_power==0").count()
    df_ap_coverage_stats_test.select("has_wlans", "radio_missing",
                                 (F.col("max_tx_power")>0).alias("last_seen_power"),
                                     (F.col("max_power")>0).alias("power_anomal"))\
            .groupBy("has_wlans", "radio_missing", "last_seen_power", "power_anomal").count().show()

