
import json
from pyspark.sql import functions as F

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
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 6
    end_time = start_time + 3600
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job= data_enrichment_job("ap_coverage_enrichment",  start_time , end_time, spark=spark, test_env='staging', debug_mode=False)
    job.execute()

def test_detection():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 5
    end_time = start_time + 3600

    job = start_debug_job('ap_coverage_detection', start_time, end_time, spark=spark, test_env='staging', debug_mode=False)
    data = run_category_transform(job, 'all')

    # job= data_enrichment_job("ap_coverage_enrichment",  start_time , end_time, spark=spark, test_env='staging', debug_mode=False)
    # job.execute()

    pass


def check_coverage_anomaly_from_ap_events():
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, FloatType, LongType, StructType, StructField, ArrayType, MapType
    import json
    from copy import deepcopy
    s3_bucket= 's3://mist-aggregated-stats-production/event_generator/ap_last_seen/'.replace("production", "production")
    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))

    def flat_radios(input):
         if input.get('radios'):
            radios = input.pop('radios')
            res = deepcopy(input)

            for r in radios:
                res.update(r)
                yield res
         else:
            return input

    schema = StructType([
        StructField("org_id", StringType()),
        StructField("site_id", StringType()),
        StructField("ap_id", StringType()),
        StructField("model", StringType()),
        StructField("firmware_version", StringType()),
        StructField("terminator_timestamp", LongType()),
        StructField("dev", StringType()),
        StructField("band", StringType()),
        StructField("channel", IntegerType()),
        StructField("bandwidth", IntegerType()),
        StructField("max_tx_power", IntegerType())
    ])
    df_ap = spark.createDataFrame(ap_rdd.flatMap(flat_radios), schema)

    df_ap.printSchema()

    # df_ap.filter(~(df_ap.dev=="r2")).select("max_tx_power").summary().show()
    df_ap.filter(df_ap.dev!="r2").select("max_tx_power").summary().show()

    df_ap_radio_off= df_ap.filter("dev != 'r2'").filter("max_tx_power == 0")

    # df_ap_radio_off.select("org_id", "site_id", "ap_id", "dev", "band").show(truncate=False)
    # df_ap_radio_off.select("max_tx_power", "bandwidth").summary().show()
    #
    #
    # df_ap_radio_off=df_ap_radio_off.withColumn("ap_rad", F.concat(F.col("ap_id"), F.lit(":"), F.col("dev")))
    # df_ap_radio_off.count()

    df_site_radio_off = df_ap_radio_off.select("org_id", "site_id", "band", "ap_rad")\
        .groupBy("org_id", "site_id", "band").agg(F.count('ap_rad').alias("radios"),
                                                  F.collect_set("ap_rad").alias("radios_off"),
                                                  )\
        .orderBy(F.col("radios").desc())

    df_site_radio_off.show(truncate=False)

    df_site_radio_off.orderBy(F.col("radios").desc()).select("org_id", "site_id", "band", "radios").show(10, truncate=False)


    df_site_radio_off.count()




    # joined with SLE-coverage

    s3_bucket = "s3://mist-aggregated-stats-staging/aggregated-stats/ap_coverage_stats/dt=2020-10-30/hr=00/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.count()
    df_ap_coverage_stats.printSchema()

    # df_filter = df_ap_coverage_stats\
    #     .filter(df_ap_coverage_stats.ap2.isNotNull())\
    #     .select("ap_id", "terminator_timestamp", "dev", F.col("band").alias("band_off"), "max_tx_power")
    df_ap_radio_off = df_ap_radio_off.select("ap_id", "terminator_timestamp", "dev", F.col("band").alias("band_off"), "max_tx_power")

    # df_coverage_with_radio_off = df_ap_coverage_stats.join(df_ap_radio_off, [df_ap_coverage_stats.ap2 == df_ap_radio_off.ap_id], how='left')

    df_coverage_with_radio_off = df_ap_coverage_stats.join(df_ap_radio_off, [df_ap_coverage_stats.ap2 == df_ap_radio_off.ap_id], how='inner')

    df_coverage_with_radio_off.count()


    df_site_radio_off=df_site_radio_off.withColumnRenamed("site_id", "off_site_id").withColumnRenamed("band", "off_band")
    df_ap_coverage_stats_site = df_ap_coverage_stats.join(df_site_radio_off,
                                                          [df_ap_coverage_stats.org_id == df_site_radio_off.org_id],
                                                          how='inner')

    df_ap_coverage_stats_site.count()


    #
    #
    # s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2020-10-05/hr=*/'
    # df_coverage = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
    #     filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
    #     .map(lambda x: x.get("source")) \
    #     .toDF()
    # df_filter= df.filter('ap1_avg_nclients>2.0 and ap1_combined_score>0.5')



    # agg(F.count("ap_id").desc()).show()

    # df_ap.select("ap_id", F.explode("radios")).show()

    # df_ap = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))\
    #         .map(lambda x: x.get("source")) \
    #         .toDF()

    df_ap.count()
    return df_ap

def check_coverage_anomaly_from_ap_events():
    s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2020-10-05/hr=*/'
    df_coverage = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    df_coverage.count()
    return df_coverage


def check_sticky_clients():
    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/sticky_client_stats/dt=2020-10-06/hr=21/*.csv'
    df_sticky = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df_sticky.count()

    return df_sticky


def check_coverage_anomaly_stats():
    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats/dt=2020-10-30/hr=00/*.csv'
    df_coverage_anomaly_stats = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)

    df_coverage_anomaly_stats.count()

    return df_coverage_anomaly_stats


def check_coverage_anomaly_stats_test():
    s3_bucket = 's3://mist-aggregated-stats-staging/aggregated-stats/ap_coverage_stats_test/dt=2020-10-27/hr=00/'
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.count()
    df_ap_coverage_stats_test.printSchema()

    df_ap_coverage_stats_test.select("avg_nclients", "util_ap", "error_rate", "max_power").summary().show()

    return df_ap_coverage_stats_test


def check_ap_coverage_stats():
    s3_bucket = "s3://mist-aggregated-stats-staging/aggregated-stats/ap_coverage_stats/dt=2020-10-27/hr=00/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.count()
    df_ap_coverage_stats.printSchema()

    df_ap_coverage_stats.select("ap1_avg_nclients", "ap1_util_ap", "ap1_error_rate", "ap1_max_power").summary().show()

    return df_ap_coverage_stats




def check_coverage_events():
    s3_bucket='s3://mist-aggregated-stats-production/ap-coverage-test/event_data_production/dt=2020-10-27/'
    # gs_bucket='gs://mist-aggregated-stats-production/ap-coverage-test/event_data_production/dt=2020-10-23/'

    df = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df.printSchema()

    # df.count()

    df.select("ap_id").agg(F.countDistinct("ap_id")).show()
    df.select("ap_id").agg(F.count("ap_id").desc()).show()

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
    df_orgs.count()
    df_orgs.show(5, truncate=False)

    df_sites= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id").agg(
        F.count("ap_id").alias("count"),
        F.countDistinct("ap_id").alias("aps")) \
        .orderBy(F.col('aps').desc()
                 )
    df_sites.show(5, truncate=False)


    df_site_aps= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id", "ap_id").count().orderBy(F.col('count').desc())
    df_site_aps.count()
    df_site_aps.show(5, truncate=False)

    df_filter= df.filter('ap1_avg_nclients>2.0 and ap1_combined_score>0.3')
    df_filter.count()


    select_org = "f5451dc6-aa80-4d1c-a49a-dede30b6d878"  # PetSmart
    # select_org ="0992350f-e897-4719-8671-010a7e4ebf9c"  # MIT
    # df_selected_org= df_filter.filter(F.col("org_id")=="bbb101eb-b62d-4fb1-8c3d-030c6db7e208")
    # df_selected_org = df_filter.filter(F.col("org_id")=="0992350f-e897-4719-8671-010a7e4ebf9c") # MIT
    df_selected_org = df_filter.filter(F.col("org_id")==select_org)

    # df_selected_org = df_filter.filter(F.col("site_id")=="6532c75a-c109-4f82-8270-36e199ca692e")
    cols = ["site_id", "ap_id", "band", 'ap1_sle_coverage', 'ap1_avg_nclients', 'ap1_coverage_score', "ap1_combined_score"]
    df_selected_sites = df_selected_org.select(cols) \
        .groupBy("site_id", "band").agg(
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



def test_ap_has_neighbor():
    s3_bucket = "s3://mist-aggregated-stats-staging/aggregated-stats/ap_has_neighbors/dt=2020-10-16/hr=00/"

    df = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df.count()




#
# coverage-events
#


def get_site_info(site_id):
    import requests
    res= requests.get("")


df_selected_org= df_filter.filter(F.col("org_id")=="bbb101eb-b62d-4fb1-8c3d-030c6db7e208")
cols = ["site_id", "ap_id", "band", 'ap1_sle_coverage', 'ap1_avg_nclients', 'ap1_coverage_score', "ap1_combined_score"]
df_selected_sites = df_selected_org.select(cols) \
    .groupBy( "site_id", "band").agg(
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
