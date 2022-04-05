import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime,timedelta


spark = SparkSession \
    .builder \
    .appName("ap-capacity-events") \
    .getOrCreate()


def test_jobs():
    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 0
    end_time = start_time + 3600

    agg_job = stats_aggregator_job(start_time, end_time, "ap-events",  spark, "production")
    agg_job.execute()

    enrich_job= data_enrichment_job("ap_coverage_enrichment",  start_time, end_time, spark=spark, test_env='production', debug_mode=True)
    enrich_job.execute()

    detection_job = start_debug_job('ap_capacity_detection', start_time, end_time, spark=spark, test_env='production', debug_mode=True)
    data = run_category_transform(detection_job, 'all')
    gen = get_event_generator(detection_job, 'all', 'APCapacityEvent')
    event_rdd = gen.generate_event(data, spark)
    event_df = event_rdd.toDF()
    print("event_df ", event_df.count())
    event_df.show()

    # job = entity_suggestion_job(start_time, end_time, spark=spark, test_env='staging', debug_mode=False)
    # # job = entity_suggestion_job(start_time, end_time, spark=spark, debug_mode=False)
    # suggestions, alerts = recommend_for_entity(job, 'ap')

def test_aggregator():
    """
    :return:
    """
    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 0
    end_time = start_time + 3600
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job = stats_aggregator_job(start_time, end_time, "ap-events",  spark, "production")
    job.execute()


def test_enrichment():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds() - 3600 #//(3600*24) * (3600*24) - (3600*24) * 2
    end_time = start_time + 3600 * 1
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    # job= data_enrichment_job("ap_capacity_enrichment",  start_time , end_time, spark=spark, test_env='staging', debug_mode=True)

    job= data_enrichment_job("ap_capacity_enrichment",  start_time , end_time, spark=spark, test_env='staging', debug_mode=True)

    job.execute()

def test_detection():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 2
    end_time = start_time + 3600

    job = start_debug_job('ap_capacity_detection', start_time, end_time, spark=spark, test_env='production', debug_mode=True)
    data = run_category_transform(job, 'all')
    # print("data", data.count())
    cols = ['ap1_avg_nclients', 'ap1_util_ap',
            'ap1_util_all', 'ap1_util_all_base', 'ap1_rssi_mean', 'ap1_rssi_mean_base',
            'ap1_sle_coverage', 'ap1_sle_coverage_base',
            'ap1_sle_capacity', 'ap1_sle_capacity_base',
            'ap1_capacity_util_all', 'ap1_capacity_util_all_base']
    data.filter('ap1_capacity_avg_nclients>0').select(cols).show()

    gen = get_event_generator(job, 'all', 'APCapacityEvent')
    event_rdd = gen.generate_event(data, spark)
    event_df2 = event_rdd.toDF()
    print("event_df ", event_df2.count())

    #

    from analytics.event_generator.ap_capacity_event import *
    ap_coverage_stats_df = data
    features_df = gen.extract_feature_df(data)

    features_df.select((F.col("max_tx_power")>0).alias("power_on"), "radio_missing") \
        .groupBy("power_on", "radio_missing").count().show()

    features_df.select("strong_neighbors", "off_neighbors").summary().show()

    select_org = "22f1cc2d-ea8a-47ea-b4c0-689a86a0bedf"  # Target CORPORATIONFRI
    feature_df_org = features_df.filter(F.col("org_id")==select_org)
    feature_df_org.count()


def test_recommender():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24)
    end_time = start_time + 3600*12

    job = entity_suggestion_job(start_time, end_time, spark=spark, test_env='staging', debug_mode=False)
    # job = entity_suggestion_job(start_time, end_time, spark=spark, debug_mode=False)
    recommenders_list = job.config['suggestion_category']['ap']['recommenders']
    recommenders_short_list = [x for x in recommenders_list if x.get("symptom") in ['insufficient_capacity', 'insufficient_coverage']]
    job.config['suggestion_category']['ap']['recommenders'] = recommenders_short_list

    suggestions, alerts = recommend_for_entity(job, 'ap')

    suggestions, alerts


    d = job.prepare_data()
    # d.map(lambda x: (x.get("event_name"), 1)).groupByKey().collect()
    data_rdd = d.filter(lambda x: x.get("event_name")=="insufficient_coverage")
    entity_type = "ap"
    # suggestions, alerts= job.execute_for_type(data_rdd, entity_type)

    #
    recommender_config = job.config.get('suggestion_category')
    entity_type_config = recommender_config.get(entity_type)

    suggestions = []
    alerts = []
    from analytics.transformer.util import recursive_get_field
    collected_data = data_rdd \
        .filter(lambda r: r.get('entity_type', '') == entity_type) \
        .groupBy(lambda r: tuple([recursive_get_field(r, f) for f in entity_type_config.get('event_group_fields')])) \
        .collect()
    grouped_events = dict(collected_data)
    recommender = job.recommenders_by_entity_type.get(entity_type)[0]
    new_suggestions, new_alerts = recommender.recommend(job.spark, grouped_events)

    # suggestions = job.generate_suggestion(grouped_events)
    # suggestions = job.execute_for_type(filtered_events, "ap")

    # from analytics.recommender.ap_coverage_recommender import ApCoverageRecommender
    # coverage_job = ApCoverageRecommender(job.config)

    # job = start_debug_job('ap_coverage_detection', start_time, end_time, spark=spark, test_env='staging', debug_mode=True)
    # data = run_category_transform(job, 'all')
    #
    from analytics.recommender.recommender import *

    ApCoverage_Recommender = ApCoverageRecommender()



def check_capacity_anomaly_from_ap_events():
    s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2021-04-25/hr=*/'
    df_capacity = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    df_capacity.printSchema()

    df_capacity.select("sle_capacity", "sle_capacity_anomaly_score").summary().show()
    df_capacity.count()
    return df_capacity


def check_capacity_anomaly_stats():


    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/capacity_anomaly_stats_parquet/' \
               'dt=2021-09-20/hr=10/last_1_day/*.parquet'

    df_capacity_anomaly_stats = spark.read.parquet(s3_bucket)


    # s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/capacity_anomaly_stats/dt=2021-06-16/hr=18/*.csv'
    # df_capacity_anomaly_stats = spark.read.format("csv") \
    #     .option("header", "true").option("inferSchema", "true") \
    #     .load(s3_bucket)

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

    # s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_capacity_stats_test/dt=2021-04-25/hr=*/'
    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/dt=2022-03-17/hr=*/'
    df_ap_capacity_stats_test = spark.read.parquet(s3_bucket)
    df_ap_capacity_stats_test.printSchema()
    df_ap_capacity_stats_test.filter("capacity_avg_nclients>0").select("capacity_util_all", "capacity_util_all_base", "sle_capacity_base", "sle_capacity").show()

    df_ap_capacity_stats_test.select((F.col("max_tx_power")>0).alias("power_on"), "radio_missing") \
        .groupBy("power_on", "radio_missing").count().show()

    df_ap_capacity_stats_test.filter(F.col("org_id").isNull()).select("org_id", "site_id").groupBy("org_id", "site_id").count().show()

    df_ap_capacity_stats_test.count()
    df_ap_capacity_stats_test.select("avg_nclients", "sle_capacity", "util_ap", "error_rate", "max_power").summary().show()

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



def check_coverage_merge():
    from pyspark.sql import functions as F
    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/'
    # s3_gs_bucket += 'ap_capacity_stats/dt=2021-04-25/hr=00/'
    s3_gs_bucket += 'ap_coverage_stats/dt=2022-03-17/hr=*/'

    # s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-03-2[6]/hr=*/"
    df_ap_capacity_stats_1 = spark.read.parquet(s3_gs_bucket)
    df_ap_capacity_stats_1.printSchema()

    cols_1 = ["ap1_sle_capacity", "ap1_sle_capacity_base", "ap1_sle_capacity_anomaly_score",
               "ap1_capacity_avg_nclients", "ap1_error_rate",
              "ap1_capacity_anomaly_count", "ap1_util_ap",
               "ap1_capacity_util_all", "ap1_capacity_util_all_base"]
    df_ap_capacity_stats_1.filter("ap1_capacity_avg_nclients>0").select(cols_1).show()

    df_ap_capacity_stats_1.select(cols_1).summary().show()

    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/'
    s3_gs_bucket += 'ap_coverage_stats/dt=2021-04-25/hr=00/'

    # s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-03-2[6]/hr=*/"
    df_ap_capacity_stats_2 = spark.read.parquet(s3_gs_bucket)
    df_ap_capacity_stats_2.printSchema()

    cols_2 = ["ap1_sle_capacity", "ap1_sle_capacity_anomaly_score",
              "ap1_capacity_avg_nclients", "ap1_capacity_error_rate",
              "ap1_capacity_anomaly_count", "ap1_capacity_util_ap"]

    df_ap_capacity_stats_2.select(cols_2).summary().show()








def check_capacity_anomaly_from_ap_events(last_days=3):
    files = []
    for i in range(7):
        date_day= (datetime.now()  - timedelta(days=i)).strftime("%Y-%m-%d")

        s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt={}/hr=*/*.seq'.format(date_day)
        files.append(s3_bucket)
        # files = "dir1, dir2, dir3,"
    files = ",".join(files)
    rdd = spark.sparkContext.sequenceFile(files)
    df_capacity = rdd.map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    return df_capacity

def check_impact_aps(impacted_aps):
    impacted_aps = ['ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'd4dc09e4d38c', 'd4dc09e4d38c',
                    'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'ac23160dbbeb',
                    'ac23160dbbeb', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
                    'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
                    'd4dc092b8263', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc092b8263']


    df_capacity = check_capacity_anomaly_from_ap_events(7)
    df_capacity.printSchema()

    from pyspark.sql.types import *
    def ap_id_reformat(ap):
        return ap.replace("-", "")
    ap_id_reformat_f = F.udf(ap_id_reformat, StringType())
    df_capacity = df_capacity.withColumn("ap", ap_id_reformat_f(F.col("ap")))

    capacity_cols = ["timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
                     "interference_type", "sle_capacity", "util_ap", "util_all_mean"]

    df_capacity.select(capacity_cols).show()

    df_capacity_aps = df_capacity.filter(df_capacity.ap.isin(impacted_aps))
    df_capacity_aps.count()

    # df_capacity.show(2)
    df_capacity_aps.select(capacity_cols).show()