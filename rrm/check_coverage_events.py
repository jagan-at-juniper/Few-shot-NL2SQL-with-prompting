
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
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600 * 1
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job= data_enrichment_job("ap_coverage_enrichment",  start_time , end_time, spark=spark, test_env='production', debug_mode=False)

    job.execute()

def test_detection():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600

    job = start_debug_job('ap_coverage_detection', start_time, end_time, spark=spark, test_env='staging', debug_mode=True)
    data = run_category_transform(job, 'all')

    data.count()

    gen = get_event_generator(job, 'all', 'APCoverageEvent')
    event_rdd = gen.generate_event(data, spark)
    event_df = event_rdd.toDF()

    #
    ap_coverage_stats_df = data
    features_df = gen.extract_feature_df(data)

    features_df.select((F.col("max_tx_power")>0).alias("power_on"), "radio_missing")\
        .groupBy("power_on", "radio_missing").count().show()

    features_df.select("strong_neighbors", "off_neighbors").summary().show()

    select_org = "22f1cc2d-ea8a-47ea-b4c0-689a86a0bedf"  # Target CORPORATIONFRI
    feature_df_org = features_df.filter(F.col("org_id")==select_org)
    feature_df_org.count()

def test_recommender():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) /4
    end_time = start_time + 3600

    job = entity_suggestion_job(start_time, end_time, spark=spark, test_env='production', debug_mode=False)
    # job = entity_suggestion_job(start_time, end_time, spark=spark, debug_mode=False)
    suggestions, alerts = recommend_for_entity(job, 'ap')

    d = job.prepare_data()
    # d.map(lambda x: (x.get("event_name"), 1)).groupByKey().collect()
    data_rdd = d.filter(lambda x: x.get("event_name")=="insufficient_coverage")
    entity_type = "ap"
    suggestions, alerts= job.execute_for_type(data_rdd, entity_type)

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


def test_feature(job, data):
    from pyspark.sql import functions as F
    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600

    job = start_debug_job('ap_coverage_detection', start_time, end_time, spark=spark, test_env='staging', debug_mode=True)
    data = run_category_transform(job, 'all')

    gen = get_event_generator(job, 'all', 'APCoverageEvent')
    features_df = gen.extract_feature_df(data)
    # event_candidates_df = event_df.filter("avg_nclients>2.0 and ap_combined_score>0.5 and sle_coverage < 0.6")

    event_df = features_df.filter("avg_nclients>1.0 and ap_combined_score>0.3 and sle_coverage < 0.7")
    candidate_filter = "avg_nclients>2.0 and ap_combined_score>0.5 and sle_coverage < 0.6 and" \
                       " coverage_anomaly_count > 2 and (anomaly_neighbors > 1 or off_neighbors > 0)"
    event_candidates_df = event_df.filter(candidate_filter)

    # # ###
    select_org = "bbb101eb-b62d-4fb1-8c3d-030c6db7e208"  # US-walmart
    # select_org = "22f1cc2d-ea8a-47ea-b4c0-689a86a0bedf"  # Target CORPORATIONFRI
    # # select_org = "c1cac1c4-1753-4dde-a065-e17a1c305c2d" # GAP
    # select_org = "604411f1-4e45-4bed-9a69-cc37b247fdf9" # US- Sam's

    df_selected_org = event_candidates_df.filter(F.col("org_id") == select_org)

    cols_2 =  ["site_id", "ap_id", "band", 'sle_coverage', 'avg_nclients', 'util_ap',
               'ap_coverage_score', "ap_combined_score",
               "strong_neighbors","anomaly_neighbor_ids",
               "off_neighbor_ids"]
    df_selected_org.filter(F.col("off_neighbors")>0).select(cols_2).show(truncate=False)

    cols = ["site_id", "ap_id", "band", 'sle_coverage', 'avg_nclients', 'util_ap',
            'ap_coverage_score', "ap_combined_score",
            "strong_neighbors","anomaly_neighbors",
            "off_neighbors"]
    df_selected_sites = df_selected_org.select(cols) \
        .groupBy("site_id", "band").agg(
        F.count("ap_id").alias("anomalies"),
        F.collect_set("ap_id").alias("impacted_aps"),
        F.countDistinct("ap_id").alias("count_aps"),
        F.avg('sle_coverage').alias("avg_sle_coverage"),
        F.max("ap_coverage_score").alias("worst_coverage_score"),
        F.max("ap_combined_score").alias("worset_combined_score"),
        F.sum("avg_nclients").alias("sum_nclients"),
        F.max("strong_neighbors").alias("strong_neighbors"),
        F.max("anomaly_neighbors").alias("anomaly_neighbors"),
        F.max("off_neighbors").alias("off_neighbors")
    )
    df_selected_sites = df_selected_sites.orderBy(
        F.col('count_aps').desc(),
        F.col("anomaly_neighbors").desc()
    )

    df_selected_sites.count()
    # df_selected_sites.select("count_aps", "avg_sle_coverage",
    #                          "worst_coverage_score",
    #                          "worset_combined_score","sum_nclients").summary().show()
    df_selected_sites.filter(F.col("band")=="24").show(10, truncate=False)
    df_selected_sites.filter(F.col("band")=="5").show(10, truncate=False)


    # ###

    features_df_count = features_df.count()
    event_df_count = event_df.count()
    event_candidates_df_count = event_candidates_df.count()
    print(features_df_count, event_df_count, event_candidates_df_count)

    event_df.select("avg_error_rate", "avg_nclients", "sle_coverage", "coverage_anomaly_count", "ap_coverage_anomaly_score", "ap_coverage_score", "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors").summary().show()
    event_df.select("band").groupBy("band").count().show()

    event_candidates_df.select("band").groupBy("band").count().show()
    event_candidates_df.select("avg_error_rate", "avg_nclients", "sle_coverage",
                               "coverage_anomaly_count", "ap_coverage_anomaly_score",
                               "ap_coverage_score", "ap_combined_score",
                               "strong_neighbors", "off_neighbors", "anomaly_neighbors").summary().show()
    event_candidates_df.select( "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors").summary().show()
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.stat import Correlation

    df_t = event_candidates_df.select( "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors")
    r1 = Correlation.corr(df_t, "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors").head()
    print("Pearson correlation matrix:\n" + str(r1[0]))

    df_pd = event_candidates_df.select( "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors", (F.col("anomaly_neighbors")/F.col("strong_neighbors")).alias("anomaly_ratio")).toPandas()

    event_candidates_df.select( "ap_combined_score",  "strong_neighbors", "off_neighbors", "anomaly_neighbors",
                                (F.col("off_neighbors")/F.col("strong_neighbors")).alias("off_ratio"),
                                (F.col("anomaly_neighbors")/F.col("strong_neighbors")).alias("anomaly_ratio"),
                                (F.col("off_neighbors")>0).alias("has_offline_neighbors"),
                                (F.col("anomaly_neighbors")>2).alias("has_anomaly_neighbors")
                                )\
        .summary().show()

    event_candidates_df.select(
                                (F.col("off_neighbors")>0).alias("has_offline_neighbors"),
                                (F.col("anomaly_neighbors")>2).alias("has_anomaly_neighbors")
                                ).groupBy("has_offline_neighbors", "has_anomaly_neighbors").count().show()


    candidate_rdd = event_candidates_df \
        .rdd \
        .map(lambda r: ('_'.join([r['org_id'], r['site_id'], r['ap_id'], str(r['band'])]), r.asDict())) \
        .groupByKey().persist()

    feature_rdd = gen.compose_entity_features(candidate_rdd)
    event_rdd = gen.gen_intra_batch_event(feature_rdd)
    combined_event_rdd = gen.cross_batch_event_correlation(event_rdd)
    # event_rdd = gen.cross_batch_event_correlation(event_rdd)

    data_collect = combined_event_rdd.filter(lambda x: x.get("serverity")>50).collect()

    ### tmp
    event_rdd = feature_rdd \
        .map(lambda kv_tuple: (kv_tuple[0], {'details': gen.compose_event_details(kv_tuple[1]),
                                             'val_list': kv_tuple[1]['val_list']})) \
        .map(lambda kv_tuple: compose_ap_event(gen.config['entity_type'],
                                               kv_tuple[0],
                                               kv_tuple[1]['val_list'],
                                               gen.config['timestamp_field'],
                                               kv_tuple[1]['details'],
                                               gen.config))


    pass

def test_detection_cross_batch():


    from pyspark.sql import functions as F
    env = "production"
    env = "staging"

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600 *1

    job = start_debug_job('ap_coverage_detection', start_time, end_time, spark=spark, test_env='staging', debug_mode=True)
    gen = get_event_generator(job, 'all', 'APCoverageEvent')

    # s3_gs_bucket='s3://mist-aggregated-stats-{env}/ap-coverage-test/event_data_{env}/dt=2020-12-01/hr=*'.format(env=env)
    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_candidates/dt=2021-01-27/hr=10/'

    event_df = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_gs_bucket)
    event_df.printSchema()

    event_candidates_df = event_df.filter("avg_nclients>2.0 and ap_combined_score>0.5 and sle_coverage < 0.6 and off_neighbors>0")

    candidate_rdd = event_candidates_df.filter('off_neighbors>0') \
        .rdd \
        .map(lambda r: ('_'.join([r['org_id'], r['site_id'], r['ap_id']]), r.asDict())) \
        .groupByKey().persist()

    # candidate_rdd.map(lambda x: max([x0.get("avg_nclients") for x0 in x[1]])).collect()
    feature_rdd = gen.compose_entity_features(candidate_rdd)
    event_rdd = gen.gen_intra_batch_event(feature_rdd)
    combined_event_rdd = gen.cross_batch_event_correlation(event_rdd)

    # feature_rdd.map(lambda x:  [xx for xx in x[1].get("val_list")]).first()

def check_coverage_anomaly_from_ap_events():
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, FloatType, LongType,\
        StructType, StructField, ArrayType, MapType, BooleanType
    import json
    from copy import deepcopy
    s3_bucket= 's3://mist-aggregated-stats-production/event_generator/ap_last_seen/'.replace("production", "production")
    # s3_bucket= 's3://mist-aggregated-stats-production/event_generator/dt=2020-11-09/hr=19/'
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
        StructField("max_tx_power", IntegerType()),
        StructField("wlans", ArrayType(StringType())),
        StructField("radio_missing", BooleanType())
    ])
    df_ap_radio = spark.createDataFrame(ap_rdd.flatMap(flat_radios), schema)

    df_ap_radio.printSchema()

    df_ap_radio = df_ap_radio.filter(df_ap_radio.dev!="r2")

    df_ap_radio.select((F.col("max_tx_power")>0).alias("power_on"),
                       (F.size("wlans")>0).alias("has_wlans"))\
        .groupBy("power_on", "has_wlans").count().show()

    df_ap_radio.select("dev", "band", (F.col("bandwidth")==0).alias("bandwidth0"),
                       (F.col("max_tx_power")==0).alias("power_off"),
                       "radio_missing",
                       (F.size("wlans")<1).alias("no_wlans")) \
        .groupBy("dev", "band", "bandwidth0", "power_off", "radio_missing", "no_wlans").count().show()


    df_ap_radio =    df_ap_radio.select("*",
                                        (F.col("bandwidth")==0).alias("bandwidth0"),
                                        (F.col("max_tx_power")==0).alias("power_off"),
                                        (F.size("wlans")<1).alias("no_wlans")
                                        )


    df_ap_radio.select("bandwidth0", "power_off", "radio_missing", "no_wlans")\
        .groupBy( "bandwidth0", "power_off", "radio_missing", "no_wlans").count().show()


    """
    +----------+---------+-------------+--------+------+
|bandwidth0|power_off|radio_missing|no_wlans| count|
+----------+---------+-------------+--------+------+
|      true|     true|        false|   false|  1137|
|     false|     true|        false|    true|     2|
|     false|    false|        false|   false|575432|
|      true|     true|        false|    true| 10617|
|     false|    false|        false|    true|    18|
|      true|     true|         true|    true|   810|
+----------+--------+-------------+---------+------+
    """
    filter_legacy = "not bandwidth0 and power_off and not radio_missing and no_wlans"
    df_ap_radio.filter(filter_legacy).show()

    filter_has_wlans = "bandwidth0 and power_off and not radio_missing and not no_wlans"
    df_ap_radio.filter(filter_has_wlans).count()

    #
    filter_no_wlans_with_power = "not bandwidth0 and not power_off and not radio_missing and no_wlans"
    df_ap_radio.filter(filter_no_wlans_with_power).count()


    filter_no_wlans = "bandwidth0 and power_off and not radio_missing and no_wlans"
    df_ap_radio.filter(filter_no_wlans).count()


    df_ap_radio.filter(F.col("bandwidth")==0 ).filter(F.col("max_tx_power")==0).filter(F.size("wlans")==0 ).show()

    df_ap_radio.filter(F.col("bandwidth")==0 ).filter(F.col("max_tx_power")==0).filter(F.size("wlans")==0 ).show()

    #  tx_power=0 and has_wlan
    df_ap_radio.filter(F.size("wlans")>0 ).filter(F.col("max_tx_power")==0)\
        .select("org_id", 'firmware_version')\
        .groupBy("org_id", 'firmware_version').count().show()

    # df_ap.filter(~(df_ap.dev=="r2")).select("max_tx_power").summary().show()

    # df_ap_radio.write.parquet("s3://mist-aggregated-stats-production/event_generator/ap_radio_last_seen/")

    df_ap_radio.filter(F.col("site_id")=="b8a9571e-6724-4ccf-b635-07a1968fd795").select("ap_id", "model", "dev", "band", "bandwidth", "max_tx_power").orderBy(F.col("max_tx_power").asc()).show(80)

    df_ap_radio.select("max_tx_power").summary().show()

    df_ap_radio_off= df_ap_radio\
        .filter("max_tx_power == 0")\
        .withColumn('band_off', F.col('band').cast(IntegerType()))\
        .withColumn("ap_rad", F.concat(F.col("ap_id"), F.lit(":"), F.col("dev")))

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

    df_ap_radio_off = df_ap_radio_off.select("ap_id", "terminator_timestamp", "dev", F.col("band").alias("band_off"), "max_tx_power")


    # # join coverage-hole events
    #
    # s3_bucket='s3://mist-aggregated-stats-production/ap-coverage-test/event_data_production/dt=2020-10-30/'
    # # gs_bucket='gs://mist-aggregated-stats-production/ap-coverage-test/event_data_production/dt=2020-10-23/'
    #
    # df_events = spark.read.format("csv") \
    #     .option("header", "true").option("inferSchema", "true") \
    #     .load(s3_bucket)
    # df_events.printSchema()
    #
    # df_filter= df_events.filter('ap1_avg_nclients>2.0 and ap1_combined_score>0.5')
    # df_filter.count()
    #
    # df_filter = df_filter.join(df_ap_radio_off, [df_filter.ap2 == df_ap_radio_off.ap_id,
    #                                              df_filter.band == df_ap_radio_off.band_off],
    #                                                        how='inner')



    # joined with SLE-coverage

    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-01-08/hr=*/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.count()
    df_ap_coverage_stats.printSchema()

    # df_filter = df_ap_coverage_stats\
    #     .filter(df_ap_coverage_stats.ap2.isNotNull())\
    #     .select("ap_id", "terminator_timestamp", "dev", F.col("band").alias("band_off"), "max_tx_power")

    # df_coverage_with_radio_off = df_ap_coverage_stats.join(df_ap_radio_off, [df_ap_coverage_stats.ap2 == df_ap_radio_off.ap_id], how='left')

    # df_coverage_with_radio_off = df_ap_coverage_stats.join(df_ap_radio_off, [df_ap_coverage_stats.ap2 == df_ap_radio_off.ap_id], how='inner')
    df_coverage_with_radio_off = df_ap_coverage_stats.join(df_ap_radio_off, [df_ap_coverage_stats.ap2 == df_ap_radio_off.ap_id,
                                                                             df_ap_coverage_stats.band == df_ap_radio_off.band_off],
                                                           how='inner')


    df_coverage_with_radio_off.count()  # 0


    df_site_radio_off = df_site_radio_off.withColumnRenamed("site_id", "off_site_id").withColumnRenamed("band", "off_band")
    df_ap_coverage_stats_site = df_ap_coverage_stats.join(df_site_radio_off,
                                                          [df_ap_coverage_stats.site_id == df_site_radio_off.site_id],
                                                          how='inner')

    df_ap_coverage_stats_site.count()  # 0


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

def check_capacity_anomaly_from_ap_events():
    s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2020-12-09/hr=*/'
    df_capacity = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    df_capacity.printSchema()

    df_capacity.count()
    return df_capacity

def check_coverage_anomaly_from_ap_events():
    s3_bucket= 's3://mist-secorapp-production/ap-events/ap-events-production/dt=2021-01-08/hr=*/'
    df_coverage = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_coverage_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    df_coverage.printSchema()

    df_coverage.count()
    return df_coverage


def check_top_scan():

    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/top_1_time_epoch_by_site_ap_ap2_band/dt=2021-02-04/hr=*/*.csv'
    df_scan = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    df_scan.printSchema()
    pass

def check_sticky_clients():
    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/sticky_client_stats/dt=2020-12-03/hr=*/*.csv'
    # s3_bucket ='gs://mist-aggregated-stats-production/aggregated-stats/sticky_client_stats/dt=2020-11-11/hr=*/*.csv'
    df_sticky = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)

    df_sticky.printSchema()

    df_sticky.count()

    return df_sticky


def check_coverage_anomaly_stats():

    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/coverage_anomaly_stats/dt=2021-02-19/hr=20/*.csv'
    df_coverage_anomaly_stats = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)

    df_coverage_anomaly_stats.printSchema()

    df_coverage_anomaly_stats.count()

    from analytics.event_generator.ap_coverage_event import *
    features_df = df_ap_coverage_stats_test
    features_df = features_df.withColumn("ap1_coverage_score",
                                         ap_coverage_score(F.col("avg_nclients"),
                                                           F.col("sle_coverage"),
                                                           F.col("sle_coverage_anomaly_score"),
                                                           F.col("coverage_anomaly_count"),
                                                           F.col("sticky_uniq_client_count")
                                                           )
                                         )

    features_df.select("avg_nclients", "sle_coverage", "sle_coverage_anomaly_score",
                       "coverage_anomaly_count", "sticky_uniq_client_count",
                       "ap1_coverage_score").summary().show()


    return df_coverage_anomaly_stats


def check_coverage_anomaly_stats_test():
    from pyspark.sql import functions as F

    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/dt=2021-02-*/hr=*/'
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats_test.printSchema()

    df_ap_coverage_stats_test.select((F.col("max_tx_power")>0).alias("power_on"), "radio_missing")\
        .groupBy("power_on", "radio_missing").count().show()

    df_ap_coverage_stats_test.filter(F.col("org_id").isNull()).select("org_id", "site_id").groupBy("org_id", "site_id").count().show()

    df_ap_coverage_stats_test.count()
    df_ap_coverage_stats_test.select("avg_nclients", "util_ap", "error_rate", "max_power").summary().show()

    # test ap_coverage_score
    from analytics.event_generator.ap_coverage_event import *
    features_df = df_ap_coverage_stats_test
    features_df = features_df.withColumn("ap1_coverage_score",
                                         ap_coverage_score(F.col("avg_nclients"),
                                                           F.col("sle_coverage"),
                                                           F.col("sle_coverage_anomaly_score"),
                                                           F.col("coverage_anomaly_count"),
                                                           F.col("sticky_uniq_client_count")
                                                           )
                                         )

    features_df.select("avg_nclients", "sle_coverage", "sle_coverage_anomaly_score",
                       "coverage_anomaly_count", "sticky_uniq_client_count",
                       "ap1_coverage_score").summary().show()

    return df_ap_coverage_stats_test


def check_ap_coverage_stats():
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-02-0[78]/hr=*/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats.printSchema()

    df_ap_coverage_stats.select((F.col("ap1_max_tx_power")>0).alias("ap1_power_on"), "ap1_radio_missing") \
        .groupBy("ap1_power_on", "ap1_radio_missing").count().show()

    df_ap_coverage_stats.select( "ap1_max_tx_power", "ap2_max_tx_power").summary().show()

    df_ap_coverage_stats.filter(F.col("org_id").isNull()).select("org_id", "site_id").groupBy("org_id", "site_id").count().show()

    df_ap_coverage_stats.count()

    df_ap_coverage_stats.select("ap1_avg_nclients", "ap1_util_ap", "ap1_error_rate", "ap1_max_power").summary().show()


    select_org = "22f1cc2d-ea8a-47ea-b4c0-689a86a0bedf"  # Target CORPORATIONFRI
    df_ap_coverage_stats_org = df_ap_coverage_stats.filter(F.col("org_id")==select_org)

    # df_ap_coverage_stats_org = df_ap_coverage_stats.filter()
    return df_ap_coverage_stats


def check_coverage_candidates():
    from pyspark.sql import functions as F
    env = "production"
    # env = "staging"

    # s3_gs_bucket='s3://mist-aggregated-stats-{env}/ap-coverage-test/event_data_{env}/dt=2020-12-01/hr=*'.format(env=env)
    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_candidates/dt=2021-02-2*/hr=*/'

    df_events = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_gs_bucket)
    df_events.printSchema()
    print("df_events", df_events.count())

    df_filter = df_events.filter('avg_nclients>2.0 and ap_combined_score>0.5 and sle_coverage < 0.6 and'
                                 '(accomplices > 1 or off_neighbors>0)')
    # df_filter= df_events #.filter('avg_nclients>2.0')
    print("df_filter", df_filter.count())
    # df_events.count()

    df_events.select("ap_id").agg(F.countDistinct("ap_id")).show()
    df_events.select("ap_id").agg(F.count("ap_id").desc()).show()

    df_events.select("site_id", "ap_id").groupBy("ap_id").count().count()

    df_events.select("avg_nclients", "sle_coverage", "max_tx_power",  "ap_coverage_score", "avg_error_rate",
                     "ap_combined_score", "strong_neighbors",
                     "off_neighbors",
                     F.col("off_neighbors")/F.col("strong_neighbors")).summary().show()

    df_events.select("strong_neighbors", "off_neighbors", F.col("off_neighbors")/F.col("strong_neighbors")) \
            .summary("count", "mean", "stddev", "min", "50%", "75%", "90%", "95%", "max").show()

    # df_filter= df_events.filter('avg_nclients>2.0 and ap_combined_score>0.5 and sle_coverage < 0.6')
    # # df_filter= df_events #.filter('avg_nclients>2.0')
    # df_filter.count()

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

    df_sites.count()

    df_sites.show(5, truncate=False)


    df_site_aps= df_filter.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id", "ap_id").count().orderBy(F.col('count').desc())
    df_site_aps.count()
    df_site_aps.show(5, truncate=False)

    # df_filter= df_events.filter('avg_nclients>2.0 and ap_combined_score>0.3')
    # df_filter.count()


    select_org = "f5451dc6-aa80-4d1c-a49a-dede30b6d878"  # PetSmart
    select_org ="0992350f-e897-4719-8671-010a7e4ebf9c"  # MIT
    select_org = "bbb101eb-b62d-4fb1-8c3d-030c6db7e208"  # US-walmart
    # select_org = "22f1cc2d-ea8a-47ea-b4c0-689a86a0bedf"  # Target CORPORATIONFRI
    select_org = "c1cac1c4-1753-4dde-a065-e17a1c305c2d" # GAP
    select_org = "604411f1-4e45-4bed-9a69-cc37b247fdf9" # US- Sam's

    # select_org = "e390bbc0-c971-4e8e-9cd0-47ca740c7a84"  # Homedepot, GCP
    # df_selected_org= df_filter.filter(F.col("org_id")=="bbb101eb-b62d-4fb1-8c3d-030c6db7e208")
    # df_selected_org = df_filter.filter(F.col("org_id")=="0992350f-e897-4719-8671-010a7e4ebf9c") # MIT

    df_selected_org = df_filter.filter(F.col("org_id")==select_org)

    df_selected_org.count()

    df_selected_org.groupBy("org_id").agg(
        F.countDistinct("site_id").alias("count_sites"),
        F.countDistinct("ap_id").alias("count_aps")
    ).select("count_sites", "count_aps").orderBy(F.col("count_aps").desc()).show()

    df_selected_org.groupBy("site_id").agg(
        F.countDistinct("ap_id").alias("count_aps")
    ).select("site_id", "count_aps").orderBy(F.col("count_aps").desc()).show()

    # df_selected_org = df_filter.filter(F.col("site_id")=="6532c75a-c109-4f82-8270-36e199ca692e")

    cols_2 =  ["site_id", "ap_id", "band", 'sle_coverage', 'avg_nclients',
               'ap_coverage_score', "ap_combined_score",
               "strong_neighbors","accomplice_ids",
               "off_neighbor_ids"]

    df_selected_org.select(cols_2).orderBy(F.col("ap_combined_score").desc()).show(truncate=False)

    df_selected_org.filter(F.col("off_neighbors")>0).select(cols_2).show(truncate=False)

    cols = ["site_id", "ap_id", "band", 'sle_coverage', 'avg_nclients',
            'ap_coverage_score', "ap_combined_score",
            "strong_neighbors","accomplices",
            "off_neighbors"]

    df_selected_sites = df_selected_org.select(cols) \
        .groupBy("site_id", "band").agg(
        F.count("ap_id").alias("anomalies"),
        F.collect_set("ap_id").alias("impacted_aps"),
        F.countDistinct("ap_id").alias("count_aps"),
        F.avg('sle_coverage').alias("avg_sle_coverage"),
        F.max("ap_coverage_score").alias("worst_coverage_score"),
        F.max("ap_combined_score").alias("worst_combined_score"),
        F.sum("avg_nclients").alias("sum_nclients"),
        F.max("strong_neighbors").alias("strong_neighbors"),
        F.max("accomplices").alias("accomplices"),
        F.max("off_neighbors").alias("off_neighbors"),
        F.max("ap_combined_score").alias("max_severity")
    )
    df_selected_sites = df_selected_sites.orderBy(
        F.col('worst_combined_score').desc(),
        F.col("accomplices").desc()
                 )

    df_selected_sites.count()
    df_selected_sites.select("anomalies", "count_aps",
                             "avg_sle_coverage", "worst_coverage_score",
                             "worst_combined_score", "sum_nclients",
                             "strong_neighbors",
                             "accomplices", "off_neighbors").summary().show()

    df_selected_sites.filter(F.col("band")=="24").orderBy(F.col("worst_combined_score").desc())\
        .show(10, truncate=False)

    df_selected_sites.filter(F.col("band")=="5").orderBy(F.col("worst_combined_score").desc())\
        .show(10, truncate=False)


    df_selected_sites.filter(F.col("band")=="24").filter(F.col("accomplices")>0).orderBy(F.col("worst_combined_score").desc()) \
        .show(10, truncate=False)

    df_selected_sites.filter(F.col("band")=="5").filter(F.col("accomplices")>=0).orderBy(F.col("sum_nclients").desc()) \
        .show(10, truncate=False)

    df_selected_sites.filter("avg_sle_coverage < 0.5 and worset_combined_score > 0.7 and sum_nclients >30") \
        .show(20, truncate=False)

    alist= df_selected_sites.select('site_id').take(10)
    alist = [x.site_id for x in alist]
    for site_x in alist:
        get_site_info(site_x)



def check_es_events():
    from pyspark.sql import functions as F
    import json
    #
    # for i in range(10,17):
    #     s3_bucket = "s3://mist-aggregated-stats-production/entity_event/entity_event-production/dt=2020-12-{}/hr=*/APCoverageEvent_*.seq".format(i)
    #     print(s3_bucket)
    #     event_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    #     event_df = event_rdd.toDF()
    #     aps = event_df.select("ap_id").groupBy("ap_id").count().orderBy("count", descending=True).count()
    #     sites= event_df.select("site_id").groupBy("site_id").count().orderBy("count", descending=True).count()
    #     orgs= event_df.select("org_id").groupBy("org_id").count().orderBy("count", descending=True).count()
    #     print(i, orgs, sites, aps)

    s3_bucket = "s3://mist-aggregated-stats-production/entity_event/entity_event-production/" \
                "dt=2021-02-19/hr=*/APCoverageEvent_*.seq".format("production", "staging")

    print(s3_bucket)
    event_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    event_df = event_rdd.toDF()

    aps = event_df.select("ap_id").groupBy("ap_id").count().orderBy("count", descending=True).count()
    sites= event_df.select("site_id").groupBy("site_id").count().orderBy("count", descending=True).count()
    orgs= event_df.select("org_id").groupBy("org_id").count().orderBy("count", descending=True).count()
    print(orgs, sites, aps)

    select_org = "604411f1-4e45-4bed-9a69-cc37b247fdf9" # US- Sam's
    df_selected_org = event_df.filter(F.col("org_id")==select_org)

    event_site_ap_df= event_df.select("org_id", "site_id", "ap_id").groupBy("org_id", "site_id")\
        .agg(F.count("ap_id").alias("counts"),
             F.countDistinct("ap_id").alias("aps"))\
        .orderBy("counts", descending=True)

    event_df.filter(F.col("event_duration")==0).select("ap_id").groupBy("ap_id").count().orderBy("count", descending=True).count()
    event_df_filtered = event_df.filter(F.col("event_duration")>0)
    aps_filtered = event_df_filtered.select("ap_id").groupBy("ap_id").count().orderBy("count", descending=True).count()
    sites_filtered= event_df_filtered.select("site_id").groupBy("site_id").count().orderBy("count", descending=True).count()
    orgs_filtered= event_df_filtered.select("org_id").groupBy("org_id").count().orderBy("count", descending=True).count()
    print(orgs_filtered, sites_filtered, aps_filtered)

    # .select("ap_id").groupBy("ap_id").count().orderBy("count", descending=True).count()

    org_id = 'c1cac1c4-1753-4dde-a065-e17a1c305c2d'
    event_df_org = event_df.filter(F.col("org_id")==org_id)

    site_s7621 = 'a960792a-0299-479d-b637-a17df7be6da4'
    site_s6784= '3ce68a98-4362-435b-8d29-1e82eb914f53'
    site_s6778 = '8b2c2245-8c4d-42e6-b004-446b73461b52'
    event_df_org_site1 = event_df_org.filter(F.col("site_id")==site_s7621)
    event_df_org_site1.count()

    event_df_org_site2 = event_df_org.filter(F.col("site_id")==site_s6784)
    event_df_org_site2.count()

    event_df_org_site3 = event_df_org.filter(F.col("site_id")==site_s6778)
    event_df_org_site3.count()


def get_suggestion_events():

    import json
    from pyspark.sql import functions as F
    s3_bucket = "s3://mist-aggregated-stats-production/entity_suggestion/" \
                "entity_suggestion-production/dt=2021-02-19*/hr=*/*.seq".replace("production", "production")

    suggestions_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    suggestions_df = suggestions_rdd.toDF()
    # suggestions_df.select("entity_type", "suggestion", "category", "symptom", "impact_scope", "display_name")\
    #     .groupBy("entity_type", "suggestion", "category", "symptom", "impact_scope", "display_name")\
    #     .count()\
    #     .orderBy("entity_type", "category")\
    #     .show()
    suggestions_df_coverage = suggestions_df.filter(F.col("category")=="rf")
    suggestions_df_coverage.count()
    suggestions_df_coverage.select("duration", "severity").summary().show()



def get_site_info(site_id):
    import requests
    api_url = "http://papi-staging.mist.pvt/internal/sites/{}".format(site_id)
    # api_url = "http://papi-production.mist.pvt/internal/sites/88ea1e74-ab6d-49bc-9b79-bbae0ef37b81"
    # print(api_url)
    res = requests.get(api_url).json()
    print(site_id, res.get("name"), res.get("num_aps"))

def test_ap_has_neighbor():
    s3_bucket = "s3://mist-aggregated-stats-staging/aggregated-stats/ap_has_neighbors/dt=2020-11-21/hr=16/"

    df_ap_coverage_stats_neighbors = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats_neighbors.printSchema()

    df_ap_coverage_stats_neighbors.count()

    df_ap_coverage_stats_neighbors.filter(F.col("org_id").isNull()).select("org_id", "site_id").groupBy("org_id", "site_id").count().show()



def test_ap_has_no_neighbor():
    """ alll from staging??"""
    from pyspark.sql import functions as F
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_no_neighbors/dt=2021-02-04/hr=*/"

    df_ap_coverage_stats_no_neighbors = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats_no_neighbors.printSchema()

    df_ap_coverage_stats_no_neighbors.count()

    df_t = df_ap_coverage_stats_no_neighbors\
        .select("org_id", "site_id", "ap1","band")\
        .groupBy("org_id", "site_id", "ap1", "band")\
        .count().orderBy(F.col("count").desc())

    df_t.count()

    df_sites = df_t.select("org_id", "site_id", "band") \
        .groupBy("org_id", "site_id", "band") \
        .count().orderBy(F.col("count").desc())
    df_sites.select("band").groupBy("band").count().show()

    sites= df_sites.filter(F.col("band")=="5").select("site_id").limit(10).toPandas()
    sites = set(sites['site_id'])
    for site_id in sites :
        res = get_site_info(site_id)


def no_neighbor():
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-02-04/hr=20/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats.printSchema()

    df_t = df_ap_coverage_stats \
        .select("org_id", "site_id", "ap1_model", "ap1","ap2") \
        .groupBy("org_id", "site_id", "ap1_model", "ap1") \
        .agg( F.countDistinct("ap2").alias("neighbors"))
        # .orderBy(F.col("count").desc())

    df_sites_sum = df_t.select("org_id", "site_id", "ap1", "ap1_model", "neighbors") \
        .groupBy("org_id", "site_id", "ap1_model") \
        .agg(F.countDistinct("ap1").alias("aps"),
             F.sum("neighbors").alias("sum_neighbors")
             ).orderBy(F.col("sum_neighbors").asc())

    df_sites_sum.filter(~df_sites_sum.ap1_model.like("AP21-%"))\
        .filter("sum_neighbors <1 and aps>1")\
        .orderBy(F.col("aps").desc())\
        .show(truncate=False)

    df_t0 = df_t.filter(F.col("neighbors")<1)
    df_sites = df_t0.select("org_id", "site_id", "ap1_model") \
        .groupBy("org_id", "site_id", "ap1_model") \
        .count().orderBy(F.col("count").desc())

    # df_sites.show(20, truncate=False)

    df_sites_model = df_sites.filter(~df_sites.ap1_model.like("AP21-%"))
    print(df_sites.count(), df_sites_model.count())

    df_sites_model.show(10, truncate=False)

    site_id = "fdf1b743-a8cb-4227-a015-b3d587eb7caf"
    df_t0.filter(F.col("site_id") == site_id)
    ap_no_neighbors = df_t0.filter(F.col("site_id") == site_id).select("ap1").toPandas()['ap1']

    df_scan.filter(F.col("site")=="fdf1b743-a8cb-4227-a015-b3d587eb7caf")

def testtt():
    dt="dt=2021-02-04/hr=*/"
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/{}".format(dt)
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.printSchema()
    ap_list = ['5c5b352f7390',
             '5c5b352f73bd',
             '5c5b352f73ea',
             '5c5b352f750c',
             '5c5b352f7511',
             '5c5b358e000f',
             '5c5b358e005a',
             '5c5b358e00a0',
             '5c5b358e0172',
             '5c5b358e022b',
             '5c5b358e0235',
             '5c5b358e0d57',
             '5c5b358e0ded']
    ap_id = ap_list[0]
    df_ap_coverage_stats.filter(F.col("ap1")==ap_id)\
        .select("org_id", "site_id", "band", "ap1", "ap2", "rssi").show()

    s3_bucket ='s3://mist-aggregated-stats-production/aggregated-stats/top_1_time_epoch_by_site_ap_ap2_band/{}/*.csv'.format(dt)
    df_scan = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_bucket)
    # df_scan.printSchema()
    df_scan.filter(F.col("ap")==ap_id).orderBy(F.col("stats_agg_time").desc()).show()

# coverage-events
#
def test():
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, FloatType,\
        LongType, StructType, StructField, ArrayType, MapType, BooleanType
    import json
    from copy import deepcopy
    s3_bucket= 's3://mist-aggregated-stats-staging/event_generator/ap_last_seen/'
    # s3_bucket= 's3://mist-aggregated-stats-production/event_generator/dt=2020-11-19/hr=1*/*/'
    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    # ap_rdd.filter(lambda x: x.get("ap_id") == "5c5b352e2d46").count()

    #
    # aps = ["d420b0810db0", "d420b0810e5f", "d420b08116de", "d420b0810de2",
    #        "d420b08116d9", "d420b0810c4d", "d420b0811701", "d420b0810bc1", "d420b0810e41"]
    aps = ["d420b0810db0", "d420b0810e5f", "d420b08116de", "d420b0810de2",
           "d420b08116d9", "d420b0810c4d", "d420b0811701", "d420b0810bc1", "d420b0810e41"]

    ap1= ap_rdd.filter(lambda x: x.get("ap_id")==aps[0]).collect()
    [(x.get("terminator_timestamp"), x.get("radios")[1].get("max_tx_power") ) for x in ap1]

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
        StructField("max_tx_power", IntegerType()),
        StructField("radio_missing", BooleanType()),
        StructField("has_wlans", BooleanType())
    ])
    df_ap_radio = spark.createDataFrame(ap_rdd.flatMap(flat_radios), schema)

    df_ap_radio.printSchema()

    # df_ap.filter(~(df_ap.dev=="r2")).select("max_tx_power").summary().show()
    df_ap_radio = df_ap_radio.filter(df_ap_radio.dev!="r2")
    # df_ap_radio.write.parquet("s3://mist-aggregated-stats-production/event_generator/ap_radio_last_seen/")

    df_ap_radio.filter(F.col("site_id")=="b8a9571e-6724-4ccf-b635-07a1968fd795").select("ap_id", "model", "dev", "band", "bandwidth", "max_tx_power").orderBy(F.col("max_tx_power").asc()).show(80)


    # df_filter.filter("off_neighbors==9").select("org_id", "site_id", "band",  "ap_id", "model", "off_neighbor_ids").filter(F.col("site_id")=="b8a9571e-6724-4ccf-b635-07a1968fd795").show(truncate=False)
    aps = ["d420b0810db0", "d420b0810e5f", "d420b08116de", "d420b0810de2",
           "d420b08116d9", "d420b0810c4d", "d420b0811701", "d420b0810bc1", "d420b0810e41"]
    aps = ["d420b085fcb7|16", "5c5b35500442", "5c5b3550d417",
              "5c5b355004fb","d420b085fd5f","5c5b350e60b4","5c5b352e2cab"]

    # df_ap_radio_site = df_ap_radio.filter(F.col("site_id")=="b8a9571e-6724-4ccf-b635-07a1968fd795")

    df_ap_radio_site_aps = df_ap_radio.filter(df_ap_radio_site.ap_id.isin(aps))
    df_ap_radio_site_aps.select("ap_id", "model", "dev", "band", "bandwidth", "max_tx_power") \
        .orderBy(F.col("max_tx_power").asc()).show(80)




    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))

    aps = ["d420b0810db0", "d420b0810e5f", "d420b08116de", "d420b0810de2",
           "d420b08116d9", "d420b0810c4d", "d420b0811701", "d420b0810bc1", "d420b0810e41"]
    aps = ["5c5b35ae9cf4", "5c5b35aea195", "5c5b35aea1f4", "5c5b35aea9ec", "5c5b35aea384", "5c5b35aea1ea",
           "5c5b35aea203", "5c5b35aea9ab", "5c5b35aea1d6", "5c5b35aea410"]
    ap1 = aps[0]

    ap1_rdd = ap_rdd.filter(lambda x: x.get("ap_id")==aps[0]).collect()
    [(x.get("terminator_timestamp"), x.get("radios")[1].get("max_tx_power") ) for x in ap1_rdd]                                                                                                                                                                                                                        |org_id                              |site_id                             |band|ap_id       |model  |off_neighbor_ids                                                                                                              |


    df_ap_coverage_stats_test.filter(F.col("ap_id")==ap1) \
        .select("ap_id", "dev", "band", "stats_agg_time", "max_tx_power", "avg_nclients").show()


    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/dt=2020-11-21/hr=*/'
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats_test.printSchema()
    df_ap_coverage_stats_test.select("dev").groupBy("dev").count().show()


def test_radio_off():
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, FloatType, LongType, StructType, StructField, ArrayType, MapType
    import json
    from copy import deepcopy

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

    s3_bucket= 's3://mist-aggregated-stats-production/event_generator/ap_last_seen/'.replace("production", "production")
    # s3_bucket= 's3://mist-aggregated-stats-production/event_generator/dt=2021-02-17/hr=1*/*/'
    ap_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))

    df_ap_radio = spark.createDataFrame(ap_rdd.flatMap(flat_radios), schema)
    df_ap_radio.printSchema()

    df_ap_radio = df_ap_radio.filter(df_ap_radio.dev!="r2")
    df_ap_radio_off = df_ap_radio.filter(F.col("max_tx_power") == 0)
    df_ap_radio_off.count()

    df_ap_radio_off.select("org_id", "site_id", "ap_id", "band", "dev") \
        .groupBy("org_id", "site_id", "ap_id", "band", "dev").count().orderBy("count", descending=True).count()



    # radio-off after enrich-ap
    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/dt=2020-11-21/hr=16/'
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats_test.printSchema()
    df_ap_coverage_stats_test.select("dev").groupBy("dev").count().show()

    df_ap_coverage_stats_test_off = df_ap_coverage_stats_test.filter(F.col("max_tx_power")==0)

    df_ap_coverage_stats_test_off.count()

    df_ap_coverage_stats_test_radio_off = df_ap_coverage_stats_test_off.select("org_id", "site_id", "ap_id", "band", "dev")\
        .groupBy("org_id", "site_id", "ap_id", "band", "dev").count().orderBy("count", ascending=False)

    df_ap_coverage_stats_test_radio_off.count()



    df_ap_radio_off_pd =df_ap_radio_off.filter("band==5").toPandas()
    df_ap_coverage_stats_test_radio_off_pd = df_ap_coverage_stats_test_radio_off.filter("band==5").toPandas()


    list(df_ap_coverage_stats_test_radio_off_pd['ap_id'])
    list_ap_2 = list(df_ap_coverage_stats_test_radio_off_pd['ap_id'])
    list_ap_1 = list(df_ap_radio_off_pd["ap_id"])

    import numpy as np
    questional_ap = np.setdiff1d(list_ap_2,list_ap_1)





def check_coverage_anomaly_stats_test():
    from pyspark.sql import functions as F

    s3_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats_test/dt=2020-12-2*/hr=*/'
    df_ap_coverage_stats_test = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats_test.printSchema()

    org_id = 'c1cac1c4-1753-4dde-a065-e17a1c305c2d'  # GAP
    df_org = df_ap_coverage_stats_test.filter(F.col("org_id")==org_id).filter("avg_nclients>0")
    df_org.count()


    cols= ['site_id', 'ap_id', 'band', 'avg_nclients', 'max_tx_power',
           'sle_coverage', 'sle_coverage_anomaly_score',
           'coverage_anomaly_count','sticky_client_count']

    site_s7621 = 'a960792a-0299-479d-b637-a17df7be6da4'
    site_s6784= '3ce68a98-4362-435b-8d29-1e82eb914f53'
    site_s6778 = '8b2c2245-8c4d-42e6-b004-446b73461b52'

    df_org_site_s7621 = df_org.filter(F.col("site_id")==site_s7621)
    df_org_site_s7621.count()
    df_org_site_s7621.select(cols).groupBy("site_id", "ap_id", "band").agg(
        F.avg("avg_nclients").alias("avg_nclients"),
        F.min("max_tx_power").alias("min_max_tx_power"),
        F.min("sle_coverage").alias("min_sle_coverage"),
        F.max("sle_coverage_anomaly_score").alias("max_sle_coverage_dev"),
        F.count("coverage_anomaly_count").alias("coverage_anomaly_count")
    ).show(truncate=False)

    df_org_site_s6778 = df_org.filter(F.col("site_id")==site_s6778)
    df_org_site_s6778.count()
    # df_org_site_s6778.select(cols).show()
    df_org_site_s6778.select(cols).groupBy("site_id", "ap_id", "band").agg(
        F.avg("avg_nclients").alias("avg_nclients"),
        F.min("max_tx_power").alias("min_max_tx_power"),
        F.min("sle_coverage").alias("min_sle_coverage"),
        F.max("sle_coverage_anomaly_score").alias("max_sle_coverage_dev"),
        F.count("coverage_anomaly_count").alias("coverage_anomaly_count")
    ).show(truncate=False)


    df_org_site_s6784 = df_org.filter(F.col("site_id")==site_s6784)
    df_org_site_s6784.count()
    # df_org_site_s6784.select(cols).show()
    df_org_site_s6784.select(cols).groupBy("site_id", "ap_id", "band").agg(
        F.avg("avg_nclients").alias("avg_nclients"),
        F.min("max_tx_power").alias("min_max_tx_power"),
        F.min("sle_coverage").alias("min_sle_coverage"),
        F.max("sle_coverage_anomaly_score").alias("max_sle_coverage_dev"),
        F.count("coverage_anomaly_count").alias("coverage_anomaly_count")
    ).show(truncate=False)




    ##
    from pyspark.sql import functions as F
    from analytics.event_generator.ap_coverage_event import *
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-01-07/hr=*/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    df_ap_coverage_stats.printSchema()
    org_id = 'c1cac1c4-1753-4dde-a065-e17a1c305c2d'  # GAP
    df_ap_coverage_stats = df_ap_coverage_stats.filter(F.col("org_id")==org_id)


    site_s7621 = 'a960792a-0299-479d-b637-a17df7be6da4'
    site_s6784= '3ce68a98-4362-435b-8d29-1e82eb914f53'
    site_s6778 = '8b2c2245-8c4d-42e6-b004-446b73461b52'
    df_org_site_s6778 = df_ap_coverage_stats.filter(F.col("site_id")==site_s6778)
    df_org_site_s6778.count()

    df_org_site_s6784 = df_ap_coverage_stats.filter(F.col("site_id")==site_s6784)
    df_org_site_s6784.count()

    df_org_site_s7621 = df_ap_coverage_stats.filter(F.col("site_id")==site_s7621)
    df_org_site_s7621.count()

    features_df = df_org_site_s6778.filter(df_org_site_s6778.ap1_sle_coverage.isNotNull())\
        .groupBy("site_id", "band", "ap1") \
        .agg(
        F.avg("ap1_error_rate").alias("avg_error_rate"),
        F.max("ap1_max_tx_power").alias("max_tx_power"),
        F.min("stats_agg_time").alias("start_time"),
        F.avg("ap1_avg_nclients").alias("ap1_avg_nclients"),
        F.avg("ap1_sle_coverage").alias("ap1_sle_coverage"),
        F.avg("ap1_coverage_anomaly_count").alias("ap1_coverage_anomaly_count"),
        F.avg("ap1_sle_coverage_anomaly_score").alias("ap1_sle_coverage_anomaly_score"),
        F.max("ap1_sticky_uniq_client_count").alias("ap1_sticky_uniq_client_count"),
        F.countDistinct("ap2").alias("strong_neighbors"),
        F.collect_set("ap2").alias("strong_neighbor_ids"),
        F.countDistinct(neighbor_anomalies(F.col("ap2"),
                                           F.col("ap2_sle_coverage"),
                                           F.col("ap2_avg_nclients"))
                        ).alias("accompliances"),
        F.collect_set(neighbor_off(F.col("ap2"),
                                   F.col("ap2_max_tx_power"), F.col("ap2_radio_missing"), F.col("ap2_has_wlans"))
                      ).alias("off_neighbor_ids")
    )
    # coverage anomaly score, extracting from AP's
    features_df = features_df.withColumn("ap1_coverage_score",
                                         ap_coverage_score(F.col("ap1_avg_nclients"),
                                                           F.col("ap1_sle_coverage"),
                                                           F.col("ap1_sle_coverage_anomaly_score"),
                                                           F.col("ap1_coverage_anomaly_count"),
                                                           F.col("ap1_sticky_uniq_client_count")
                                                           )
                                         ) \
        .withColumn("off_neighbors", F.size("off_neighbor_ids"))
    features_df = features_df.withColumn("ap1_combined_score",
                                         combined_score(
                                             F.col("ap1_coverage_score"),
                                             F.col("strong_neighbors"),
                                             F.col("accompliances")
                                         )
                                         )

    features_df.select("site_id", "ap1", "band", "avg_error_rate", "ap1_avg_nclients", "ap1_sle_coverage",
                               "ap1_coverage_anomaly_count", "ap1_sle_coverage_anomaly_score",
                               "ap1_sticky_uniq_client_count",
                               "strong_neighbors",
                               "strong_neighbor_ids", "accompliances",
                       "off_neighbor_ids", "ap1_combined_score" )\
        .show()


    features_df = df_org_site_s6784.filter(df_org_site_s6778.ap1_sle_coverage.isNotNull()) \
        .groupBy("site_id", "band", "ap1") \
        .agg(
        F.avg("ap1_error_rate").alias("avg_error_rate"),
        F.max("ap1_max_tx_power").alias("max_tx_power"),
        F.min("stats_agg_time").alias("start_time"),
        F.avg("ap1_avg_nclients").alias("ap1_avg_nclients"),
        F.avg("ap1_sle_coverage").alias("ap1_sle_coverage"),
        F.avg("ap1_coverage_anomaly_count").alias("ap1_coverage_anomaly_count"),
        F.avg("ap1_sle_coverage_anomaly_score").alias("ap1_sle_coverage_anomaly_score"),
        F.max("ap1_sticky_uniq_client_count").alias("ap1_sticky_uniq_client_count"),
        F.countDistinct("ap2").alias("strong_neighbors"),
        F.collect_set("ap2").alias("strong_neighbor_ids"),
        F.countDistinct(neighbor_anomalies(F.col("ap2"),
                                           F.col("ap2_sle_coverage"),
                                           F.col("ap2_avg_nclients"))
                        ).alias("accompliances"),
        F.collect_set(neighbor_off(F.col("ap2"),
                                   F.col("ap2_max_tx_power"), F.col("ap2_radio_missing"), F.col("ap2_has_wlans"))
                      ).alias("off_neighbor_ids")
    )

    features_df = features_df.withColumn("ap1_combined_score",
                                         combined_score(
                                             F.col("ap1_coverage_score"),
                                             F.col("strong_neighbors"),
                                             F.col("accompliances")
                                         )
                                         )

    features_df.select("site_id", "ap1", "band", "avg_error_rate", "ap1_avg_nclients", "ap1_sle_coverage",
                               "ap1_coverage_anomaly_count", "ap1_sle_coverage_anomaly_score",
                               "ap1_sticky_uniq_client_count",
                               "strong_neighbors",
                               "strong_neighbor_ids", "accompliances", "off_neighbor_ids" )


    env = "production"
    # s3_gs_bucket='s3://mist-aggregated-stats-{env}/ap-coverage-test/event_data_{env}/dt=2020-12-01/hr=*'.format(env=env)
    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_candidates/dt=2021-01-14/hr=1*/'

    df_events = spark.read.format("csv") \
             .option("header", "true").option("inferSchema", "true") \
             .load(s3_gs_bucket)
    df_events.printSchema()


    cols= ['site_id', 'ap_id', 'band', 'avg_nclients', 'max_tx_power',
           'sle_coverage',  'accompliances', 'off_neighbors', 'ap_combined_score'
           ]

    df_events_filter = df_events.filter(F.col("org_id")==org_id).filter("avg_nclients > 0")
    df_events_site_s7621 = df_events_filter.filter(F.col("site_id")==site_s7621)
    df_events_site_s7621.count()

    df_events_site_s7621.select(cols).groupBy("site_id", "ap_id", "band").agg(
        F.avg("avg_nclients").alias("avg_nclients"),
        F.min("max_tx_power").alias("min_max_tx_power"),
        F.min("sle_coverage").alias("min_sle_coverage"),
        F.max("ap_combined_score").alias("ap_combined_score"),
        F.count("accompliances").alias("accompliances"),
        F.count("off_neighbors").alias("off_neighbors")
    ).show(truncate=False)

    df_events_site_s6778 = df_events_filter.filter(F.col("site_id")==site_s6778)
    df_events_site_s6778.count()

    df_events_site_s6784 = df_events_filter.filter(F.col("site_id")==site_s6784)
    df_events_site_s6784.count()



def test_sams_6355():
    from pyspark.sql import functions as F
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_coverage_stats/dt=2021-02-12/hr=*/"
    df_ap_coverage_stats = spark.read.parquet(s3_bucket)
    # df_ap_coverage_stats.count()
    df_ap_coverage_stats.printSchema()

    site_id = "a18a2ef5-e266-4b5a-b2ce-7a5822deb9de"   # Sam's 6355

    df_site = df_ap_coverage_stats.filter(F.col("site_id")==site_id)

    cols = ["ap1", "band", "ap1_avg_nclients", "ap1_util_ap", "ap1_sle_coverage", "ap1_sle_coverage_anomaly_score",
            "ap1_error_rate",
            "ap2", "rssi", "ap2_avg_nclients", "ap2_util_ap", "ap2_sle_coverage", "ap2_sle_coverage_anomaly_score",

            ]

    df_site.select(cols).show()



