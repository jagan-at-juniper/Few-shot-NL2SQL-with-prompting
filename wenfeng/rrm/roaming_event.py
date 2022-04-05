
def test_detection():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 3
    end_time = start_time + 3600

    job = start_debug_job('site_roaming_detection', start_time, end_time, spark=spark, test_env='production', debug_mode=True)

    data = run_category_transform(job, 'all')
    # print("data", data.count())

    gen = get_event_generator(job, 'all', 'SiteRoamingEvent')
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



def test_client_event():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 3
    end_time = start_time + 3600

    job = start_debug_job('roaming-client-events', start_time, end_time, spark=spark, test_env='staging', debug_mode=True)

    data = run_category_transform(job, 'all')
    # print("data", data.count())

    gen = get_event_generator(job, 'all', 'RoamingClientEvent')
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


def test_roaming():
    from pyspark.sql import functions as F
    import time

    from analytics.jobs.utils import *


    current_epoch_seconds = int(time.time())
    start_time = current_epoch_seconds - 7200
    end_time = start_time + 3600

    # job = start_debug_job('roaming_client_event', start_time, end_time, test_env='staging')  #7.31 1pm PST
    job = start_debug_job('roaming-client-events', start_time, end_time, test_env='staging')  #7.31 1pm PST
    data_rdd = run_category_transform(job, 'all').persist()
    data_rdd.count()

    gen = get_event_generator(job, 'all', 'RoamingClientEvent')
    event_rdd = gen.generate_event(data_rdd, spark)
    event_df = event_rdd.toDF()
    event_df.count()
