import json
from pyspark.sql import functions as F


def test_aggregator():

    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    start_time = current_epoch_seconds()//(3600*24) * (3600*24) - (3600*24) * 1
    end_time = start_time + 3600
    # job = stats_aggregator_job(start_time, end_time, "client-events",  spark, "production")

    job = stats_aggregator_job(start_time, end_time, "ap",  spark, "production")
    job.execute()


def test_test():
    from analytics.utils.time_util import current_epoch_seconds
    from analytics.jobs.utils import *
    from datetime import datetime, timedelta
    # single test run:
    print("<=== START NEXT AGGREGATION ===>")
    k=0
    k_off=10
    print(str(k))
    print(str((k_off - k)))
    start_time = current_epoch_seconds() - 3600 * (k_off-k)
    end_time = start_time + 600 * 1
    print(datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))
    job = start_debug_job('ap-stats-analytics', start_time, end_time, spark=spark, test_env='production', debug_mode=True)
    data = run_category_transform(job, 'ap21_radio')
    gen = get_event_generator(job, 'ap21_radio', 'AP21FailureEvent')
    event_rdd = gen.generate_event(data, spark)

