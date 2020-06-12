#
#  spark-submit --deploy-mode cluster --master yarn ./report.py --report_date 2020-06-01
#
#  Create action and it's recovery report
#  results will be saved in to mist-aggregated-stats-production/automated_action_report folder for each report day
#  It will create 3 reports:
#  raw_data: each action and the event triggered this action
#  stats: each action name total count and recovery count
#  If there's any reboot_ap action, it provides report about each rebooted AP 
#
#


import argparse
import json
import logging
import math
import random
import time
from collections import Counter
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row


REPORT_BUCKET = 'mist-aggregated-stats-production'


def setup_spark_configs():
    try:
        ## Spark Submit Stuff
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('-report_date', '--report_date', type=str,
                            help='Report date in YYYY-MM-DD format')
        args = parser.parse_args()

        report_date = validate(args.report_date)
        conf = SparkConf().setAppName(f"Entity Action Report For Date {report_date}".format(report_date))
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)

        return spark, report_date
    except Exception as e:
        print('what is the error {}'.format(e))
        print("Running this as a spark shell, filling date variable with yesterdays date")
        d = datetime.today() - timedelta(days=1)
        date = d.strftime('%Y-%m-%d')
        conf = SparkConf().setAppName(f"Entity Action Report For Date {report_date}")
    finally:
        print(f"Running the action report for {report_date}")


def validate(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def flatten_action_data(base_action_rdd):
    """
    :param base_action_rdd:
    :return: dataframe ['action', 'ap_id', 'org_name', 'event_name', 'event_type']
    """
    # and r.get('action') != 'do_nothing')\
    # and r.get('action') = 'reboot_ap')\
    action_ap_rdd = base_action_rdd.map(lambda r: json.loads(r[1])) \
        .map(lambda r: r[0] if type(r) is list else r) \
        .filter(lambda r: r.get('who') in ['MarvisImmediateAction', 'MarvisAutoAction']
                          and r.get('action') != 'do_nothing') \
        .repartition(5) \
        .persist()

    action_aps = action_ap_rdd.map(lambda r: r['display_entity_id']).collect()
    last_seen_list = last_seen_ap(action_ap_rdd.context, action_aps)
    action_status_rdd = action_ap_rdd \
        .mapPartitions(lambda action_iter: ap_action_recover_status(action_iter, last_seen_list)) \
        .persist()

    stat = [{'category': 'total_action_count', 'count': action_status_rdd.count()}]
    action_counter = Counter(
        action_status_rdd.map(lambda r: (r['action'], r.get('event_recovered', 'unknown'))).collect())

    for k, c in action_counter.items():
        stat.append({'category': k[0] + (' recovered' if k[1] else ' not recovered'),
                     'count': c})

    reboot_rdd = action_status_rdd \
        .filter(lambda r: r['action'] == 'reboot_ap')

    if reboot_rdd.count() > 0:

        reboot_df = reboot_rdd.map(lambda r:
                                   Row(org_id=r['org_id'],
                                       ap_id=r['ap_id'],
                                       target=r['target'],
                                       action=r['action'],
                                       action_time=r['when'],
                                       event_name=r['event_name'],
                                       event_recovered=r.get('event_recovered', 'unknown'),
                                       ap_is_up=r['ap_up'])) \
            .toDF()
    else:
        reboot_df = None

    return action_status_rdd, reboot_df, stat


def join_action_df_w_org_df(df_with_event_name, df_org_id_name_map):
    df_action_w_org = df_with_event_name \
        .join(df_org_id_name_map, df_with_event_name.org_id == df_org_id_name_map.id, 'left') \
        .drop('created_time', 'modified_time', 'msp_id', 'tzoffset', 'secret', 'loaded_date', 'id', 'org_id')

    return df_action_w_org


def last_seen_ap(sc, aps):
    ap_last_seen_rdd = sc.sequenceFile('s3://{}/event_generator/ap_last_seen'.format(REPORT_BUCKET)).map(
        lambda r: json.loads(r[1]))

    return ap_last_seen_rdd.filter(lambda r: r['ap_id'] in aps).map(lambda r: (r['ap_id'], r)).collect()


def ap_action_recover_status(action_iter, last_seen_list):
    """
    Is AP up and is the stats recovered?
    :return:
    """
    last_seen_data = dict(last_seen_list)

    for action in action_iter:

        ap_id = action.get('display_entity_id')
        action_time_sec = to_seconds(action.get('when'))

        act = {'org_id': action.get('org_id'),
               'ap_id': ap_id,
               'target': '_'.join(action.get('target_id').split('_')[2:]),
               'event_name': ' & '.join([id.split('&')[1] for id in action['event_id']]),
               'when': action.get('when'),
               'action': action.get('action'),
               }
        last_seen = last_seen_data.get(ap_id)

        if last_seen:
            act['latest_uptime'] = last_seen.get('uptime')
            act['last_seen'] = last_seen.get('terminator_timestamp')
            act['ap_up'] = last_seen.get('terminator_timestamp') > action.get('when')

        event_ids = action.get('event_id')
        events = query_events(event_ids)

        if events:
            end_times = [to_seconds(e.get('end_time')) < (action_time_sec + 600) for e in events]
            act['latest_event_end_time'] = max([to_milliseconds(e.get('end_time')) for e in events])
            act['event_recovered'] = any(end_times)
            act['event_ids'] = event_ids

        yield act


def query_events(event_ids, env='production'):
    s = Search(index='entity_event*', using=get_es(env=env))

    query_list = [Q('terms', row_key=event_ids)]

    q = Q('bool', must=query_list)
    s = s.query(q)  # sort by descending order

    print(s.to_dict())
    resp = s.execute()
    print(resp)
    if resp and resp.hits.total.value > 0:
        return [r.to_dict() for r in resp.hits]
    else:
        return []


def get_es(env='production'):
    es_hosts = ['es7-access-{}-{}.mist.pvt'.format('%03d' % d, env) for d in range(0, 3)]
    random.shuffle(es_hosts)
    es_port = '9200'

    if es_hosts:
        host_configs = [{'host': hh, 'port': es_port} for hh in es_hosts]
        return Elasticsearch(host_configs)
    else:
        return None


def to_milliseconds(long_ts):
    """
    convert given timestamp to seconds format
    :param long_ts:
    :return:
    """
    if long_ts <= 0:
        return long_ts

    dd = int(math.log10(long_ts)) + 1
    if dd < 13:
        return int(long_ts * 1000)
    elif 13 <= dd < 16:
        return int(long_ts)
    elif 16 <= dd:
        return int(long_ts / 1000)


def to_seconds(long_ts):
    """
    convert given timestamp to seconds format
    :param long_ts:
    :return:
    """
    if long_ts <= 0:
        return long_ts

    dd = int(math.log10(long_ts)) + 1
    if dd < 13:
        return long_ts
    elif 13 <= dd < 16:
        return int(long_ts / 1000)
    elif 16 <= dd:
        return int(long_ts / 10 ** 6)


def save_file_to_s3_action_reports(action_dataframe, report_date):
    """
    :param dataframe: final action dataframe ready to save
    :param date: format
    :return:
    """
    try:
        action_dataframe.coalesce(1) \
            .write \
            .option("header", "true") \
            .option("sep", ",") \
            .mode("overwrite") \
            .csv("s3://{}/automated_action_report/{}/".format(REPORT_BUCKET, report_date))
    except IOError as err:
        print("I/O error: {0}".format(err))


if __name__ == '__main__':
    """
    import the module we want to run.  this can either be a py file or a package, but should be somewhere under
    local file.jobs.
    *** it also needs to implement a run_jobs method*** 
    """
    start = time.time()

    spark, report_date = setup_spark_configs()

    print('***** Reporting date: {}'.format(report_date))
    rdd_action_data = spark.sparkContext.sequenceFile(
        's3://mist-aggregated-stats-production/entity_action/entity_action-production/dt={}/hr=*/*.seq'
            .format(report_date))

    df_org_id_name_map = spark.read.load("s3://mist-secorapp-production/dimension/org/part-*.parquet") \
        .withColumnRenamed("name", "org_name")

    action_rdd, reboot_df, stats = flatten_action_data(rdd_action_data)

    if reboot_df is not None:
        action_w_org_df = join_action_df_w_org_df(reboot_df, df_org_id_name_map)
        save_file_to_s3_action_reports(action_w_org_df, report_date)

    spark.sparkContext.parallelize(stats).toDF().repartition(1).write \
        .option("header", "true") \
        .option("sep", ",") \
        .mode("overwrite") \
        .csv('s3://{}/automated_action_report/{}/stats'.format(REPORT_BUCKET, report_date))

    if action_rdd.count() > 0:
        action_rdd \
            .map(lambda r: ('', json.dumps(r))) \
            .repartition(1) \
            .saveAsSequenceFile('s3://{}/automated_action_report/{}/raw_data'.format(REPORT_BUCKET, report_date))

    end = time.time()
    logging.info("***** Execution of job took {} seconds".format(end - start))

