import argparse
import csv
import os
import math
import pandas as pd

import boto3

from elasticsearch import Elasticsearch
from datetime import datetime, timezone, timedelta

ENV = os.environ.get('ENV', 'staging')
ES7_HOST = ['es715-access-000-{}.mist.pvt'.format(ENV),
            'es715-access-001-{}.mist.pvt'.format(ENV),
            'es715-access-002-{}.mist.pvt'.format(ENV)
            ]
INDEX_NAME = 'entity_action_test'

# FILE SYSTEM CONFIG
SITE_WINDOW_BUCKET = "mist-aggregated-stats-{}".format(ENV)
SITE_WINDOW_PATH = "aggregated-stats/site_maintenance_window_v2/day_name={DAY}"
SITE_WINDOW_LOCAL_PATH = "/tmp/site_maintenance_window.csv"


class S3StorageClient:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.paginator = self.s3_client.get_paginator('list_objects_v2')

    def is_path_exists(self, fs_bucket, fs_path):
        resp = self.s3_client.list_objects_v2(Bucket=fs_bucket,
                                              Prefix=fs_path)
        return resp.get('KeyCount') > 0

    def download_file(self, fs_bucket, fs_path, local_file_path):
        resp = self.s3_client.download_file(fs_bucket, fs_path, local_file_path)
        return parse_fs_status(resp)

    def compose_file_uri(self, fs_bucket, fs_path):
        return 's3://{}/{}'.format(fs_bucket, fs_path)

    def get_fs_prefix(self):
        return "s3"

    def list_file(self, fs_bucket, fs_path, complete_path=True, recursive=True):
        """

        :param fs_bucket:
        :param fs_path:
        :param complete_path:
        :param recursive:
        :return:
        """
        if recursive:
            delimiter = ''
            resp_key = 'Contents'
            name_key = 'Key'
        else:
            delimiter = '/'
            resp_key = 'CommonPrefixes'
            name_key = 'Prefix'

        pages = self.paginator.paginate(Bucket=fs_bucket,
                                        Prefix=fs_path,
                                        Delimiter=delimiter)

        resp = []
        for page in pages:
            if page.get('KeyCount', 0):
                if complete_path:
                    resp += map(lambda r: r.get(name_key), page.get(resp_key))
                else:
                    resp += map(lambda r: r.get(name_key).split('/')[-2]
                    if r.get(name_key).endswith('/')
                    else r.get(name_key).split('/')[-1],
                                page.get(resp_key))
        return resp


def parse_fs_status(resp):
    if resp and resp.get('ResponseMetadata') and resp.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        return True
    else:
        return False


def convert_date_to_utc_datetime(date_str):
    try:
        date_format = '%Y-%m-%d'
        datetime_str = datetime.strptime(date_str, date_format)
        return datetime_str.replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError("Incorrect date format. Use YYYY-MM-DD.")


def get_es():
    return Elasticsearch([{'host': es, 'port': '9200'} for es in ES7_HOST])


def to_seconds(long_ts):
    """
    Convert given timestamp to seconds
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


def to_utc_datetime(timestamp):
    return datetime.utcfromtimestamp(to_seconds(timestamp))


def get_start_and_end_time(date):
    start_datetime = convert_date_to_utc_datetime(date)
    start_time = start_datetime.timestamp()
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    end_time = end_datetime.timestamp()
    return to_milliseconds(start_time), to_milliseconds(end_time)


def ts_to_monthly_index_partition(long_ts):
    """
    Convert the given timestamp to ES monthly time partitioned suffix
    eg: for 1547658000450571, it returns 201901
    :param long_ts:
    :return:
    """
    dt = to_utc_datetime(long_ts)
    return dt.strftime('%Y%m')


def ts_to_daily_index_partition(long_ts):
    """
    Convert the given timestamp to ES daily time partitioned suffix
    :param long_ts:
    :return:
    """
    dt = to_utc_datetime(long_ts)
    return dt.strftime('%Y%m%d')


def ts_to_hourly_index_partition(long_ts):
    """
    Convert the given timestamp to ES hourly time partitioned suffix
    eg: for 1547658000450571, it returns 2019011617
    :param long_ts:
    :return:
    """
    dt = to_utc_datetime(long_ts)
    return dt.strftime('%Y%m%d%H')


def get_index(data_source, start_ts, end_ts, partition='month'):
    index_str_set = set()

    if partition == 'month':
        index_str_set.add(ts_to_monthly_index_partition(start_ts))
        index_str_set.add(ts_to_monthly_index_partition(end_ts))
        return ','.join(['{}_{}*'.format(data_source, s) for s in list(index_str_set)])

    elif partition == 'day':
        index_str_set.add(ts_to_daily_index_partition(start_ts))
        index_str_set.add(ts_to_daily_index_partition(end_ts))
        return ','.join(['{}_{}*'.format(data_source, s) for s in list(index_str_set)])

    elif partition == 'hour':
        index_str_set.add(ts_to_hourly_index_partition(start_ts))
        index_str_set.add(ts_to_hourly_index_partition(end_ts))
        return ','.join(['{}_{}*'.format(data_source, s) for s in list(index_str_set)])

    else:
        return '{}'.format(data_source)


def get_hourly_site_count_from_es(end_time, start_time):
    index = get_index(INDEX_NAME, start_time, end_time)
    es_client = get_es()
    response = es_client.search(
        index=index,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "when": {
                                    "gte": start_time
                                }
                            }
                        },
                        {
                            "match": {
                                "action": "synthetic_test"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "hour": {
                    "date_histogram": {
                        "field": "when",
                        "interval": "hour"
                    },
                    "aggs": {
                        "site_count": {
                            "cardinality": {
                                "field": "site_id"
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    "when": {
                        "order": "desc"
                    }
                }
            ]
        }
    )
    hourly_test_count = dict()
    hour = 0
    for bucket in response['aggregations']['hour']['buckets']:
        hourly_test_count[hour] = (bucket.get('site_count', {}).get('value', 0), bucket.get('doc_count', {}))
        hour += 1

    return hourly_test_count


def download_file_with_extension_from_cloud(bucket, folder, extension, local_path):
    """
    Download the file with the given extension from the cloud storage and save it into a local directory
    :return:
    """
    found = False
    try:
        fs_client = S3StorageClient()

        if fs_client.is_path_exists(bucket, folder):
            file_list = fs_client.list_file(bucket, folder)

            for file in file_list:
                if file.endswith(extension):
                    found = True
                    fs_client.download_file(bucket, file, local_path)
                    break

            if not found:
                raise FileNotFoundError

    except Exception as e:
        print("Error in downloading the {} file: {}".format(extension, e))
    return found


def get_current_datetime():
    return datetime.now()


def get_day_string_from_dt(datetime_obj=None):
    """
    Return the day string from the datetime_obj
    If datetime_obj is None, return current UTC day
    eg: return "Mon"
    :return:
    """
    if not datetime_obj or not isinstance(datetime_obj, datetime):
        datetime_obj = get_current_datetime()
    return datetime_obj.strftime("%a")


def download_maintenance_window(day):
    """
    Download the test config file from the cloud storage and save it into a local directory
    :return:

    Args:
        datetime_obj:

    """
    test_config_cloud_dir = SITE_WINDOW_PATH.format(DAY=day)  # noqa
    is_downloaded = download_file_with_extension_from_cloud(SITE_WINDOW_BUCKET,
                                                            test_config_cloud_dir,
                                                            'csv',
                                                            SITE_WINDOW_LOCAL_PATH)
    return is_downloaded


def convert_df_to_dict(df_object, index_key=''):
    """
    Convert the given df to a dict with index_key values as dict keys
    Example:
    For a DF,

        site_id	hour
    0	site1	3
    1	site2	1
    2	site3	4
    3	site4	2

    if index_key =='site_id', we return

    {
        'hour': {
            'site1': 3,
            'site2': 1,
            'site3': 4,
            'site4': 2
        }
    }

    If index key is not specified, treat first column as index key
    :param df_object:
    :param index_key:
    :return:
    """
    if not isinstance(df_object, pd.DataFrame):
        return {}
    if not index_key:
        return df_object.to_dict()
    return df_object.set_index(index_key).to_dict()


def get_test_config(day, cond=''):
    """
    Return a dict for the test config
    {
        'site1': 3,
        'site2': 1,
        'site3': 4,
        'site4': 2
    }
    :param day:
    :param cond:
    :return:
    """
    try:
        # Download site config if it does not exist locally
        if not os.path.isfile(SITE_WINDOW_LOCAL_PATH):
            download_maintenance_window(day)

        data = pd.read_csv(SITE_WINDOW_LOCAL_PATH)
        data.drop_duplicates(subset='site_id', keep="first")
        if cond:
            data = data.query(cond)
        return convert_df_to_dict(data, 'site_id').get('hour', {})

    except Exception as ex:
        print('Unable to load content from test config : {},\n Error: {}'.format(SITE_WINDOW_LOCAL_PATH, ex))
    return {}


def validate_queued_tests(date):
    start_time, end_time = get_start_and_end_time(date)
    hourly_test_count = get_hourly_site_count_from_es(end_time, start_time)
    current_day = get_day_string_from_dt(convert_date_to_utc_datetime(date))
    result = list()
    for hour, count in hourly_test_count.items():
        test_config = get_test_config(current_day, cond='hour=={}'.format(hour))
        result.append((hour, len(test_config), count[0], count[1], len(test_config) - count[0]))
    with open('tests_queued.csv', 'w', newline='') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(('Hour', 'Sites in Window', 'Sites Queued', 'APs Queued', 'Scope Not Found'))
        writer.writerows(result)


def main():
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('-date',
                           required=True,
                           help='date (YYYY-MM-DD)')

    args = my_parser.parse_args()
    date = args.date
    validate_queued_tests(date)


if __name__ == '__main__':
    main()
