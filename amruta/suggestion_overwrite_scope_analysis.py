from elasticsearch.helpers import streaming_bulk
import argparse
import json
import logging
import random
import time

from elasticsearch import Elasticsearch
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from collections import Counter
from io import StringIO # python3; python2: BytesIO
import boto3

def setup_spark_configs():
    try:
        ## Spark Submit Stuff
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('-date', '--date', type=str,
                            help='Date in YYYY-MM-DD format')
        parser.add_argument('-suggestion', '--suggestion', type=str,
                            help='Enter suggestion to analyze eg. test_replace_cable')
        parser.add_argument('-env', '--env', type=str,
                            help='staing/production')
        args = parser.parse_args()

        date = args.date
        suggestion = args.suggestion
        env = args.env

        conf = SparkConf().setAppName(f"Entity Suggestion For Date {args.date}")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)
        return spark, date, suggestion, env
    except Exception as e:
        print('Error is {}'.format(e))
    finally:
        print(f"Running the entity suggestion overwrite analysis for {args.date}")


def process_details(x):
    port_id_list = []
    impacted_tuple = x.get("details").get("impacted_tuple")
    for temp in impacted_tuple:
        port_id = temp.get("port_id")
        port_id_list.append(port_id)

    x["port_id_list"] = sorted(port_id_list)
    return x

if __name__ == '__main__':
    """
    import the module we want to run.  this can either be a py file or a package, but should be somewhere under
    local file.jobs.
    *** it also needs to implement a run_jobs method*** 
    """
    start = time.time()

    spark, date, suggestion, env = setup_spark_configs()

    print("Date:" + date)
    print("suggestion:" + suggestion)
    print("Env:" + env)

    print('***** Analysis date: {}'.format(date))
    suggestion_raw = spark.sparkContext.sequenceFile(
        's3://mist-aggregated-stats-{}/entity_suggestion/entity_suggestion-{}/dt={}/hr=*/EntitySuggestion_*.seq'
            .format(env, env, date))

    raw_data = suggestion_raw.map(lambda x: json.loads(x[1])).filter(
        lambda x: x.get("suggestion") == suggestion and x.get("entity_type") == "switch")

    raw_data_list = raw_data.map(lambda x: process_details(x))

    data = raw_data_list.collect()
    df_data = pd.json_normalize(data)
    df_data['port_id_list_str'] = df_data.port_id_list.apply(lambda x: ', '.join([str(i) for i in x]))

    non_duplicate_df = df_data.drop_duplicates(subset='row_key', keep="first")

    print('Total number of unique row_key for suggestion {} is {} '.format(suggestion, non_duplicate_df.shape[0]))

    count_series = df_data.groupby("row_key")['port_id_list_str'].nunique()
    count_df = pd.DataFrame({'row_key': count_series.index, 'count': count_series.values})

    row_keys_with_issue = count_df[count_df['count'] > 1]

    print('Total number of row_key WITH ISSUES for suggestion {} is {} '.format(suggestion, row_keys_with_issue.shape[0]))

    data_to_analyze = df_data[df_data.row_key.isin(row_keys_with_issue.row_key)].sort_values('row_key')

    #save data to s3 for further analysis

    data_to_analyze['details.impacted_tuple'] = data_to_analyze['details.impacted_tuple'].astype(str)

    file_name = "amruta/" + suggestion + "_" + date + ".csv"
    bucket = 'mist-data-science-dev'
    csv_buffer = StringIO()
    data_to_analyze.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())

    print('Data is uploaded on s3: {}'.format(file_name))

#spark-submit amruta/suggestion_overwrite_scope_analysis.py --date 2022-10-07 --env production --suggestion check_port