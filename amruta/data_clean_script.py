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


def setup_spark_configs():
    try:
        ## Spark Submit Stuff
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('-date', '--date', type=str,
                            help='Date in YYYY-MM-DD format')
        parser.add_argument('-hr', '--hr', type=str,
                            help='Hour in HR format eg. 18,03 etc')
        parser.add_argument('-env', '--env', type=str,
                            help='staing/production')
        args = parser.parse_args()

        date = args.date
        hour = args.hr
        env = args.env

        conf = SparkConf().setAppName(f"Entity Suggestion For Date {args.date}")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)
        return spark, date, hour, env
    except Exception as e:
        print('Error is {}'.format(e))
    finally:
        print(f"Running the entity suggestion cleaning for {args.date}")

def delete_garbage_suggestions(suggestion_ids, env='staging'):
    doc_type = '_doc'
    action_generator = []
    es_conn = get_es(env)
    index_name = "entity_suggestion_202202"
    final_res = 0

    for data in suggestion_ids:
        action_generator.append({
            '_op_type': 'delete',
            '_index': index_name,
            '_type': doc_type,
            '_id': data
        })

    print(action_generator)
    res_gen = streaming_bulk(es_conn, action_generator, max_retries=3, initial_backoff=10)

    for status, res in res_gen:
        if status:
            continue
            # print(status)
            # print(res)
        else:
            print('Error: {}'.format(res))
            final_res = 1

    es_conn.indices.refresh(index=index_name)
    return final_res

def get_es(env='staging'):
    es_hosts = ['es7-access-{}-{}.mist.pvt'.format('%03d' % d, env) for d in range(0, 3)]
    random.shuffle(es_hosts)
    es_port = '9200'

    if es_hosts:
        host_configs = [{'host': hh, 'port': es_port} for hh in es_hosts]
        print(host_configs)
        return Elasticsearch(host_configs)
    else:
        return None


if __name__ == '__main__':
    """
    import the module we want to run.  this can either be a py file or a package, but should be somewhere under
    local file.jobs.
    *** it also needs to implement a run_jobs method*** 
    """
    start = time.time()

    spark, date, hour, env = setup_spark_configs()

    print("Date:" + date)
    print("Hour:" + hour)
    print("Env:" + env)

    print('***** cleaning date: {}'.format(date))
    suggestion_raw = spark.sparkContext.sequenceFile(
        's3://mist-aggregated-stats-{}/entity_suggestion/entity_suggestion-{}/dt={}/hr={}/APOfflineEvent_1646119421.seq'
            .format(env, env, date, hour))

    #get all the garbage suggestions with impacted_ap_count as zero for that hour
    suggestion_data = suggestion_raw.map(lambda x: json.loads(x[1])).filter(
        lambda x: x["status"] == "open" and x["details"].get("impacted_ap_count") != None and x["details"]["impacted_ap_count"] == 0)

    suggestion_data_list = suggestion_data.collect()

    df_suggestion = pd.json_normalize(suggestion_data_list)

    row_keys_to_be_removed = list(set(list(df_suggestion["row_key"]))) #use set operation to get only the unique row_keys data
    print("Length of suggestions to be removed is: " + str(len(row_keys_to_be_removed)))
    delete_garbage_suggestions(row_keys_to_be_removed, env=env)

    end = time.time()
    logging.info("***** Execution of job took {} seconds".format(end - start))