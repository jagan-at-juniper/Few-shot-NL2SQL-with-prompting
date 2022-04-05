
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("ap-zero-clients") \
    .getOrCreate()

spark.sparkContext().setLogLevel("ERROR");

env = "production"
provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

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


def get_es_events():
    """
    curl -XGET "http://es7-access-000-production.mist.pvt:9200/entity_suggestion_202202*/_search" -H 'Content-Type: application/json' -d'{ "query": {   "query_string": {     "query": "symptom:insufficient_capacity AND status:open"   } }, "size": 10,  "sort": [   {     "modification_time": {       "order": "desc"     }   } ],  "aggs": {    "agg1": {      "terms": {"field": "org_id" }    },    "agg2": {    "terms": {  "field": "status"  }    },    "agg3": {    "terms": {  "field": "unique_key"  }    }  }}'
    :return:
    """
    from datetime import datetime
    from elasticsearch import Elasticsearch
    from elasticsearch_dsl import Search

    # env = "production"
    es_host = 'es-proxy-{env}.mist.pvt'.format(env=env)
    es_host = "es7-access-000-{env}.mist.pvt".format(env=env)
    es = Elasticsearch([{'host': es_host, 'port': 9200}])

    es_index = "entity_suggestion_202202*"

    query=  {
        "query_string": {
            "query": "symptom:insufficient_capacity AND status:open"
        }
    }
    resp = Search(index=es_index).using(client=es).query(query).execute()

    es_count = resp.hits.total if resp and resp.hits else 0
    print("es_count", es_count)
    # print("Got %d Hits:" % resp['hits']['total'])
    for hit in resp['hits']['hits']:
        print(hit["_source"])


def get_event():
    """

    :return:
    """
    s3_gs_bucket = 's3://mist-aggregated-stats-production/aggregated-stats/'
    s3_gs_bucket += 'ap_capacity_candidates/dt=2022-02-*/hr=*/'

    df_events = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load(s3_gs_bucket)

    df_events=df_events.withColumn("date_hour", F.from_unixtime(F.col('timestamp')/1000, format='yyyy-MM-dd HH'))

    df_events.printSchema()
    print("df_events", df_events.count())

    candidate_saver_filter = "ap_combined_score>0.5 and sle_capacity < 0.6 and capacity_anomaly_count > 2 and " \
                             "(avg_nclients > site_avg_nclients or avg_nclients > 5.0) and " \
                             "(util_ap > site_avg_util or util_ap > 10.0)"

    candidate_emit_filter = "(accomplices > 0 or off_neighbors > 0 or ap_combined_score > 0.8)"
    df_events.filter(candidate_emit_filter).select("band").groupBy("band").count().show()
    cols = ["avg_nclients","util_ap", "capacity_anomaly_count", # "sticky_uniq_client_count",
             'neighbors', "accomplices",
             "off_neighbors",
             "strong_neighbors","ap_capacity_score", "ap_combined_score", "neighbors_score"]

    df_events.select(cols).summary().show()

    impacted_aps = set(df_events.select('ap_id').toPandas()['ap_id'])
    impacted_aps = [ x.replace("-", "") for x in impacted_aps ]
    print(impacted_aps)
    return impacted_aps

def check_capacity_anomaly_from_ap_events(last_days=3):
    files = []
    for i in range(last_days):
        date_day= (datetime.now()  - timedelta(days=i)).strftime("%Y-%m-%d")

        s3_bucket = 's3://mist-secorapp-production/ap-events/ap-events-production/dt={}/hr=*/*.seq'.format(date_day)
        files.append(s3_bucket)
        # files = "dir1, dir2, dir3,"
    files = ",".join(files)
    rdd = spark.sparkContext.sequenceFile(files)
    df_capacity = rdd.map(lambda x: json.loads(x[1])). \
        filter(lambda x: x['event_type'] == "sle_capacity_anomaly") \
        .map(lambda x: x.get("source")) \
        .toDF()

    return df_capacity

def check_impact_aps():
    impacted_aps = ['ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'ac23160dbbeb', 'd4dc09e4d38c', 'd4dc09e4d38c',
                    'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'd4dc09e4d38c', 'ac23160dbbeb',
                    'ac23160dbbeb', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
                    'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7',
                    'd4dc092b8263', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc09e4d3d7', 'd4dc092b8263']
    impacted_aps = ['5c5b35ae3e4e', '5c5b35cf546e', 'd420b04281b8', 'd420b0c0f677']
    impacted_aps = get_event()

    # df_capacity = check_capacity_anomaly_from_ap_events(7)
    df_capacity = check_capacity_anomaly_from_ap_events(7)
    df_capacity.printSchema()

    def ap_id_reformat(ap):
        return ap.replace("-", "")
    ap_id_reformat_f = F.udf(ap_id_reformat, StringType())
    df_capacity = df_capacity.withColumn("ap", ap_id_reformat_f(F.col("ap")))

    # df_capacity.select(capacity_cols).show()

    df_capacity_aps = df_capacity.filter(df_capacity.ap.isin(impacted_aps))
    n_row = df_capacity_aps.count()
    print("df_capacity_aps = ", n_row)

    s3_path = "{fs}://mist-data-science-dev/wenfeng/ap-capacity-aps/".format(fs=fs)
    print(s3_path)
    df_capacity_aps.write.save(s3_path,  format='parquet',  mode='overwrite',   header=True)
    # df_capacity_aps_1 = spark.read.parquet(s3_path)
    # df_capacity_aps_1.show(2)

    df_capacity_aps = df_capacity_aps.withColumn("date_hour", F.from_unixtime(F.col('timestamp'), format='yyyy-MM-dd HH'))


    capacity_cols = ["date_hour", "timestamp", "ap", "band", "channel", "impacted_wlans", "avg_nclients", "error_rate",
                     "interference_type", "anomaly_type", "sle_capacity", "util_ap", "util_all_mean"]

    df_capacity_aps.orderBy("ap", "band", "timestamp").select(capacity_cols).show(n_row)
    return df_capacity_aps


df_capacity_aps = check_impact_aps()