# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: PySpark
#     language: python
#     name: pysparkkernel
# ---

# +
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
#
import requests
import csv
import json
import time
import pytz
from datetime import datetime, timedelta
import urllib.parse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import statistics
import random

headers = ""
bearer = ""

first_time = True
asn_dict = {}
result_df = pd.DataFrame()
bar_df = pd.DataFrame(columns=["as_number", "dd_count"])

START_DATE_TIME = "2023-01-31T09:00:00"
END_DATE_TIME = "2023-01-31T11:00:00"

MAX_RECORDS=10000

tot_records = 0

def call_dd_api( url, action, parameters ):
    global first_time, headers, bearer
    
    if( first_time ):
        bearer = 'Bearer ' + "eyJhbGciOiJIUzUxMiIsImtpZCI6InA0cmtzNjYzaGEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhcGkiLCJpYXQiOjE2NzUxMjk0NzgsImp0aSI6IjE3Mzc3ZjIzLTk4OWQtNGNkNi1iODZkLTNmOTU4MDY5YTczYyJ9.QU-eKKZleSrbbOHB95v1wy4yh9StlneLVqrDtov_MT5Fc3vZQfASreQQcPIuyAwlTHPgNC21BnS-aMJ8Nf9NzQ"
        headers = {'authorization': bearer }
        first_time = False
        
    response = requests.request(action, url, headers=headers, params=urllib.parse.urlencode(parameters))
    if( "error" in response.json() and response.json()["error"] ):
        return None
    else:
        return response

def calc_median ( dd_arr ):
    arr = []
    if( len( dd_arr ) > 0 ):
        for i in range(len(dd_arr)):
            arr.append( dd_arr[i]['total'])
        m = statistics.median( arr )
    else:
        m = None
    return( m )

def call_papi( url, action, parameters ):
    response = requests.request("GET", url, headers=headers, params=urllib.parse.urlencode(parameters))
    return response

#if( 'site_id' in aps_sws_count_dict ):
#    num_aps = aps_sws_count_dict['site_id']['num_aps']
#    num_sws = aps_sws_count_dict['site_id']['num_sws']
#else:
#    papi_resp = call_papi("http://papi-internal-production.mist.pvt/internal/sites/" + str( site_id ))
#    num_aps = papi_resp['num_aps']
#    num_sws = papi_resp['num_sws']
#    aps_sws_count_dict['site_id'] = { 'num_aps': num_aps, 'num_sws': num_sws, 'start_time':  }

def process_event( asn_dict, org_id, site_id, entity_type, event_name, event_type ):
    global tot_records, result_df, bar_df, ts_start_date_time, ts_end_date_time
    if( (org_id, site_id) in asn_dict ):
        asnlst = asn_dict[org_id, site_id]  
        if( type(asnlst) == list ): # note that there are sometimes multiple ASNs associted with orgid, siteid
            asns = asnlst
        else:
            asns = [ asnlst ]
            
        for n in range( len( asns ) ):
            asn = asns[n][2:]
            # get start and end hour for downdetector api time range
            resp_time = resp['hits']['hits'][i]['_source']['end_time'] / 1000  # round to milliseconds
            dd_starttime = max( ts_start_date_time, resp_time - 900 ) # back 15 mins or start of day whichever is nearer
            dd_endtime = min( ts_end_date_time, resp_time + 900 ) # forward 15 mins or end of day whichever is nearer
            utctz = pytz.timezone('UTC')
            dd_startdate = datetime.fromtimestamp(dd_starttime,utctz).isoformat()
            dd_enddate = datetime.fromtimestamp(dd_endtime,utctz).isoformat() 
            params = {}
            params['startdate'] = dd_startdate 
            params['enddate'] = dd_enddate 
            tot_records += 1
            
            # get downdetector problem report
            dd_url = "https://downdetectorapi.com/v2/networks/" + str(asn) + "/reports" 
            ddresp = call_dd_api( dd_url, "GET", params )
            if( ddresp != None ):
                dataline = {
                    "datetime": datetime.fromtimestamp(round(resp_time), utctz).strftime('%d/%m/%y %H:%M'),
                    "org_id": org_id,
                    "site_id": site_id,
                    "entity_type": entity_type,
                    "event_name": event_name,
                    "event_type": event_type,
                    "as_number": asn,
                    "dd_count": calc_median( ddresp.json() )
                    }
                result_df = result_df.append(dataline, ignore_index = True)
                bar_row = [ asn, calc_median( ddresp.json() ) ]
                bar_df.loc[len(bar_df)] = bar_row

es = Elasticsearch('http://es715-access-000-production.mist.pvt:9200')

spark_asn_df = spark.read.parquet('s3://mist-data-science-dev/rjagannathan/orgid_siteid_asn.parquet')
asn_df = spark_asn_df.toPandas()
asn_dict = asn_df.groupby(['org_id','site_id'])['service_provider_asn'].agg(','.join).apply(lambda s: s.split(',') if ',' in s else s).to_dict()

past_day_timestamp = datetime.now() - timedelta(1) # previous day
START_DATE_TIME = past_day_timestamp.strftime("%Y-%m-%d") + "T00:00:00"
END_DATE_TIME = past_day_timestamp.strftime("%Y-%m-%d") + "T23:59:59"
YRMO = past_day_timestamp.strftime("%Y%m")
ts_start_date_time = round( time.mktime(time.strptime(START_DATE_TIME, '%Y-%m-%dT%H:%M:%S')) / 1000 )
ts_end_date_time = round( time.mktime(time.strptime(END_DATE_TIME, '%Y-%m-%dT%H:%M:%S')) / 1000 )

EE_INDEX = "entity_event_" + str(YRMO)

q = {
    "query": {
        "range": {
            "start_time": {
                "gte": START_DATE_TIME, 
                "lte": END_DATE_TIME
            }
        }
    },
    "sort": [
        {"end_time": "asc"},
        {"start_time": "asc"},
        {"site_id": "asc"}
    ]
}

resp = es.search(index=str(EE_INDEX), body=q, size=10000 )

pit_es = Elasticsearch('http://es715-access-000-production.mist.pvt:9200/_pit?keep_alive=1m')

tot_records=0
while( len( resp['hits']['hits'] ) > 0 and tot_records <= MAX_RECORDS ):
    nhits = resp['hits']['total']['value']
    count = len( resp['hits']['hits'] )
    starttime=0
    utctz=pytz.timezone('UTC')
    sort_start_time = resp['hits']['hits'][0]["sort"][0] 
    sort_end_time = resp['hits']['hits'][-1]["sort"][0] 

    for i in range(count):
        if( ( 'event_name' in resp['hits']['hits'][i]['_source'] ) and
            ( 'event_type' in resp['hits']['hits'][i]['_source'] ) and
            (
                ( ( resp['hits']['hits'][i]['_source']['event_name'] == 'switch_disconnect' ) or
                  ( resp['hits']['hits'][i]['_source']['event_name'] == 'gateway_disconnect' ) or
                  ( ( resp['hits']['hits'][i]['_source']['event_name'] == 'ap_disconnect' ) and
                    ( resp['hits']['hits'][i]['_source']['event_type'] == 'default_gateway_unreachable' )
                  ) or 
                  ( ( resp['hits']['hits'][i]['_source']['event_name'] == 'ap_disconnect' )  and
                    ( resp['hits']['hits'][i]['_source']['event_type'] == 'site_down' ) 
                  )
                )
            ) and
            ( 'org_id' in resp['hits']['hits'][i]['_source'] ) and
            ( 'site_id' in resp['hits']['hits'][i]['_source'] )
        ):
            process_event(asn_dict,
                           resp['hits']['hits'][i]['_source']['org_id'], 
                           resp['hits']['hits'][i]['_source']['site_id'], 
                           resp['hits']['hits'][i]['_source']['entity_type'],
                           resp['hits']['hits'][i]['_source']['event_name'],
                           resp['hits']['hits'][i]['_source']['event_type']
                           
                          )
            (org_id, site_id) = random.choice(list(asn_dict))
            process_event( asn_dict,
                           org_id,
                           site_id,
                           "placebo_entity_type",
                           "placebo_event_type",
                           "placebo_event_name"
                        )

                    
    #if( starttime != 0 ):
    #    print( str(datetime.fromtimestamp(starttime,utctz).isoformat()) + "  count=" + str(count) + ", " + " records=" + str(tot_records) )
    search_after_sort_end_time = resp['hits']['hits'][-1]["sort"][0]
    search_after_sort_start_time = resp['hits']['hits'][-1]["sort"][1]
    search_after_sort_site_id = resp['hits']['hits'][-1]["sort"][2]
    q = {
        "query": {
            "range": {
                "start_time": {
                    "gte": START_DATE_TIME, 
                    "lt": END_DATE_TIME
                }
            }
        },
        "search_after": [search_after_sort_end_time, search_after_sort_start_time, search_after_sort_site_id],
        "sort": [
            {"end_time": "asc"},
            {"start_time": "asc"},
            {"site_id": "asc"}      
        ]
    }
    resp = es.search(index=str(EE_INDEX), body=q, size=10000 )
    

current_timestamp = datetime.now()
current_datetime  = current_timestamp.strftime("%Y%m%d%H%M%S")
START_DATE_TIME_STR = START_DATE_TIME.replace(":", "_")
END_DATE_TIME_STR = END_DATE_TIME.replace(":", "_")

res_temporal_df = spark.createDataFrame(result_df)
res_temporal_df_name = "dd_ee_asn_temporal_data_" + START_DATE_TIME_STR + "_" + END_DATE_TIME_STR
res_temporal_df.write.format('parquet').mode('overwrite').save('s3://mist-data-science-dev/rjagannathan/' + str(res_temporal_df_name))

res_bar_df = spark.createDataFrame(bar_df)
res_bar_df_name = "dd_ee_asn_count_by_asn_data_" + START_DATE_TIME_STR + "_" + END_DATE_TIME_STR
res_bar_df.write.format('parquet').mode('overwrite').save('s3://mist-data-science-dev/rjagannathan/' + str(res_bar_df_name))

print( "For period " + str(START_DATE_TIME) + " to " + str(END_DATE_TIME) + ", " + str(tot_records) + " events associated with down detector data;" + "\n" + \
      "see parquet files " + str( res_temporal_df_name ) + " and " + str( res_bar_df_name ) + " in s3://mist-data-science-dev/rjagannathan/" )

# -

