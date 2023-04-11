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
import os

headers = ""
bearer = ""

first_time = True
asn_dict = {}
sws_oid_sid_dict = {}
aps_oid_sid_dict = {}

result_df = pd.DataFrame()
bar_df = pd.DataFrame(columns=["as_number", "dd_count"])

START_DATE_TIME = "2023-01-31T09:00:00"
END_DATE_TIME = "2023-01-31T11:00:00"

MAX_RECORDS=10000

BATCH_TIME = 1800

PLACEBO_THRESHOLD=0.10

tot_records = 0

def call_dd_api( url, action, parameters ):
    global first_time, headers, bearer
    
    if( first_time ):
        bearer = 'Bearer ' + "eyJhbGciOiJIUzUxMiIsImtpZCI6InA0cmtzNjYzaGEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhcGkiLCJpYXQiOjE2NzUxMjk0NzgsImp0aSI6IjE3Mzc3ZjIzLTk4OWQtNGNkNi1iODZkLTNmOTU4MDY5YTczYyJ9.QU-eKKZleSrbbOHB95v1wy4yh9StlneLVqrDtov_MT5Fc3vZQfASreQQcPIuyAwlTHPgNC21BnS-aMJ8Nf9NzQ"
        headers = {'authorization': bearer }
        first_time = False
    response = requests.request(action, url, headers=headers, params=urllib.parse.urlencode(parameters))
    try:
        return( response.json() )
    except Exception:
        return( None )

def calc_median ( dd_arr ):
    arr = []
    if( dd_arr != None and len( dd_arr ) > 0 ):
        for i in range(len(dd_arr)):
            arr.append( dd_arr[i]['total'])
        m = int(round(statistics.median( arr )))
    else:
        m = 0
    return( m )

def call_restful_api( url, action, parameters ):
    response = requests.request("GET", url, headers=headers, params=urllib.parse.urlencode(parameters))
    return response.json()

#if( 'site_id' in aps_sws_count_dict ):
#    num_aps = aps_sws_count_dict['site_id']['num_aps']
#    num_sws = aps_sws_count_dict['site_id']['num_sws']
#else:
#    papi_resp = call_restful_api("http://papi-internal-production.mist.pvt/internal/sites/" + str( site_id ))
#    num_aps = papi_resp['num_aps']
#    num_sws = papi_resp['num_sws']
#    aps_sws_count_dict['site_id'] = { 'num_aps': num_aps, 'num_sws': num_sws, 'start_time':  }

def process_event( asn_dict, org_id, site_id, entity_type, event_type, event_name ):
    global tot_records, result_df, bar_df, ts_start_date_time, ts_end_date_time
    #print("process_event: " + str( org_id ) + " " + str( site_id ) + " " + str(event_name ) )
    if( (org_id, site_id) not in asn_dict ):
        print( "\tno asn found in asn_dict" )
    else:
        asnlst = asn_dict[org_id, site_id]  
        if( type(asnlst) == list ): # note that there are sometimes multiple ASNs associted with orgid, siteid
            asns = asnlst
        else:
            asns = [ asnlst ]
            
        for n in range( len( asns ) ):
            asn = asns[n][2:]

            try:    
                asndeg = (call_restful_api("https://api.asrank.caida.org/v2/restful/asns/" + str(asn), "GET", {}))['data']['asn']['asnDegree']['total']
            except Exception:
                asndeg = 1
            # get start and end hour for downdetector api time range
            resp_time = resp['hits']['hits'][i]['_source']['end_time'] / 1000
            dd_starttime = max( ts_start_date_time, resp_time - 900 ) # back 15 mins or start of day whichever is nearer
            dd_endtime = min( ts_end_date_time, resp_time + 900 ) # forward 15 mins or end of day whichever is nearer
            #print( str(resp_time) + " " + str(dd_starttime) + " " + str(dd_endtime) )
            utctz = pytz.timezone('UTC')
            dd_startdate = datetime.fromtimestamp(dd_starttime,utctz).isoformat()
            dd_enddate = datetime.fromtimestamp(dd_endtime,utctz).isoformat() 
            params = {}
            params['startdate'] = dd_startdate 
            params['enddate'] = dd_enddate 

            #print( "\tquery time range: " + dd_startdate + " to " + dd_enddate )
            
            enable_dd_api = False
            if( event_name == "switch_disconnect" ):
                if( (org_id, site_id) not in sws_oid_sid_dict ):
                    c = (call_restful_api( "http://papi-internal-production.mist.pvt/internal/sites/" + str(site_id), "GET", {}))['num_switches']
                    sws_oid_sid_dict[org_id, site_id] = \
                        { 
                          "cnt": c,
                          "tme": resp_time 
                        }
                if((resp_time - (sws_oid_sid_dict[org_id, site_id]["tme"])) < BATCH_TIME ):
                    sws_oid_sid_dict[org_id, site_id]["cnt"] -= 1
                    #print( "enable_dd_api=" + str(enable_dd_api) + " event_name: " + event_name + " " + str(site_id) + " " + str( sws_oid_sid_dict[org_id, site_id]["cnt"] ))
                    if( sws_oid_sid_dict[org_id, site_id]["cnt"] == 0 ):
                        enable_dd_api = True  
                        del sws_oid_sid_dict[org_id, site_id]
                else:
                    del sws_oid_sid_dict[org_id, site_id]
            elif( event_name == "ap_disconnect" ):
                if( (org_id, site_id) not in aps_oid_sid_dict ):
                    c = (call_restful_api( "http://papi-internal-production.mist.pvt/internal/sites/" + str(site_id), "GET", {}))['num_aps']
                    aps_oid_sid_dict[org_id, site_id] = \
                        {
                          "cnt": c,
                          "tme": resp_time
                        }
                    enable_dd_api = True
                if((resp_time - (aps_oid_sid_dict[org_id, site_id]["tme"])) < BATCH_TIME ):
                    aps_oid_sid_dict[org_id, site_id]["cnt"] -= 1
                    #print( "enable_dd_api=" + str(enable_dd_api) + " event_name: " + event_name + " " + str(site_id) + " " + str( aps_oid_sid_dict[org_id, site_id]["cnt"] ))
                    if( aps_oid_sid_dict[org_id, site_id]["cnt"] == 0 ):
                        del aps_oid_sid_dict[org_id, site_id]
                else:
                    del aps_oid_sid_dict[org_id, site_id]
            elif( event_name == "gateway_disconnect" ):
                enable_dd_api = True
            elif( event_name == "placebo_event_name" ):
                enable_dd_api = True

            if( enable_dd_api ):
                # get downdetector problem report
                #print( "\tcalling dd api for " + str(asn) )
                dd_url = "https://downdetectorapi.com/v2/networks/" + str(asn) + "/reports" 
                ddresp = call_dd_api( dd_url, "GET", params )
                if( ddresp == None ):
                    return
                #print( "\tdd call returned  " + str(ddresp) )
                #{'error': True, 'message': 'Please check your input values: startdate (2023-03-25 10:07:46+00:00) is after enddate (2023-03-25 00:59:59+00:00).', 'statusCode': 400}
                try:
                    asndeg = (call_restful_api("https://api.asrank.caida.org/v2/restful/asns/" + str(asn), "GET", {}))['data']['asn']['asnDegree']['total']
                except Exception:
                    asndeg = 1
                if( 'error' not in ddresp ):
                    dataline = {
                        "datetime": datetime.fromtimestamp(round(resp_time), utctz).strftime('%d/%m/%y %H:%M'),
                        "org_id": org_id,
                        "site_id": site_id,
                        "entity_type": entity_type,
                        "event_name": event_name,
                        "event_type": event_type,
                        "as_number": asn,
                        "asn_degree": asndeg,
                        "dd_count": calc_median( ddresp )
                        }
                    print( "\tdataline: " + json.dumps( dataline ) )
                    tot_records += 1
                    result_df = result_df.append(dataline, ignore_index = True)
                    bar_row = [ asn, dataline['dd_count'] ]
                    bar_df.loc[len(bar_df)] = bar_row

                if( random.random() < PLACEBO_THRESHOLD ):
                    (org_id, site_id) = random.choice(list(asn_dict))
                    asnlst = asn_dict[org_id, site_id]
                    if( type(asnlst) == list ): # note that there are sometimes multiple ASNs associted with orgid, siteid
                        asn = asnlst[0]
                    else:
                        asn = [ asnlst ][0][2:]

                    try:
                        asndeg = (call_restful_api("https://api.asrank.caida.org/v2/restful/asns/" + str(asn), "GET", {}))['data']['asn']['asnDegree']['total']
                    except Exception:
                        asndeg = 1
                    dd_url = "https://downdetectorapi.com/v2/networks/" + str(asn) + "/reports"
                    ddresp = call_dd_api( dd_url, "GET", params )
                    if( ddresp == None ):
                        return
                    #print( "\tdd call returned  " + str(ddresp) )
                    #{'error': True, 'message': 'Please check your input values: startdate (2023-03-25 10:07:46+00:00) is after enddate (2023-03-25 00:59:59+00:00).', 'statusCode': 400}
                    if( 'error' not in ddresp ):
                        dataline = {
                            "datetime": datetime.fromtimestamp(round(resp_time), utctz).strftime('%d/%m/%y %H:%M'),
                            "org_id": org_id,
                            "site_id": site_id,
                            "entity_type": "placebo_entity_type",
                            "event_name": "placebo_event_name",
                            "event_type": "placebo_event_type",
                            "as_number": asn,
                            "asn_degree": asndeg,
                            "dd_count": calc_median( ddresp )
                         }
                        print( "\tdataline: " + json.dumps( dataline ) )
                        tot_records += 1
                        result_df = result_df.append(dataline, ignore_index = True)
                        bar_row = [ asn, dataline['dd_count'] ]
                        bar_df.loc[len(bar_df)] = bar_row

es = Elasticsearch('http://es715-access-000-production.mist.pvt:9200')

spark_asn_df = spark.read.parquet('s3://mist-data-science-dev/rjagannathan/orgid_siteid_asn.parquet')
asn_df = spark_asn_df.toPandas()
asn_dict = asn_df.groupby(['org_id','site_id'])['service_provider_asn'].agg(','.join).apply(lambda s: s.split(',') if ',' in s else s).to_dict()

past_day_timestamp = datetime.now() - timedelta(4) # previous day
START_DATE_TIME = past_day_timestamp.strftime("%Y-%m-%d") + "T00:00:00"
END_DATE_TIME = past_day_timestamp.strftime("%Y-%m-%d") + "T23:59:59"
print( "START_DATE_TIME=" + START_DATE_TIME + " END_DATE_TIME=" + END_DATE_TIME )
YRMO = past_day_timestamp.strftime("%Y%m")
ts_start_date_time = round( time.mktime(time.strptime(START_DATE_TIME, '%Y-%m-%dT%H:%M:%S')))
ts_end_date_time = round( time.mktime(time.strptime(END_DATE_TIME, '%Y-%m-%dT%H:%M:%S')))

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
                           resp['hits']['hits'][i]['_source']['event_type'],
                           resp['hits']['hits'][i]['_source']['event_name']
                           
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

if( len( result_df ) ):
    res_temporal_df = spark.createDataFrame(result_df)
    res_temporal_df_name = "dd_ee_asn_temporal_data_grouped_" + START_DATE_TIME_STR + "_" + END_DATE_TIME_STR
    res_temporal_df.write.format('parquet').mode('overwrite').save('s3://mist-data-science-dev/rjagannathan/' + str(res_temporal_df_name))

if( len( bar_df ) ):
    res_bar_df = spark.createDataFrame(bar_df)
    res_bar_df_name = "dd_ee_asn_count_by_asn_data_grouped_" + START_DATE_TIME_STR + "_" + END_DATE_TIME_STR
    res_bar_df.write.format('parquet').mode('overwrite').save('s3://mist-data-science-dev/rjagannathan/' + str(res_bar_df_name))

if( len( result_df ) > 0 and len( bar_df ) > 0 ):
    print( "For period " + str(START_DATE_TIME) + " to " + str(END_DATE_TIME) + ", " + str(tot_records) + " events associated with down detector data;" + "\n" + \
           "see parquet files " + str( res_temporal_df_name ) + " and " + str( res_bar_df_name ) + " in s3://mist-data-science-dev/rjagannathan/" )
