import requests
import numpy as np
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from dateutil import tz
#import pygsheets
from dateutil.relativedelta import relativedelta
import ast
import math
import operator
import requests
import json
from elasticsearch import Elasticsearch
es = Elasticsearch()
query = """
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "event_name": "frequent_reinit"
          }
        },
        {
          "range": {
            "end_time": {
              "gte": 1666137600000
          }
        }
      },
      {
          "range": {
            "end_time": {
              "lt": 1666224000000
          }
        }
      }
      ]
    }
  },
  "sort": [
    {
      "end_time": {
        "order": "desc"
      }
    }
  ],
  "size": 10000
}
"""

PROD_ES_SERVER = 'http://es715-access-000-production.mist.pvt'
PROD_ES_INDEX = 'entity_event_2022*'

headers = {
    'Content-Type': 'application/json'
}

es = Elasticsearch([PROD_ES_SERVER])
# cur_time = int(datetime.today().timestamp()*1000)-40*60*1000
# query = query % (cur_time, cur_time + 24*60*60*1000)
# query = query % (1612226308000, 1612312708000)
res = es.search(index=PROD_ES_INDEX, body=query)

if len(res['hits']['hits']) == 0:  # no data for the week
    df_es2 = pd.DataFrame()
else:
    # Check if there is 'query', 'numResults' in the columns
    df_es2 = pd.DataFrame([x['_source'] for x in res['hits']['hits']])

#filter out by severity
#df_es2 = df_es2.loc[(df_es2['severity'] >= 60)]

#calculate hours for event_duration
#df_es2['event_duration_hours'] = df_es2.apply(lambda x: x['event_duration']/3600, axis=1)

#extract total_reinit in one field
#df_es2['total_reinit'] = df_es2.apply(lambda x: x['details']['features']['tot_reinit'], axis=1)

#filter out where event_duration_hours are zero
#df_es2 = df_es2[df_es2['event_duration_hours'] != 0]

#calculate average reinit
#df_es2['avg_reinit'] = df_es2.apply(lambda x: x['total_reinit']/x['event_duration_hours'], axis=1)

#filter out where avg reinit is less than 30, only keep with greater than 30
#greater_than_30_avg = df_es2.loc[(df_es2['avg_reinit'] >= 30)]

#print(greater_than_30_avg.shape)

df_es2.to_csv('/Users/anagarkar/Downloads/frequent_reinit_19Oct.csv')

