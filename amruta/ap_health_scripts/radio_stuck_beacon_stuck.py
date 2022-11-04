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
{"query": {
    "bool": {
      "must": [
        {
          "match": {
            "event_name": "radio_stuck"
          }
        },
        {
          "match": {
            "event_type": "beacon_stuck"
          }
        },
        {
          "match": {
            "org_id": "d8cec22ee0c2-11e5-8d0f-02e208b2d34f"
          }
        },
        {
          "range": {
            "start_time": {
              "gte": 1663804800000
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
}"""

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

df_es2 = df_es2.loc[(df_es2['severity'] >= 60)]

print(df_es2.shape)
df_es2.to_csv('/Users/anagarkar/Downloads/beacon_stuck_d8cec22ee0c2-11e5-8d0f-02e208b2d34f.csv')
