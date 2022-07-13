import os
import json
import math
import time
from arango import ArangoClient
from arango.http import DefaultHTTPClient
import pandas as pd

class CustomHTTPClient(DefaultHTTPClient):
    REQUEST_TIMEOUT = 1000

ENV = 'production'
LAST_TIMESTAMP = 1656075315
COLLECTION_NAME = "clientConnectedTo"

client = ArangoClient(hosts='http://arangod-topology-cluster-{}.mist.pvt'.format(ENV),
                      http_client=CustomHTTPClient())
db = client.db('mist-topology-{}'.format(ENV), '','')

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

wcid_data = pd.read_csv("client_wcid_v2.csv")
# wcid_data = wcid_data[wcid_data.orgId == '204d90a2-c42d-402f-a97c-6b84625c0029']
wcid_dict = wcid_data.to_dict(orient='records')
# print(wcid_dict)
wcid_list = chunker(wcid_dict, 1000)

q2 = """
    FOR r IN @records
        LET clients = (FOR c IN {COLLECTION_NAME}
            FILTER c.siteId == r.siteId AND c.wcid == r.wcid AND c.isExpired == false  RETURN c)
        FILTER LENGTH(clients) > 1

    RETURN {{wcid: r.wcid, clients:  clients}}
"""

query_update = """
    UPDATE {{ _key: '{_key}' }} WITH {{ expiredAt: {expiredAt},  isExpired: {isExpired} }} IN {COLLECTION_NAME}
"""

def update_query(query):
    return db.aql.execute(query)

total_update_count = 0
for wcid_chunk in wcid_list:
    query = q2.format(COLLECTION_NAME=COLLECTION_NAME)
    time.sleep(2)
    cur = db.aql.execute(query, bind_vars= {"records": wcid_chunk} ,batch_size=1000)
    records = list(cur)
    devices = []
    data = []
    print("records size : ", len(records))
    for x in records:
        if x['wcid'] in devices:
            print("WCID in the list : ", x['wcid'])
            continue
        devices.append(x['wcid'])
        sorted_list = sorted(x['clients'], key=lambda p: p['createdAt'], reverse=True)
        previous_expired_at = 'null'
        previous_is_expired = False
        for d in sorted_list:
            d['expiredAt'] = previous_expired_at
            d['isExpired'] = previous_is_expired
            #d['lastModifiedAt'] = LAST_TIMESTAMP
            previous_expired_at = d['createdAt']
            previous_is_expired = True
            data.append(d)
        data = list(filter(lambda k: k.get('isExpired') == True, data))
        for y in data:
            uq = query_update.format(COLLECTION_NAME=COLLECTION_NAME,
                                     _key=y['_key'], expiredAt=y['expiredAt'],
                                     isExpired=y['isExpired'])
                                    #lastModifiedAt=y['lastModifiedAt'])
            print(uq)
            try:
                update_query(uq)
            except Exception as e:
                print(e)
        total_update_count += len(data)
        print("Total update : ", total_update_count)
    time.sleep(3)
print("Final Total update : ", total_update_count)
