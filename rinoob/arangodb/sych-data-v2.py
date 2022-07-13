import os
import json
import math
import time
from arango import ArangoClient
from arango.http import DefaultHTTPClient

class CustomHTTPClient(DefaultHTTPClient):
    REQUEST_TIMEOUT = 1000

ENV = 'production'

client = ArangoClient(hosts='http://arangod-topology-cluster-{}.mist.pvt'.format(ENV),
                      http_client=CustomHTTPClient())
db = client.db('mist-topology-{}'.format(ENV), '','')

group_by_map = {
    'device': "orgId = d.orgId, mac = d.mac",
    'connectedTo': "orgId = d.orgId, source = d.source, target = d.target, sourcePort = d.sourcePort, targetPort = d.targetPort, relType = d.relType",
    'client': "orgId = d.orgId, wcid = d.wcid",
    'clientConnectedTo': "orgId = d.orgId, wcid = d.wcid"
#     'application': "",
#     'appRunOn': ""

}
grp = {
    'device': "{orgId: orgId, mac: mac, g: g[*].d}",
    'connectedTo': "{orgId: orgId, source: source, target: target, sourcePort: sourcePort, targetPort: targetPort, relType: relType, g: g[*].d}",
    'client': "{orgId: orgId, wcid: wcid, g: g[*].d}",
    'clientConnectedTo': "{orgId: orgId, wcid: wcid, g: g[*].d}"
}

query_1 = """
    FOR d IN {COLLECTION_NAME}
        FILTER d.isExpired == false
        COLLECT {group_by_filter} INTO g
        LET cnt = COUNT(g)
        FILTER cnt > 25
        SORT orgId DESC
        LIMIT ({page}-1)*{limit}, {limit}
        RETURN {collect_filter}
"""
query_update = """
    UPDATE {{ _key: '{_key}' }} WITH {{ expiredAt: {expiredAt},  isExpired: {isExpired} }} IN {COLLECTION_NAME}
"""

def execute_query(query, COLLECTION_NAME, page, limit):
    group_by_filter = group_by_map[COLLECTION_NAME]
    collect_filter = grp[COLLECTION_NAME]
    q = query.format(group_by_filter=group_by_filter,
                     collect_filter=collect_filter,
                     COLLECTION_NAME=COLLECTION_NAME, page=page, limit=limit)
    #print(q)
    cur = db.aql.execute(q, full_count=True, batch_size=1000)
    extras = cur.statistics()
    return list(cur), extras
def update_query(query):
    return db.aql.execute(query)


COLLECTION_NAME = 'clientConnectedTo'
start_page = 1
start_limit = 1000
total_update_count = 0
records, extras = execute_query(query_1, COLLECTION_NAME,start_page, start_limit)
total_pages = math.ceil(extras.get('fullCount', 1) / start_limit)
print('extras : ', extras, 'Total pages :', total_pages )

for idx in range(0, total_pages):
    data = []
    records, extras = execute_query(query_1, COLLECTION_NAME, start_page, start_limit)
    for x in records:
        #print("---")
        sorted_list = sorted(x['g'], key=lambda p: p['createdAt'], reverse=True)
        previous_expired_at = None
        previous_is_expired = False
        for d in sorted_list:
            d['expiredAt'] = previous_expired_at
            d['isExpired'] = previous_is_expired
            previous_expired_at = d['createdAt']
            previous_is_expired = True
            data.append(d)
    # non_expired = list(filter(lambda k: k.get('isExpired') == False, data))
    data = list(filter(lambda k: k.get('isExpired') == True, data))
    total_update_count += len(data)
    print('Total update :', total_update_count)
    for y in data:
        uq = query_update.format(COLLECTION_NAME=COLLECTION_NAME, _key=y['_key'], expiredAt=y['expiredAt'], isExpired=y['isExpired'])
        #print(uq)
        update_query(uq)
    time.sleep(2)
print("-----------------\n Final Total update count : ", total_update_count)
