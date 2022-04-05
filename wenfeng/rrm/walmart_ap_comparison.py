from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

env = "production"
es = Elasticsearch([{'host': 'es-proxy-{env}.mist.pvt'.format(env=env), 'port': 9200}])

es_index = "entity_event_202201*"

query=  {
    "query_string": {
        "query": "event_name:radio_stuck AND event_type:partial_beacon_stuck AND severity>=0"
    }
}
resp = Search(index=es_index).using(client=es).query(query).execute()

es_count = resp.hits.total if resp and resp.hits else 0

print("Got %d Hits:" % resp['hits']['total'])
for hit in resp['hits']['hits']:
    print(hit["_source"])