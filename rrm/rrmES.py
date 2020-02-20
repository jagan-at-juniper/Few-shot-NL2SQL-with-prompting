from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

env = "production"
es = Elasticsearch([{'host': 'es-access-000-{env}.mist.pvt'.format(env=env), 'port': 9200}])

es_index = "discovered_switches_201908"

resp = Search(index=es_index).using(client=es).query('match', site_id="291ba26-6e1e-11e5-9cdd-02e208b2d34f").execute()

es_count = resp.hits.total if resp and resp.hits else 0

print("Got %d Hits:" % resp['hits']['total'])
for hit in resp['hits']['hits']:
    print(hit["_source"])
