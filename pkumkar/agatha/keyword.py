import os

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

SEMANTIC_SEARCH_INDEX_NAME = 'marvis_docs'
SEARCH_FIELDS = ['title', 'url']

env = os.environ.get('ENV', 'staging')


def get_es():
    host_list = [
        'es7-access-000-{}.mist.pvt'.format(env),
        'es7-access-001-{}.mist.pvt'.format(env),
        'es7-access-002-{}.mist.pvt'.format(env)
    ]
    return Elasticsearch([{'host': es, 'port': '9200'} for es in host_list])


def get_es_keyword_query(query):
    kw_query = {
        "match": {
            "title": {
                "query": query,
                "fuzziness": "AUTO",
            }
        },
    }
    return kw_query


def get_response_based_on_keyword(query_list):
    kw_search_urls = []
    kw_search_scores = []

    client = get_es()

    for query in query_list:
        if isinstance(query, str):
            kw_query = get_es_keyword_query(query)
            response = client.search(
                index=SEMANTIC_SEARCH_INDEX_NAME,
                body={
                    "size": 5,
                    "query": kw_query,
                    "_source": {"includes": SEARCH_FIELDS}
                }
            )
            urls = [np.NaN]
            scores = [np.NaN]

            docs = response.get('hits', {}).get('hits', {})
            for doc in docs:
                urls.append(doc.get('_source', {}).get('url', ""))
                scores.append(doc.get('_score', 0))

            if len(docs) < 5:
                print(urls, scores)
                print(query)
                break
            kw_search_urls.extend(urls)
            kw_search_scores.extend(scores)

    # print(kw_search_urls, kw_search_scores)
    #     break

    return kw_search_urls, kw_search_scores


def get_kw_search_results():
    data = pd.read_csv('queries.csv')
    query_list = list(data['query'])
    urls, scores = get_response_based_on_keyword(query_list)
    data['kw_search_urls'] = urls
    data['kw_search_scores'] = scores

    return data


data = get_kw_search_results()
data.to_excel("queries_with_kw.xlsx")
