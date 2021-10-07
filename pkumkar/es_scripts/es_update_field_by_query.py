import os
import time

from elasticsearch import Elasticsearch


def get_es(env):
    env = os.environ.get('ENV', env)
    es7_host = ['es7-access-000-{}.mist.pvt'.format(env),
                'es7-access-001-{}.mist.pvt'.format(env),
                'es7-access-002-{}.mist.pvt'.format(env)
                ]
    es_port = '9200'
    return Elasticsearch([{'host': es, 'port': es_port} for es in es7_host])


def generate_search_query(entities, field, value):
    search_query = {
        "bool": {
            "must": [
                {
                    "terms": {
                        "display_entity_type": entities
                    }
                },
                {
                    "match": {
                        field: value
                    }
                }
            ]
        }
    }

    return search_query


def execute_search_query(es_client, index_name, entities, field, value):
    query = {
        "query": generate_search_query(
            entities=entities,
            field=field,
            value=value)
    }
    resp = es_client.search(index=index_name,
                            body=query)

    return resp


def generate_update_query(entities, field, old_value, new_value):
    update_query = {
        "query": generate_search_query(entities, field, old_value),
        "script": {
            "source": f"ctx._source['{field}'] = '{new_value}'",
            "lang": "painless"
        }
    }
    return update_query


def execute_update_by_query(es_client, index_name, entities, field, old_value, new_value):
    query = generate_update_query(entities=entities,
                                  field=field,
                                  old_value=old_value,
                                  new_value=new_value)

    update_response = es_client.update_by_query(index=index_name,
                                                body=query,
                                                request_timeout=60)
    return update_response


def update_by_query(es_client, index_name, entities, field, old_value, new_value):
    search_resp = execute_search_query(es_client=es_client,
                                       index_name=index_name,
                                       entities=entities,
                                       field=field,
                                       value=old_value)

    count = search_resp.get('hits').get('total').get('value')
    if count > 0:

        update_resp = execute_update_by_query(es_client=es_client,
                                              index_name=index_name,
                                              entities=entities,
                                              field=field,
                                              old_value=old_value,
                                              new_value=new_value
                                              )

        if update_resp.get('updated'):
            print('Changed {} entries from {} to {}'.format(update_resp.get('updated'), old_value, new_value))
    else:
        print('{} entries to change from {} to {}'.format(count, old_value, new_value))

    time.sleep(2)


if __name__ == '__main__':
    mapping = {
        'high_memory': 'high_memory_usage',
        'high_cpu_control_plane': 'high_control_plane_cpu_usage',
        'high_cpu_data_plane': 'high_data_plane_cpu_usage',
        'high_temp_chassis': 'high_chassis_temp',
        'high_temp_cpu': 'high_cpu_temp',
        'high_power': 'high_power_usage',
        'high_cpu': 'high_cpu_usage',
    }

    # Change this line based on VPN
    es_client = get_es('staging')

    index_name = 'entity_event_2021*'
    entities = ['switch', 'gateway']
    field = 'event_type'

    for old_value, new_value in mapping.items():
        update_by_query(es_client=es_client,
                        index_name=index_name,
                        entities=entities,
                        field=field,
                        old_value=old_value,
                        new_value=new_value)
