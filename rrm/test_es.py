


entity_suggestion_202106 = []
for x in res_search['hits']['hits'][:]:
    ind = x.get('_index')
    sug = x.get('_source')
    if ind=="entity_suggestion_202106":
        entity_suggestion_202106.append(sug)
#     bulk_save(ind, [sug])
#     print(ind)
len(entity_suggestion_202106)
bulk_save("entity_suggestion_202106", entity_suggestion_202106)