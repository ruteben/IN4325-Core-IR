from elasticsearch import Elasticsearch
es = Elasticsearch()
index = "dbpedia_2015_10"
query = {
    "query": {
        "match": {
            "names": "Black River"
        }
    }
}
query_all = {"query": {
    "match_all": {}
}}
res = es.search(index, body=query)
print("Got %d Hits" % res['hits']['total'])
for hit in res['hits']['hits']:
    # print(hit['_source'].keys())
    print(hit['_source']['names'])
    print("Related entities %s" % hit['_source']['related_entity_names'])