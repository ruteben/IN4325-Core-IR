from elasticsearch import Elasticsearch
es = Elasticsearch()
index = "dbpedia_2015_10"

query_all = {"query": {
    "match_all": {}
}}

query = {
    "sort": ["_score"],
    "query": {
        "match": {
            "names": ""
        }
    }
}

# 1. Goal: Represent the raw content of the query/table as a set of terms, where terms are entities from a knowledge base. {q1, ...., 1n} for query q and {t1, ..., tm} for table T.
# 1.1. Detect Core Column, this column has the highest ratio of entities
# 1.2. Extract all entities from Core Column, extract top-k (k=10) entities from page title, table caption ranked using Mixture of Language Models approach.
# 1.3. Extract top-k (k=10) entities from query by issueing the same query against a knowledge base.

with open("data/queries.txt") as queries:
    for line in queries:
        print("Query: %s" % line[:-1])
        q = " ".join(line.split(sep=" ")[1:])[:-1] # take out the number, trim off the \n at the end
        query["query"]["match"]["names"] = q
        res = es.search(index, body=query)
        print("Got %d Hits" % res['hits']['total'])
        for hit in res['hits']['hits']:
            # print(hit['_source'].keys())
            print(hit['_source']['names'])
            # print("Related entities %s" % hit['_source']['related_entity_names'])