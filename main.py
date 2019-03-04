from elasticsearch import Elasticsearch
import json
import numpy as np
import re

# es = Elasticsearch()
# index = "dbpedia_2015_10"
#
# query_all = {"query": {
#     "match_all": {}
# }}
#
# query = {
#     "sort": ["_score"],
#     "query": {
#         "match": {
#             "names": ""
#         }
#     }
# }

# 1. Goal: Represent the raw content of the query/table as a set of terms, where terms are entities from a knowledge base. {q1, ...., 1n} for query q and {t1, ..., tm} for table T.
# 1.1. Detect Core Column, this column has the highest ratio of entities
# 1.2. Extract all entities from Core Column, extract top-k (k=10) entities from page title, table caption ranked using Mixture of Language Models approach.
# 1.3. Extract top-k (k=10) entities from query by issueing the same query against a knowledge base.

# with open("data/queries.txt") as queries:
#     for line in queries:
#         print("Query: %s" % line[:-1])
#         q = " ".join(line.split(sep=" ")[1:])[:-1] # take out the number, trim off the \n at the end
#         query["query"]["match"]["names"] = q
#         res = es.search(index, body=query)
#         print("Got %d Hits" % res['hits']['total'])
#         for hit in res['hits']['hits']:
#             # print(hit['_source'].keys())
#             print(hit['_source']['names'])


def detect_main_col(table, skip_rows):
    num_cols = table['numCols']
    num_rows = table['numDataRows']
    number_of_entities = np.zeros(num_cols)
    # go over all rows and columns to detect which contains most entities
    for i in range(num_rows):
        for j in range(num_cols):
            if re.search("\[.+\|.+\]", table['data'][i][j]):
                number_of_entities[j] += 1

    return np.argmax(number_of_entities)


def detect_main_column_entities(table, column):
    entities = set([])

    for row in table:
        text = row[column]
        if re.search("\[.+\|.+\]", text):
            entities |= detect_entities(text[1:-1].split("|")[0])  # use 0 for entity identifier, 1 for entity name
    return entities


def detect_entities(text):
    entities = set([])
    es = Elasticsearch()
    index = "dbpedia_2015_10"

    query = {
        "sort": ["_score"],
        "query": {
            "match": {
                "names": text
            }
        }
    }
    res = es.search(index, body=query)
    # print("Got %d Hits" % res['hits']['total'])
    for hit in res['hits']['hits']:
        # print(hit['_source'].keys())
        entities |= set(hit['_source']['names'])
    print("%d entities related to %s" % (len(entities), text))
    return entities


with open("data/tables/re_tables-0001.json", 'r') as tables:
    tables_json = json.load(tables)
    for t in tables_json:
        table = tables_json[t]
        print(table)
        header_rows = table['numHeaderRows']
        entities = set([])
        page_title = table['pgTitle']  # use this to extract entities as well
        print(page_title)
        entities |= detect_entities(page_title)
        table_caption = table['caption']  # use this to extract entities as well
        print(table_caption)
        entities |= detect_entities(table_caption)
        main_col = detect_main_col(table, header_rows)
        print("Main column is number %d, which is named %s" % (main_col, table['title'][main_col]))
        # next step is to extract entities from main column
        entities |= detect_main_column_entities(table['data'], main_col)
        print(entities)
        # break
