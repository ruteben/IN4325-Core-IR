from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time

# 1. Goal: Represent the raw content of the query/table as a set of terms, where terms are entities from a knowledge base. {q1, ...., 1n} for query q and {t1, ..., tm} for table T.
# 1.1. Detect Core Column, this column has the highest ratio of entities
# 1.2. Extract all entities from Core Column, extract top-k (k=10) entities from page title, table caption ranked using Mixture of Language Models approach.
# 1.3. Extract top-k (k=10) entities from query by issueing the same query against a knowledge base.


# Detect the main column, based on the number of entities in each column of the table
def detect_main_col(table):
    num_cols = table['numCols']
    num_rows = table['numDataRows']
    number_of_entities = np.zeros(num_cols)
    # go over all rows and columns to detect which contains most entities
    for i in range(num_rows):
        for j in range(num_cols):
            if re.search("\[.+\|.+\]", table['data'][i][j]):
                number_of_entities[j] += 1

    return np.argmax(number_of_entities)


# Detect the entities in the main column
def detect_column_entities(table, column):
    entities = set([])  # using a set so duplicates are not added
    # TODO figure out if we need to use the entity identifier to find 1 or more entities, or need to use entity name

    for row in table:
        text = row[column]
        if re.search("\[.+\|.+\]", text):  # entities are listed as [this_is_an_entity|entity], use regex to detect
            entities |= detect_entities(text[1:-1].split("|")[1])  # use 0 for entity identifier, 1 for entity name
    return entities


# Detect entities in a text using Elasticsearch index on dbpedia
def detect_entities(text):
    entities = set([])  # using a set so duplicates are not added
    es = Elasticsearch()
    index = "dbpedia_2015_10"

    # TODO include MLM, this will very likely improve the results
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
    # print("%d entities related to %s" % (len(entities), text))
    # print(entities)
    return entities


with open("data/tables/re_tables-0001.json", 'r') as tables:
    tables_json = json.load(tables)
    start = time.clock()
    for t in tables_json:
        table = tables_json[t]
        print(table)
        # check for empty table
        if not(table['numDataRows'] == 0 or table['numCols'] == 0):
            entities = set([])  # using a set so duplicates are not added
            page_title = table['pgTitle']  # use this to extract entities as well
            # print(page_title)
            entities |= detect_entities(page_title)
            table_caption = table['caption']  # use this to extract entities as well
            # print(table_caption)
            entities |= detect_entities(table_caption)
            main_col = detect_main_col(table)
            # print("Main column is number %d, which is named %s" % (main_col, table['title'][main_col]))
            # next step is to extract entities from main column
            entities |= detect_column_entities(table['data'], main_col)
            # print(entities)
    end = time.clock()
    print("Single table file took %s seconds" % (end-start))
