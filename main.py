from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time
from multiprocessing.dummy import Pool as ThreadPool
import io

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
            entities.add(text[1:-1].split("|")[1])  # use 0 for entity identifier, 1 for entity name
    return entities


# Detect entities in a text using Elasticsearch index on dbpedia
def detect_entities(entity_set):
    entities = set([])  # using a set so duplicates are not added
    es = Elasticsearch()
    index = "dbpedia_2015_10"

    # TODO include MLM, this will very likely improve the results
    search_array = []
    for entity in entity_set:
        search_array.append({'index': index, "sort": ["_score"]})
        # search_array.append({"query": {"match": {"names": entity}}})
        search_array.append({"query": {"multi_match": {"query": entity, "fields": ["names"]}}})

    request = ''
    for each in search_array:
        request += '%s \n' % json.dumps(each)
    res = es.msearch(body=request, max_concurrent_searches=1000)
    # print(res)
    # # print("Got %d Hits" % res['hits']['total'])
    for response in res['responses']:
        for hit in response['hits']['hits']:
            # print(hit['_source'].keys())
            entities |= set(hit['_source']['names'])
    # print("%d entities related to %s" % (len(entities), text))
    # print(entities)
    return entities


def retrieve(table_name):
    _t = table_name
    table = tables_json[_t]
    if not (table['numDataRows'] == 0 or table['numCols'] == 0):
        text_entities = set([])  # using a set so duplicates are not added
        page_title = table['pgTitle']  # use this to extract entities as well
        # print(page_title)
        text_entities.add(page_title)
        table_caption = table['caption']  # use this to extract entities as well
        # print(table_caption)
        text_entities.add(table_caption)
        main_col = table_core[_t]
        # print("Main column is number %d, which is named %s" % (main_col, table['title'][main_col]))
        # next step is to extract entities from main column
        text_entities |= detect_column_entities(table['data'], main_col)
        # print(entities)
        # print("Processed table %s" % _t)
        return detect_entities(text_entities)
    return None


with io.open("data/tables/table.json", 'r') as tables, open("data/tables/table-core.json", 'r') as core:
    pool = ThreadPool(64)
    all_entities = []
    table_core = json.load(core)
    start = time.clock()
    counter = 0
    for line in tables:
        tables_json = json.loads(line)
        all_entities += pool.map(retrieve, tables_json.keys())
        counter += 1
        if counter % 1000 == 0:
            print("processed %d tables" % counter)
    end = time.clock()
    print("Entire file took %s seconds" % (end-start))
