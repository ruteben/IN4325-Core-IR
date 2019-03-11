from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time

def retrieve_entities_BM25(text):
    with open("data/tables/re_tables-0001.json", 'r') as tables:
        entities = set([])  # using a set so duplicates are not added
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
        for hit in res['hits']['hits']:
            print(hit['_source'])
            entities |= set(hit['_source']['names'])
        return entities
