from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time
from multiprocessing.dummy import Pool as ThreadPool
import io


def retrieve_entities_BM25(text):
    es = Elasticsearch()
    index = "dbpedia_2015_10"

    query = {
        "query": {
            "match": {
                "catchall": text
            }
        }
    }

    res = es.search(index, body=query)
    for table in res['hits']['hits']:
        print(table['_id'])


retrieve_entities_BM25("banana")

