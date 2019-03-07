from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time

entities = set([])  # using a set so duplicates are not added
es = Elasticsearch()
knowledgebase = "dbpedia_2015_10"




def mlm(query):
    query_terms = query.split()
    for term in query_terms:
        mlm_partial_query(term)


def mlm_partial_query(query):
    query = {
        "sort": ["_score"],
        "query": {
            "match": {
                "names": text
            }
        }
    }
