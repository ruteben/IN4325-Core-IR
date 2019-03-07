from elasticsearch import Elasticsearch
import json
import numpy as np
import re
import time

entities = set([])  # using a set so duplicates are not added
es = Elasticsearch()
index = "dbpedia_2015_10"

avg_names = calculate_average_representation_size("names")
avg_categories = calculate_average_representation_size("categories")
avg_attributes = calculate_average_representation_size("attributes")
avg_similar_entity_names = calculate_average_representation_size("similar_entity_names")
avg_related_entity_names = calculate_average_representation_size("related_entity_names")

def calculate_average_representation_size(represenation):
