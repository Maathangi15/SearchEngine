from elasticsearch import Elasticsearch
import json

client = Elasticsearch(HOST="http://localhost", PORT=9200)
INDEX = 'lyrics_metaphor_db'

def search(list_):
    query_body = process_query(list_)
    response = client.search(index=INDEX, body=query_body)
    return response

def process_query(list_):
    if (list_[0] == 1):
        query_body = multi_match_query(list_[1])
    elif (list_[0] == 2):
        query_body = term_query(list_[1])
    elif (list_[0] == 3):
        query_body = year_range_search(list_[1])
    elif (list_[0] == 4):
        query_body = movie_wildcard_search(list_[1])
    else:
        query_body = basic_search(list_[1])
    return query_body

# Basic Search Query
def basic_search(query):
    q = {
        "query": {
            "query_string": {
                "query": query
            }
        }
    }
    return q

# Full text query: Multi Match Query
def multi_match_query(target):
    q = {
            "query": {
                "multi_match" : {
                    "query": target,
                    "type": "most_fields",
                    "fields": [ "இலக்கு_1", "இலக்கு_2", "இலக்கு_3", "இலக்கு_4", "இலக்கு_5", "இலக்கு_6"]
                }
            }
        }
    return q

# Multi match with aggregation
def agg_multi_match_q(query, fields=['திரைப்படம்', 'பாடல் வரிகள்'], operator='or'):
    q = {
        "size": 15,
        "explain": True,
        "query": {
            "multi_match": {
                "query": query,
                "fields": fields,
                "operator": operator,
                "type": "best_fields"
            }
        },
        "aggs": {
            
            "Lyricsist Filter": {
                "terms": {
                    "field": "பாடலாசிரியர்.keyword",
                    "size": 10
                }
            },
            "Singer Filter": {
                "terms": {
                    "field": "பாடகர்கள்.keyword",
                    "size": 10
                }
            },
            "Lyrics Filter": {
                "terms": {
                    "field": "பாடல் வரிகள்.keyword",
                    "size": 10
                }
            }
        }
    }

    q = json.dumps(q)
    return q

# Term level query: Term query
def term_query(query):
    q = {
        "query": {
            "term": {
                "வருடம்": {
                    "value": query
                }
            }
        }
    }
    return q

# Term level query: Range query
def year_range_search(query):  
    values = query.split(",")
    q={
        "query": {
             "range": {
                "வருடம்": {
                    "gte": values[0],
                    "lte": values[1],
                    "boost": 2.0
                }
            }
        }
    }
    return q

# Term level query: Wild card query
def movie_wildcard_search(query):  
    q={
        "query": {
            "wildcard": {
                "திரைப்படம்": {
                    "value": query +"*",
                    "boost": 1.0,
                    "rewrite": "constant_score"
                }
            }
        }
    }
    return q