# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 15:04:28 2020

@author: spasch
"""

from elasticsearch import Elasticsearch

es = Elasticsearch(HOST="http://localhost", PORT=9200)
es = Elasticsearch()

es = Elasticsearch(
    ['10.10.146.135'],
    http_auth=('openlab', 'openlab!'),
    scheme="https",
    port=443,
)
es = Elasticsearch()

es = Elasticsearch(HOST="10.10.146.135", PORT=9200)
es = Elasticsearch()


es.indices.create(index="first_index")

# Looking if the index exists

es.indices.exists(index="first_index")


indices=es.indices.get_alias().keys()
sorted(indices)
# Delete index

#es.indices.delete(index="first_index")


doc_1 = {"city": "Paris", "country": "France"}
doc_2 = {"city": "Vienna", "country": "Austria"}
doc_3 = {"city": "London", "country": "England"}

es.index(index="cities", doc_type="places", id=1, body=doc_1)
es.index(index="cities", doc_type="places", id=2, body=doc_2)
es.index(index="cities", doc_type="places", id=3, body=doc_3)

res = es.get(index="cities", doc_type="places", id=1)

res

# Creating our data

doc_1 = {"sentence":"Hack COVID-19 is amazing!"}
doc_2 = {"sentence":"Hack-Quarantine is stunning!"}

es.index(index="english", doc_type="sentences", id=1, body=doc_1)
es.index(index="english", doc_type="sentences", id=2, body=doc_2)

# Creating our query

## Match query 

body = {
    "from":0,
    "size":0,
    "query": {
        "match": {
            "sentence":"Hack"
        }
    }
}

res = es.search(index="english", body=body)
res


body = {
    "from":0,
    "size":2,
    "query": {
        "match": {
            "sentence":"Hack"
        }
    }
}

res = es.search(index="english", body=body)
res


body = {
    "from":0,
    "size":2,
    "query": {
        "match": {
            "sentence":"Hack Quarantine"
        }
    }
}

res = es.search(index="english", body=body)
res

#### load json
import requests, json, os
old_dir = os.getcwd()
cd "C:\\Users\\spasch.SVA\\Desktop\\elasticsearch_docs\\recipe_data"
i=0
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        f = open(filename)
        
        docket_content = f.read()
        # Send the data into es
        es.index(index='myindex', ignore=400, doc_type='docket', 
        id=i, body=json.loads(docket_content))
        i = i + 1


for filename in os.listdir(directory):
    print(filename)

