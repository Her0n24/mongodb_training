
import pymongo
import json
from pymongo import MongoClient, InsertOne
import os
from dotenv import load_dotenv, find_dotenv
import pprint
from typing import Iterable, Tuple, Any, Sequence
import glob
from bson.objectid import ObjectId
load_dotenv(find_dotenv())
from datetime import datetime

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://xwboy:{password}@cluster0.fpibh6i.mongodb.net/?appName=Cluster0&authSource=admin"
printer = pprint.PrettyPrinter()

client = MongoClient(connection_string)

### Load the example dataset json into MongoDB

db = client.jeoprady_db
question = db.question
requesting = []

# with open("JEOPARDY_QUESTIONS1.json") as f:
#     data = json.load(f)

#     # normalize to a list of dicts
#     if isinstance(data, list):
#         docs = [d for d in data if isinstance(d, dict)]
#     elif isinstance(data, dict):
#         docs = [data]
#     else:
#         docs = []

#     requesting = [InsertOne(d) for d in docs]

#     if requesting:
#         result = question.bulk_write(requesting)
#         printer.pprint(result.bulk_api_result)
#     else:
#         print("No valid documents to insert.")
#     client.close()

### Fuzzy matching

def fuzzy_matching():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": "aircraft",
                    "path": "category",
                    "synonyms": "mapping"                }
            }
        }
    ])
    printer.pprint(list(result))

# fuzzy_matching()

def autocomplete():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "autocomplete": {
                    "query": "the longest",
                    "path": "question",
                    "tokenOrder": "sequential",
                }
        }
        },
        {
            "$project": {
            "_id": 0,
            "question": 1
            }
        }
    ])
    printer.pprint(list(result))

autocomplete()