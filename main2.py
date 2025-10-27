from pymongo import MongoClient
from pymongo.collection import Collection
import os
from dotenv import load_dotenv, find_dotenv
import pprint
from typing import Iterable, Tuple, Any, Sequence
import glob
from bson.objectid import ObjectId
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://xwboy:{password}@cluster0.fpibh6i.mongodb.net/?appName=Cluster0"

client = MongoClient(connection_string)

dbs = client.list_database_names()
print(dbs)

test_db = client["test"]
test_collection = test_db["wine"]

shiraz_doc = {
    "Name/ Variety": "Shiraz",
    "year": "2018",
    "volume_ml": 750,
    "brand": "",
    "price_hkd": 150,
    "origin": "Rhône Valley, France" 
}

merlot_doc = {
    "Name/ Variety": "Merlot",
    "year": "2013",
    "volume_ml": 750,
    "brand": "",
    "price_hkd": 100,
    "origin": "Rhône Valley, France" 
}

riesling_doc = {
    "Name/ Variety": "Riesling",
    "year": "2024",
    "volume_ml": 750,
    "brand": "",
    "price_hkd": 220,
    "origin": "Rhône Valley, France" 
}

chardonnay_doc = {
    "Name/ Variety": "Chardonnay",
    "year": "2023",
    "volume_ml": 750,
    "brand": "Barefoot",
    "price_hkd": 90,
    "origin": "USA" 
}

wine_list = [shiraz_doc, merlot_doc, riesling_doc, chardonnay_doc] 

def insert_one_document(collection_name: Collection, document: dict)-> None:
    inserted_id = collection_name.insert_one(document).inserted_id
    print("Insertion of document successful with document ID")
    print(inserted_id)


def insert_multiple_documents(collection_name: Collection, documents: list) -> None:
    inserted_ids = collection_name.insert_many(documents).inserted_ids
    print("Insertion of documents successful with document IDs")
    print(inserted_ids)

# insert_multiple_documents(test_collection, wine_list)

wine_field_names = []
wine_field_values = []
seen = set()

for wine in wine_list:
    names = []
    for field in wine.keys():
        if not field in seen:
            wine_field_names.append(field)
            seen.add(field)

for field in wine_field_names:
    value = []
    for wine in wine_list:
        value.append(wine.get(field)) #get method
    wine_field_values.append(value) 

print(wine_field_names)
print(wine_field_values)

# Advance list comprehension version
# wine_field_names = list(dict.fromkeys(k for d in wine_list for k in d.keys()))
# wine_field_values = [[d.get(k) for d in wine_list] for k in wine_field_names]


def create_and_insert_multiple_documents(collection_name: Collection, field_names: Sequence[str], lists: list[list]):
    
    if not field_names or not lists:
        return
    
    # ensure all value-lists have same length
    n = len(lists[0])
    if any(len(lst) != n for lst in lists):
        raise ValueError("All inner lists must have the same length")

    docs = []

    for row_idx in range(n):
        doc = {}
        fld_idx = 0
        for fname in field_names:
            # if there's a corresponding value-list use it, otherwise set None
            if fld_idx < len(lists):
                doc[fname] = lists[fld_idx][row_idx]
            else:
                doc[fname] = None
            fld_idx += 1
        docs.append(doc)

    if docs:
        inserted_ids = collection_name.insert_many(docs).inserted_ids
    print("Insertion of documents successful with document IDs")
    print(inserted_ids)

# create_and_insert_multiple_documents(test_collection, wine_field_names, wine_field_values)

def find_all_wine():
    with test_collection.find() as cursor:
        for items in cursor:
            print(items)

def find_all_wine_by_origin(origin: str):
    matched_wine = test_collection.find({"origin": origin})
    for items in matched_wine:
        print(items)

def find_all_wine_by_year(year: str):
    matched_wine = test_collection.find({"year": year})
    for items in matched_wine:
        print(items)

def find_all_wine_by_price_range(min_price: int, max_price: int):

    price_filter = {"$and":
        [{"price_hkd": {"$gte": min_price }},
         {"price_hkd": {"$lte": max_price }}]
    }
    
    targets = test_collection.find(price_filter)
    for items in targets:
        print(items)


# def update_wine_by_id():

# find_all_wine()
# find_all_wine_by_origin("USA")
# find_all_wine_by_year("2018")
find_all_wine_by_price_range(100, 150)