from pymongo import MongoClient
from pymongo.collection import Collection
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


def update_wine_by_id(collection_name: Collection, id: str, all_updates: dict) -> None:
    # Convert to object ID 
    converted_str= ObjectId(id).toString()

    update_id = collection_name.update_one(filter = {"id": id} ,update= all_updates)
    for items in update_id:
        print(items)

# update_to_chardonney = {"price_hkd": 500}


# find_all_wine_by_origin("USA")
# find_all_wine_by_year("2018")
find_all_wine_by_price_range(100, 150)

def create_customer_collection():
    customer_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "title": "Customer Object Validaton",
        "required": ["address", "membership_tier", "name", "birthdate"],
        "properties": {
        "name": {
            "bsonType": "string",
            "description": "'name' must be a string and is required"
        },
        "birthdate": {
            "bsonType": "date",
            "description": "'birthdate' must be a date and is required"
        },
        "membership_tier": {
            "bsonType": "string",
            "description": "'membership_tier' must be a string"
        },
            "address": {
                "bsonType": "string",
                "description": "'address' must be a string and is required"
            },
        }
    }
    }
    try:
        test_db.create_collection("customer")
    except Exception as e:
        print("Collection probably exists:", e)

    test_db.command("collMod", "customer", validator= customer_validator)

def create_customer_data():
    customer_data = [
        {
            "name": "Alice",
            "birthdate": datetime(1990, 1, 1),
            "membership_tier": "gold",
            "address": "123 Main St",
            "purchased_wine": [test_db["wine"].find_one({"Name/ Variety": "Chardonnay"})["_id"]]
        },
        {
            "name": "Bob",
            "birthdate": datetime(1985, 5, 15),
            "membership_tier": "silver",
            "address": "4 Kings Rd",
            "purchased_wine": [test_db["wine"].find_one({"Name/ Variety": "Merlot"})["_id"],
                               test_db["wine"].find_one({"Name/ Variety": "Shiraz"})["_id"]]
        },
        {
            "name": "Hermes",
            "birthdate": datetime(2003, 12, 15),
            "membership_tier": "silver",
            "address": "5 Prince Rd East",
            "purchased_wine": [test_db["wine"].find_one({"Name/ Variety": "Chardonnay"})["_id"]]
        },
        {
            "name": "Zoe",
            "birthdate": datetime(2010, 1, 15),
            "membership_tier": "None",
            "address": "78 Des Voeux Rd West",
            "purchased_wine": [test_db["wine"].find_one({"Name/ Variety": "Riesling"})["_id"]]
        }


    ]
    customer_collection = test_db["customer"]
    customers = customer_collection.insert_many(customer_data).inserted_ids
    for items in customers:
        print(items)

# create_customer_data()


# # Retrieve all customers with an address containing "Rd"
# customers_address_with_a = test_db["customer"].find({"address": {"$regex": "Rd{1}"}}) #Regex stands for regular expression and {1} means that we are looking for expression with at least one Rd
# printer.pprint(list(customers_address_with_a))

# customers_and_wine = test_db["customer"].aggregate([{
#     "$lookup": {
#         "from" : "wine",
#         "localField": "purchased_wine",
#         "foreignField": "_id",
#         "as": "wine_wish_list"
#     }
# }])
# printer.pprint(list(customers_and_wine))


### More complex aggregation example, building a query pipeline

# customers_wine_count = test_db["customer"].aggregate([
#     {
#         "$lookup": {
#             "from" : "wine",
#             "localField": "purchased_wine",
#             "foreignField": "_id",
#             "as": "wine_wish_list"
#         }
#     },
#     {
#         "$addFields":{
#             "total_purchased_amount": {"$sum": "$wine_wish_list.price_hkd"}
#         }
#     },
#     {
#         "$addFields":{
#             "number_of_wine_purchased": {"$size": "$wine_wish_list"}
#         }
#     },
#     {
#         "$project":{
#             "name": 1,
#             "number_of_wine_purchased": 1,
#             "total_purchased_amount": 1,
#             "_id": 0
#         }
#     }
# ])
# printer.pprint(list(customers_wine_count))

### Write a query that finds the average age of customers purchasing each of the wine
avg_customer_age_for_items = test_db["wine"].aggregate([
    {
        "$lookup": {
            "from": "customer",
            "let": {"wineId": "$_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$in": ["$$wineId","$purchased_wine"]
                        }
                    }
                },
                {"$project": {"birthdate":1, "_id": 0 }}
            ],
            "as": "buyers"
        }
    },
    {
        "$addFields": {
            "buyer_ages":{
                "$map": {
                    "input" : "$buyers",
                    "as": "buyers",
                    "in": {
                        "$dateDiff": {
                            "startDate": "$$buyers.birthdate",
                            "endDate": "$$NOW",
                            "unit": "year"
                        }
                    }
                }
            }

        }
    },
    {
        "$addFields": {
            "avg_customer_age": {"$avg": "$buyer_ages"},
            "buyers_count": {"$size": "$buyers"}
        }
    },
    {"$project": {"Name/ Variety": 1, "avg_customer_age": 1, "buyers_count": 1, "_id": 0}}
]
)
printer.pprint(list(avg_customer_age_for_items))