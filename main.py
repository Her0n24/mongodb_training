from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://xwboy:{password}@cluster0.fpibh6i.mongodb.net/?appName=Cluster0"
client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client["test"]
collections = test_db.list_collection_names()
print(collections)

def insert_test_doc():
    collection = test_db["test"]
    test_document = {
        "name": "Hey",
        "type": "Imatype"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

production = client["production"]
person_collection = production["person_collection"]

def create_documents():
    first_names = ["Amy", "Bosco", "Charlie", "Dan", "Eason", "Fanny"]
    last_names = ["Turner", "Smith", "Johnson", "Brown", "Williams", "Jones"]
    ages = [25, 30, 35, 40, 45, 50]

    docs = []

    for first_names, last_names, ages in zip(first_names, last_names, ages):
        doc = {"first_name": first_names, "last_name": last_names, "age": ages}
        docs.append(doc)

    person_collection.insert_many(docs)

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

def find_someone(someone: str):
    target_person = person_collection.find_one({"first_name": someone})
    printer.pprint(target_person)

# find_someone("Eason")

# def remove_duplicates_by_name():
#     # find groups with the same first_name+last_name and >1 doc
#     pipeline = [
#         {"$group": {
#             "_id": {"first_name": "$first_name", "last_name": "$last_name"},
#             "ids": {"$push": "$_id"},
#             "count": {"$sum": 1}
#         }},
#         {"$match": {"count": {"$gt": 1}}}
#     ]
#     for group in person_collection.aggregate(pipeline):
#         ids = group["ids"]
#         ids.sort()             # deterministic: keep the smallest _id (or change logic)
#         keep = ids[0]
#         remove = ids[1:]
#         if remove:
#             res = person_collection.delete_many({"_id": {"$in": remove}})
#             print(f"Removed {res.deleted_count} duplicates for {group['_id']}, kept {keep}")

# remove_duplicates_by_name()

def count_all_people():
    count = person_collection.count_documnets(filter={})
    print("Numbe of people", count)

def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id) #ids when entered to the function will be strings, but need to be in ObjectId in order to be read by MongoDB
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

# get_person_by_id("68fbfa40c8c0fcb9aceb45e7")

def get_age_range(min_age, max_age):
    query = {
        "$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}    #mongodb functions/ logics 
        ]
    }
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(30,40)

def project_colunms():
    """
    indicates the columns you want in your result, using 0 as false and 1 as true
    """
    columns = {"_id": 0, "first_name": 1, "last_name": 1    }
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

# project_colunms()

#### Updating documents

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id) #ids when entered to the function will be strings, but need to be in ObjectId in order to be read by MongoDB
    
    all_updates = {
        "$set": {"new_field": True}, 
        "$inc": {"age": 1}, # increment
        "$rename": {"first_name": "first", "last_name": "last"}
    }

    person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

# update_person_by_id("68fbfa40c8c0fcb9aceb45e5")

def replace_one(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new_first_name",
        "last_name": "new_last_name",
        "age": 10
    }

    person_collection.replace_one({"_id": _id}, new_doc)

# replace_one("68fbfa40c8c0fcb9aceb45e6")

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})

# delete_doc_by_id("68fbfa40c8c0fcb9aceb45e6")

#######RELATIONS

address = {
    "_id": "68fbfa40c8c0fcb9aceb45e6",
    "street": "",
    "number": 0,
    "city":"",
    "country":""
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet":{'addresses': address}})

add_address_embed("68fbfa40c8c0fcb9aceb45e8", address)