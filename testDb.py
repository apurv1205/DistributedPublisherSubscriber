import pprint
import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["metadata"]
metadata_coll = db["metadata1"]

post = {"test":"lol"}
post_id = metadata_coll.insert_one(post).inserted_id

print post_id
raw_input()