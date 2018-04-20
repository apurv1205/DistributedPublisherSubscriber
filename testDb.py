import pprint
import json
from pymongo import MongoClient

port = "50000"

client = MongoClient("localhost", 27017)
db = client['client'+port]
metadata_coll = db["subscribedTopics"]

metadata_coll.drop()

post = [{"topic":"b"},{"topic":"a"}]
metadata_coll.insert_many(post)
metadata_coll.delete_one({"topic":"b"})
print "done..."