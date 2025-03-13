import pymongo
import os

class MongoDB:
    def __init__(self):
        self.client = pymongo.MongoClient(os.getenv("MONGO_URI", ""))
        self.db = self.client["Capstone"]

    def get_collection(self, collection_name):
        return self.db[collection_name]

# Create a single database instance
mongo_service = MongoDB()
