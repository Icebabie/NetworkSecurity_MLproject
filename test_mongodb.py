
from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://babeldivyansh28_db_user:acoit3LQOmNQGAME@cluster0.yfrsf87.mongodb.net/?appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
# from pymongo import MongoClient
# import certifi

# uri="mongodb+srv://babeldivyansh28_db_user:Divyansh123@cluster0.7kzpksb.mongodb.net/?appName=Cluster0"

# client = MongoClient(uri, tls=True, tlsCAFile=certifi.where())
# print(client.list_database_names())
