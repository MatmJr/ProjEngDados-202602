from pymongo import MongoClient
from pymongo.server_api import ServerApi
import src.config as config

# as credenciais ficam no .env e são lidas em src/config.py
uri = config.MONGODB_URI

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi("1"))

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)

db = client["sample_mflix"]

collection = db["movies"]

print(collection.find_one())
