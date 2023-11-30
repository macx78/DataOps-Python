from pymongo import MongoClient
from pymongo.operations import InsertOne
import os

#source mongodb configuration
source_username = os.environ.get('SOURCE_DB_USERNAME')
source_password = os.environ.get('SOURCE_DB_PASSWORD')
source_host = os.environ.get('SOURCE_DB_HOST')
source_port = os.environ.get('SOURCE_DB_PORT')
source_auth_db = os.environ.get('SOURCE_DB_AUTH')
source_database = os.environ.get('SOURCE_DB_NAME')

# Connect to the source MongoDB instance
source_client = MongoClient(f'mongodb+srv://{source_username}:{source_password}@{source_host}/{source_auth_db}?readPreference=primary&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1')
source_db = source_client[source_database]

# Connect to the destination MongoDB instance
destination_client = MongoClient('mongodb://general-mongodb:27017')
destination_db = destination_client['BT']

#get a list of collections in the source db
collections = source_db.list_collection_names()

te_collections = ('collection1', 'collection2', 'collection3', 'collection4')

for collection_name in collections:
    if collection_name in te_collections and destination_db[collection_name].count_documents({}) == 0:
        print(f"Collection '{collection_name}' copied started....")
        source_collection = source_db[collection_name]
        destination_collection = destination_db[collection_name]

        # Retrieve documents from the source collection
        documents = source_collection.find()

        # Copy documents to the destination collection
        #for document in documents:
        #    destination_collection.insert_one(document)
        bulk_operations = [InsertOne(document) for document in documents]

        destination_collection.bulk_write(bulk_operations)

        print(f"Collection '{collection_name}' copied completed!")
    else:
        print(f"Skipped collection: '{collection_name}'!")

# Close the connections
source_client.close()
destination_client.close()