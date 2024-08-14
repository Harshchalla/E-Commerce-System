from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference

import json

DATABASE_NAME = 'EcommerceDB'
COLLECTION_NAME = 'Orders'

w_concern = WriteConcern("majority", wtimeout=10)

dist_data = {
    "customer_name": "Aubrey_DIST",
    "customer_email": "adudding2r@hc360.com_DIST",
    "customer_shipping_address": "93 Coolidge Place_DIST",
    "customer_region": "BR_DIST",
    "status": "processed"  # Updated status to "processed"
}

client_primary = MongoClient('mongodb://localhost:27017/?directConnection=true&replicaSet=myReplicaSet')
client_secondary = MongoClient('mongodb://localhost:27018/?directConnection=true&replicaSet=myReplicaSet')
client_arbiter = MongoClient('mongodb://localhost:27019/?directConnection=true&replicaSet=myReplicaSet')

def get_collection(client):
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]

def find_all_documents_in_node(client, preference):
    collection = get_collection(client)
    cursor = collection.with_options(read_preference=preference).find()
    return list(cursor)

def update_orders_with_customer_info(client):
    try:
        with client.start_session() as session:
            session.start_transaction(
                read_concern=ReadConcern("local"),
                write_concern=w_concern,
                read_preference=ReadPreference.PRIMARY
            )

            orders_collection = get_collection(client)

            # Update orders with customer-related information and set status
            orders_collection.update_one(
                {"$or": [
                    {"customer_name": dist_data["customer_name"]},
                    {"customer_email": dist_data["customer_email"]}
                ]},
                {"$set": {"status": dist_data["status"]}},  # Set status from dist_data
                session=session
            )

            session.commit_transaction()
            print("Transaction committed successfully.")
    except Exception as e:
        print(f"Transaction aborted due to error: {e}")
        session.abort_transaction()

if __name__ == '__main__':
    with client_primary.start_session() as session:
        session.with_transaction(
            lambda s: get_collection(client_primary).insert_one(dist_data, session=s),
            read_concern=ReadConcern("local"),
            write_concern=w_concern,
            read_preference=ReadPreference.PRIMARY
        )
    
    update_orders_with_customer_info(client_primary)

    print("Documents in Primary Node:")
    primary_documents = find_all_documents_in_node(client_primary, ReadPreference.PRIMARY)
    for doc in primary_documents:
        print(doc)

    print("\nDocuments in Secondary Node:")
    secondary_documents = find_all_documents_in_node(client_secondary, ReadPreference.SECONDARY)
    for doc in secondary_documents:
        print(doc)

    print("\nDocuments in Arbiter Node:")
    arbiter_documents = find_all_documents_in_node(client_arbiter, ReadPreference.PRIMARY)
    for doc in arbiter_documents:
        print(doc)