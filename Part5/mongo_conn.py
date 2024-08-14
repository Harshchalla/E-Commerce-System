# Some instruction
# Instructions to connect and use docker with mongo
# cd current-directory
# pip install virtualenv
# pip install pymongo
# virtualenv venv -p python3
# source ./venv -p python3

# Instruction for Docker related mongoDB stuff
# docker pull mongo
# docker run -d -p 27017:27017 --name mongo_container mongo
# docker ps
# python mongo_conn.py

from pymongo import MongoClient
import json

DATABASE_NAME = 'EcommerceDB'
COLLECTION_NAME = 'Customers'

def get_collection():
    client = MongoClient()
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]

def drop_collection():
    client = MongoClient()
    db = client[DATABASE_NAME]

    # Access the collection you want to delete
    collection = db[COLLECTION_NAME]

    # Check if collection exists before dropping
    if COLLECTION_NAME in db.list_collection_names():
        # Use the drop() method to delete the collection
        collection.drop()
        print(f"Collection '{COLLECTION_NAME}' has been deleted.")
    else:
        print(f"Collection '{COLLECTION_NAME}' does not exist.")

# Create (Insert) a new customer
def create_customer(customer_name, customer_email, customer_shipping_address, customer_region):
    collection = get_collection()
    customer_data = {
        "customer_name": customer_name,
        "customer_email": customer_email,
        "customer_shipping_address": customer_shipping_address,
        "customer_region": customer_region
    }
    result = collection.insert_one(customer_data)
    
    inserted_customer_id = result.inserted_id
    # Retrieve the inserted customer data
    inserted_customer = collection.find_one({"_id": inserted_customer_id})
    
    return inserted_customer, inserted_customer_id

# Read (Retrieve) a customer by ID
def read_customer(customer_id):
    collection = get_collection()
    customer = collection.find_one({"_id": customer_id})
    return customer

def update_customer(customer_id, new_data):
    collection = get_collection()
    
    # Fetch the customer data before the update
    before_update = collection.find_one({"_id": customer_id})
    
    # Update the customer data
    result = collection.update_one({"_id": customer_id}, {"$set": new_data})
    
    # Fetch the updated customer data
    after_update = collection.find_one({"_id": customer_id})
    
    return before_update, after_update

def delete_customer(customer_id):
    collection = get_collection()
    
    # Fetch the customer data before deletion
    deleted_customer = collection.find_one({"_id": customer_id})
    
    # Delete the customer data
    result = collection.delete_one({"_id": customer_id})
    
    # Return the deleted customer's details
    return deleted_customer

# Creation operation 
def insert_mock_data():
    """Inserts the generated Mock data in JSON file into the MongoDB."""
    collection = get_collection()

    with open('mock_data.json', 'r') as file:
        mock_data = json.load(file)

    result = collection.insert_many(mock_data)
    inserted_ids = result.inserted_ids

    # Retrieve all inserted data
    all_customers = collection.find()
    
    print("\n Insertion Operation")
    print("\n Customer Details:")

    # Print details of each customer
    for customer in all_customers:
        print(customer)
    
    # Count total number of entries
    total_entries = collection.count_documents({})
    print(f"Total number of entries: {total_entries}")

# Sample Query: Retrieve all customer names from a specific region
def retrieve_customer_names_by_region(region):
    collection = get_collection()
    # Use the find() method to query customers by region
    customers = collection.find({"customer_region": region})
    # Extract customer names and store them in a list
    customer_names = [customer["customer_name"] for customer in customers]
    return customer_names

def get_customers_detail_location():
    collection = get_collection()

    # Aggregation pipeline to filter records where customer_region = 'AR'
    pipeline = [
        {"$match": {"customer_region": "AR"}}
    ]

    # Perform aggregation using the defined pipeline
    result = collection.aggregate(pipeline)

    # Print the matching records
    print("\nCustomers in region 'AR':")
    for customer in result:
        print(customer)

def get_customers_detail_name():
    collection = get_collection()

    # Aggregation pipeline to filter records where customer_name starts with 'A'
    pipeline = [
        {
            "$match": {
                "customer_name": {"$regex": "^A", "$options": "i"}  # Using regex to match names starting with 'A' (case-insensitive)
            }
        }
    ]

    # Perform aggregation using the defined pipeline
    result = collection.aggregate(pipeline)

    # Print the matching records
    print("\nCustomers with names starting with 'A':")
    for customer in result:
        print(customer)

if __name__ == '__main__':

    # database name company
    db = MongoClient().ecommercedb
    
    drop_collection()

    insert_mock_data()

    # Insert a new customer
    new_customer_name = "Ayush Joshi"
    new_customer_email = "ayushjoshi@asu.edu"
    new_customer_shipping_address = "123 Main St"
    new_customer_region = "US"
    
    new_customer, new_customer_id = create_customer(new_customer_name, new_customer_email, new_customer_shipping_address, new_customer_region)
    print("\n Create Operation Results:")
    print("\n New customer details:")
    print(new_customer)
    # print(f"New customer ID: {new_customer_id}")

    # Retrieve and print a customer by ID
    retrieved_customer = read_customer(new_customer_id)
    print("\n Retrieve Operation Results:")
    print("\n Retrieved customer:")
    print(retrieved_customer)

    # Update the customer's information
    updated_data = {"customer_name":"Ananta Shrestha","customer_email":"ananta@asu.edu"}
    before_update, after_update = update_customer(new_customer_id, updated_data)
    print("\n Update Operation Results:")
    print(f"\n Before customer information: {before_update}")
    print(f"\n After customer information: {after_update}")

    # Delete the customer
    delete_result = delete_customer(new_customer_id)
    print("\n Delete Operation Results:")
    print(f"\nDeleted {delete_result} customer(s).")

    # Query to identify all customer from region US
    sample_region = "US"
    customer_names_in_region = retrieve_customer_names_by_region(sample_region)
    print(f"Customer names in {sample_region}:")
    for customer_name in customer_names_in_region:
        print(customer_name)

    # Aggregation Pipeline Function for queries and data retrieval operations
    get_customers_detail_location()
    get_customers_detail_name()

    print("\n The entire operation is completed.")