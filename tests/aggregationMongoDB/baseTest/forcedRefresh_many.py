import certifi
import pymongo
import json

def forcedRefresh_many():

    try: 
        ca = certifi.where()
        connectionStr = ""

        client = pymongo.MongoClient(connectionStr, serverSelectionTimeoutMS=5000,tlsCAFile=ca)
        databaseName = "data-lake"
        dbname = client.get_database(databaseName)
        print("Create connecting session")
        
        parsed = dbname.get_collection("parsed-hl7")

        query_find = {'clientId': "APPRHS", 'timestamp': {'$gte': '2023-02-01', '$lt':'2023-02-28z'}, 'forcedRefresh':{'$exists': True}}

        # query_find = {'clientId': "APPRHS", 'timestamp': {'$gte': '2023-02-01', '$lte':'2023-02-28'}
        
        refresh_count = parsed.count_documents(query_find)
        print(refresh_count)
        # 49605
        #52147

        update = {"$set": {"forcedRefresh": True}}
        parsed.update_many(query_find, update)

        query_delete = {'clientId': "APPRHS", 'timestamp': {'$gte': '2023-02-01', '$lt':'2023-02-28z'}, 'forcedRefresh': {'$exists' : False}}
        # query_delete_2 = {'clientId': "APPRHS", 'timestamp': {'$gte': '2023-02-01', '$lt':'2023-02-28z'}, 'forcedRefresh': {'lt' : '2024-03-26T09:40:00' }}

        parsed.delete_many(query_delete)
        # parsed.delete_many(query_delete_2)
    
    except ConnectionError as e:
        print(f"Connection error: {e}")
        raise ConnectionError("Failed to establish a MongoDB connection")


    print("Close the connecting session")
    client.close()

if __name__ == "__main__":
    estimateTimeRefresh()

    



   