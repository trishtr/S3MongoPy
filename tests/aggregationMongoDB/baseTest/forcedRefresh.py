import certifi
import pymongo
import json
def forcedRefresh():

    try: 
        ca = certifi.where()
        connectionStr = ""

        client = pymongo.MongoClient(connectionStr, serverSelectionTimeoutMS=5000,tlsCAFile=ca)
        databaseName = "scm-data-lake"
        dbname = client.get_database(databaseName)
        print("Create connecting session")
        
        parsed = dbname.get_collection("parsed-hl7")


        # Update the document to trigger refresh
        query = {"eventId": "test_ARW3BT_9_1"}
        update = {"$set": {"forcedRefresh": True}}  # Set the forcedRefresh field to true
        parsed.update_one(query, update)

        # Query the document after refresh
        refreshed_document = parsed.find_one(query)

    except ConnectionError as e:
        print(f"Connection error: {e}")
        raise ConnectionError("Failed to establish a MongoDB connection")


    print("Close the connecting session")
    client.close()

if __name__ == "__main__":
    forcedRefresh()