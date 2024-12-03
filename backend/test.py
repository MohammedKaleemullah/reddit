from pymongo import MongoClient

uri = "mongodb+srv://mohammedkaleemullah06:aeqaasGuclMt0xtO@cluster0.khzha.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)

try:
    # Test connection
    client.admin.command('ping')
    print("Connected to MongoDB successfully")
except Exception as e:
    print(f"Could not connect to MongoDB: {e}")
