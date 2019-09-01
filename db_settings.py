from pymongo import MongoClient, errors
import config

mongo_client = MongoClient(host=config.MONGO_HOST, username=config.MONGO_USERNAME, password=config.MONGO_PASSWORD)

try:
    mongo_client.admin.command('ismaster')
    print("Connected to MongoDB")
except errors.ConnectionFailure as mongo_err:
    raise Exception("Failed to connect to mongo:{}".format(mongo_err))

mongo_db = mongo_client['{}'.format(config.MONGO_DATABASE)]
print(mongo_db.movies_initial.find_one())