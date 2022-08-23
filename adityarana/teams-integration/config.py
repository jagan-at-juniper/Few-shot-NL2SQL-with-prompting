import os
from pathlib import Path
from dotenv import load_dotenv
import pymongo


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

class DefaultConfigs:
    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
    MIST_TOKEN = os.environ.get("MIST_CHANNEL_TOKEN", "")
    MIST_ORG = os.environ.get("MIST_ORG_ID", "")

class MongoConfigs:
    MONGODB_PW = os.environ.get("MongoDB_pw", "")
    MONGODB_URL = "mongodb+srv://rana699:{}@cluster0.oujmu.mongodb.net/?retryWrites=true&w=majority".format(MONGODB_PW)
    MONGO_CLIENT = pymongo.MongoClient(MONGODB_URL)
    COLLECTION = MONGO_CLIENT["TeamsBotDB"]["credentials"]
    # other databse configs go here
