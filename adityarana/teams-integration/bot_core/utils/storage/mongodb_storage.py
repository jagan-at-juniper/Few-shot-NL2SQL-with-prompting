from config import MongoConfigs
from cachetools import TTLCache, cached

cache = TTLCache(maxsize=200, ttl=14*24*3600)

class MongoDB:
    def __init__(self):
        pass

    @staticmethod
    def re_cache():
        cache.clear()
        MongoDB._fetch_all_user_creds()

    @cached(cache)
    def _fetch_all_user_creds():
        try:
            creds = list(MongoConfigs.COLLECTION.find())
        except Exception as e:
            print(f"Unable to read credentials from DB due to exception {e}")
            creds = []

        return creds

    @staticmethod
    def fetch_credentials_for_user(user_id):
        creds = MongoDB._fetch_all_user_creds()
        user_creds = {}
        for cred in creds:
            if cred["user_id"] == user_id: user_creds = cred

        token = user_creds.get("token", "")
        org_id = user_creds.get("org_id", "")
        return token, org_id

    @staticmethod
    def set_credentials(user_id, key, value):
        find_key = {
            'user_id': user_id
        }
        data = {
            '$set': {
                key: value
            }
        }
        try:
            MongoConfigs.COLLECTION.update_one(find_key, data, upsert=True)
        except Exception as e:
            print(f"Unable to insert. Exception {e} occurred...")
            MongoDB.re_cache()
            return False

        MongoDB.re_cache()
        return True
