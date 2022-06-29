import json
from configs import *

def post_message(channel, text):
    SLACK_CLIENT.chat_postMessage(channel=channel, text=text)

def post_blocks(channel, block):
    SLACK_CLIENT.chat_postMessage(channel=channel, blocks=block)

class CREDS_OPS:
    def __init__(self, user_id, query):
        self.user_id = user_id
        self.query = query
        try:
            self.all_users_creds = json.load(open(CREDS_FILE_PATH))
        except:
            self.all_users_creds = {}
            message = "SERVER ERROR!!! Unable to fetch user credentials..."
            post_message(self.user_id, message)

    def fetch_creds(self):
        token = self.fetch_token()
        org_id = self.fetch_orgId()

        return token, org_id

    def fetch_token(self):
        token = self.all_users_creds.get(self.user_id, {"token": "", "org_id": ""}).get("token", "")
        return token

    def fetch_orgId(self):
        org_id = self.all_users_creds.get(self.user_id, {"token": "", "org_id": ""}).get("org_id", "")
        return org_id

    def verify_creds(self, token, org_id):
        if not token:
            message = "User Token not found! Please provide your token by sending `Token <your token>`"
            post_message(self.user_id, message)
            return False
    
        if not org_id:
            message = "Org Details not found! Please provide your Org ID by sending `Org <your org_id>`"
            post_message(self.user_id, message)
            return False

        return True

    def set_token(self, token):
        user_cred = self.all_users_creds.get(self.user_id, {"token": "", "org_id": ""})
        user_cred["token"] = token
        
        self.all_users_creds[self.user_id] = user_cred
        
        try:
            json.dump(self.all_users_creds, open(CREDS_FILE_PATH, "w"))
            message = "*Token Key is Set!!!!*"
            post_message(self.user_id, message)

        except:
            message = "Sorry. Unable to set your token..."
            post_message(self.user_id, message)
    
    def set_org(self, org_id):
        user_cred = self.all_users_creds.get(self.user_id, {"token": "", "org_id": ""})
        user_cred["org_id"] = org_id
        
        self.all_users_creds[self.user_id] = user_cred
        
        try:
            json.dump(self.all_users_creds, open(CREDS_FILE_PATH, "w"))
            message = "*Org ID is Set!!!!*"
            post_message(self.user_id, message)

        except:
            message = "Sorry. Unable to set your Org ID..."
            post_message(self.user_id, message)

    def is_setting_creds(self):
        if self.query.lower().startswith("token "):
            token = self.query[6:]
            self.set_token(token)
            return True
        
        if self.query.lower().startswith("org "):
            org_id = self.query[4:]
            self.set_org(org_id)
            return True
        
        return False

class ResponseHandler():
    def text_handler():
        pass

    def entity_list_handler():
        pass

    def options_handler():
        pass

    def table_handler():
        pass     
