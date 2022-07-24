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
    
    def read_pinned_messages():
        pinned_msg_object = SLACK_CLIENT.pins_list(token=USER_TOKEN, channel=BOT_ID)
        pinned_msg_list = [item.get("message", {}).get("text", "") for item in pinned_msg_object.get("items", [])]

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
    def __init__(self, response):
        self.resp_msg = response
        self.response_blocks = []
        self.response_text = ""
    
    def get_message_block(self, response_text):
        msg_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": response_text
            }
        }  
        return msg_block
    
    def text_handler(self, msg_block):
        if msg_block['response'][0].find('please visit') != -1: return
        self.response_text = "\n".join(msg_block['response'])
        response_block = self.get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def entity_list_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response'][0]['list']):
            self.response_text = "{}*{}. `{}`*\n*- Details:* {}\n- *Try:* {}\n\n".format(self.response_text, (idx+1), resp_block['title'], resp_block['description'], resp_block['display']['phrase'])

        response_block = self.get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def options_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response']):
            details = ""
            for details_block in resp_block['response']:
                if not details_block['type'] == 'text': continue
                details = "{}  *+* {}\n".format(details, details_block['response'][0])
            self.response_text = "{}*{}. `{}`* : {}\n*- Details:*\n{}\n\n".format(self.response_text, (idx+1), resp_block['title'], resp_block['description'], details)
        response_block = self.get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def table_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response'][0]['item_list']):
            name = resp_block['Name']
            site = resp_block['Site']
            mac = resp_block['Mac']
            self.response_text = "{}*{}. `{}`*\n  *+ Mac:* {}\n  *+ Site:* {}\n\n".format(self.response_text, idx+1, name, mac, site)
        response_block = self.get_message_block(self.response_text)
        self.response_blocks.append(response_block)     

    def generate_response_blocks(self):
        for msg_block in self.resp_msg:
            self.response_text = ""

            if msg_block['type'] == 'text':
                self.text_handler(msg_block)

            elif msg_block['type'] == 'entityList':
                self.entity_list_handler(msg_block)
            
            elif msg_block['type'] == 'options':
                self.options_handler(msg_block)

            elif msg_block['type'] == 'table':
                self.table_handler(msg_block)
        
        return self.response_blocks

