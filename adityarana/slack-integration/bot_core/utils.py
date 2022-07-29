import os
import re
from configs import *

DEFAULT_RESPONSES = {
    "error": "Something went wrong...",
    "invalid_token": "Invalid User Token! Please follow these steps to set your Token Key.\n1. Provide your token key by sending `Token <your token>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`",
    "invalid_org": "Org ID not found! Please follow these steps to set your Org ID.\n1. Provide your Org ID by sending `org_id <your org_id>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`"
}

class MESSENGER():
    def __init__(self):
        pass

    def post_message(self, channel, text):
        SLACK_CLIENT.chat_postMessage(channel=channel, text=text)

    def post_blocks(self, channel, block):
        SLACK_CLIENT.chat_postMessage(channel=channel, blocks=block)

class ERROR_HANDLER():
    def __init__(self):
        self.messenger = MESSENGER()

    def status_code_handler(self, status_code, user_id):
        if status_code == 401:
            self.messenger.post_message(user_id, DEFAULT_RESPONSES["invalid_token"])
    
        elif status_code == 404:
            self.messenger.post_message(self, user_id, DEFAULT_RESPONSES["invalid_org"])

class CREDS_OPS:
    def __init__(self, user_id, channel_id, query):
        self.user_id = user_id
        self.channel_id = channel_id
        self.query = query

        self.messenger = MESSENGER()
    
    def is_setting_creds(self):
        if re.match("(?i)^(token ).{30,}", self.query.strip()):
            message = "Your are setting Token key. Next step: *Pin this message to the conversation*"
            self.messenger.post_message(self.user_id, message)
            return True
        
        elif re.match("(?i)^(org_id ).{20,}", self.query.strip()):
            message = "Your are setting Org ID. Next step: *Pin this message to the conversation*"
            self.messenger.post_message(self.user_id, message)
            return True

        return False
    
    def read_pinned_messages(self):
        pinned_msg_object = SLACK_CLIENT.pins_list(token=USER_TOKEN, channel=self.channel_id)
        pinned_msg_list = [{"time": item.get("created", 0), "message": item.get("message", {}).get("text", "")} for item in pinned_msg_object.get("items", [])]
        return pinned_msg_list

    def fetch_creds_from_pinned_msg(self):
        pinned_msg_list = self.read_pinned_messages()
        creds_dict = {"token": "", "org_id": ""}

        for item in pinned_msg_list:
            pinned_msg = item.get("message", "").strip()
            
            # reading token
            if re.match("(?i)^(token ).{30,}", pinned_msg) and not creds_dict["token"]:
                token = pinned_msg[len("token "):].strip()
                creds_dict["token"] = token
            
            # reading org_id
            if re.match("(?i)^(org_id ).{20,}", pinned_msg) and not creds_dict["org_id"]:
                org_id = pinned_msg[len("org_id "):].strip()
                creds_dict["org_id"] = org_id

        return creds_dict["token"], creds_dict["org_id"]
    
    def fetch_channel_creds(self):
        token = os.environ.get('CHANNEL_TOKEN', '')
        org_id = os.environ.get('CHANNEL_ORG_ID', '')

        return token, org_id

class RESPONSE_HANDLER():
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

