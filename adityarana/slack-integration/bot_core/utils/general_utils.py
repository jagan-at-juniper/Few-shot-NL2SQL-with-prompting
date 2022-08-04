import os
import re
from configs import *

DEFAULT_RESPONSES = {
    "error": "Something went wrong...",
    "invalid_creds": "Something went wrong with fetching the credentials...\n*Please make sure token and org ID are properly set.*",
    "empty_response": "Unable to generate response for your query",
    "invalid_token": "Invalid User Token! Please follow these steps to set your Token Key.\n1. Provide your token key by sending `Token <your token>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`",
    "invalid_org": "Org ID not found! Please follow these steps to set your Org ID.\n1. Provide your Org ID by sending `org_id <your org_id>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`",
    "setting_token": "Your are setting Token key. Next step: *Pin this message to the conversation*",
    "setting_org": "Your are setting Org ID. Next step: *Pin this message to the conversation*"
}

def post_message(channel, text):
    SLACK_CLIENT.chat_postMessage(channel=channel, text=text)

def post_blocks(channel, block):
    SLACK_CLIENT.chat_postMessage(channel=channel, blocks=block)

class Error_Handler():
    def __init__(self):
        pass
    
    @staticmethod
    def status_code_handler(status_code, receiver):
        if status_code == 401:
            post_message(receiver, DEFAULT_RESPONSES["invalid_token"])
    
        elif status_code == 404:
            post_message(receiver, DEFAULT_RESPONSES["invalid_org"])

class Cred_Ops:
    def __init__(self, receiver, channel_id):
        self.receiver = receiver
        self.channel_id = channel_id

    def _read_pinned_messages(self):
        pinned_msg_object = SLACK_CLIENT.pins_list(token=USER_TOKEN, channel=self.channel_id)
        pinned_msg_list = [{"time": item.get("created", 0), "message": item.get("message", {}).get("text", "")} for item in pinned_msg_object.get("items", [])]
        return pinned_msg_list

    def _fetch_creds_from_pinned_msg(self):
        pinned_msg_list = self._read_pinned_messages()
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
    
    def _fetch_channel_creds(self):
        return MIST_CHANNEL_TOKEN, MIST_CHANNEL_ORG
    
    def is_setting_creds(self, query):
        if re.match("(?i)^(token ).{30,}", query.strip()):
            message = DEFAULT_RESPONSES["setting_token"]
            post_message(self.receiver, message)
            return True
        
        elif re.match("(?i)^(org_id ).{20,}", query.strip()):
            message = DEFAULT_RESPONSES["setting_org"]
            post_message(self.receiver, message)
            return True

        return False
    
    def fetch_credentials(self, channel_type):
        token = org_id = ""

        if channel_type == 'im':
            token, org_id = self._fetch_creds_from_pinned_msg()

        elif channel_type == 'channel':
            token, org_id = self._fetch_channel_creds()
        
        return token, org_id

    def verify_credentials(self, token, org_id):
        if not (token or org_id):
            post_message(self.receiver, DEFAULT_RESPONSES['invalid_creds'])
            return False

        return True

class Response_Handler():
    def __init__(self, response):
        self.resp_msg = response.get('data', [])
        self.response_blocks = []
        self.response_text = ""
    
    def _get_message_block(self, response_text):
        msg_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": response_text
            }
        }  
        return msg_block
    
    def _text_handler(self, msg_block):
        if msg_block['response'][0].find('please visit') != -1: return
        self.response_text = "\n".join(msg_block['response'])
        response_block = self._get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def _entity_list_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response'][0]['list']):
            self.response_text = "{}*{}. `{}`*\n*- Details:* {}\n- *Try:* {}\n\n".format(self.response_text, (idx+1), resp_block['title'], resp_block['description'], resp_block['display']['phrase'])

        response_block = self._get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def _options_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response']):
            details = ""
            for details_block in resp_block['response']:
                if not details_block['type'] == 'text': continue
                details = "{}  *+* {}\n".format(details, details_block['response'][0])
            self.response_text = "{}*{}. `{}`* : {}\n*- Details:*\n{}\n\n".format(self.response_text, (idx+1), resp_block['title'], resp_block['description'], details)
        response_block = self._get_message_block(self.response_text)
        self.response_blocks.append(response_block)

    def _table_handler(self, msg_block):
        for idx, resp_block in enumerate(msg_block['response'][0]['item_list']):
            name = resp_block['Name']
            site = resp_block['Site']
            mac = resp_block['Mac']
            self.response_text = "{}*{}. `{}`*\n  *+ Mac:* {}\n  *+ Site:* {}\n\n".format(self.response_text, idx+1, name, mac, site)
        response_block = self._get_message_block(self.response_text)
        self.response_blocks.append(response_block)     

    def generate_response_blocks(self):
        for msg_block in self.resp_msg:
            self.response_text = ""

            if msg_block['type'] == 'text':
                self._text_handler(msg_block)

            elif msg_block['type'] == 'entityList':
                self._entity_list_handler(msg_block)
            
            elif msg_block['type'] == 'options':
                self._options_handler(msg_block)

            elif msg_block['type'] == 'table':
                self._table_handler(msg_block)
        
        if len(self.response_blocks) == 0:
            response_block = self._get_message_block(DEFAULT_RESPONSES["empty_response"])
            self.response_blocks.append(response_block)
        
        return self.response_blocks

