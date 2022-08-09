import os
from botbuilder.core import MessageFactory

DEFAULT_RESPONSES = {
    "error": "Something went wrong...",
    "invalid_creds": "Something went wrong with fetching the credentials...\n*Please make sure token and org ID are properly set.*",
    "empty_response": "Unable to generate response for your query",
    "invalid_token": "Invalid User Token! Please follow these steps to set your Token Key.\n1. Provide your token key by sending `Token <your token>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`",
    "invalid_org": "Org ID not found! Please follow these steps to set your Org ID.\n1. Provide your Org ID by sending `org_id <your org_id>` in the chat.\n2. Pin that message in the chat by selecting the message, then `More Actions > Pin to this conversation`",
    "setting_token": "Your are setting Token key. Next step: *Pin this message to the conversation*",
    "setting_org": "Your are setting Org ID. Next step: *Pin this message to the conversation*"
}

async def post_message(turn_context, message):
    response =  await turn_context.send_activity(
            MessageFactory.text(message)
        )
    return response

class Error_Handler():
    def __init__(self) -> None:
        pass
    
    @staticmethod
    async def credential_error(turn_context, status_code):
        if status_code == 401:
            await post_message(turn_context, DEFAULT_RESPONSES["invalid_token"])
    
        elif status_code == 404:
            await post_message(turn_context, DEFAULT_RESPONSES["invalid_org"])


class Cred_Ops():
    def __init__(self, turn_context):
        self.turn_context = turn_context

    def _fetch_channel_credentials(self):
        token = os.environ.get("MIST_CHANNEL_TOKEN", "")
        org_id = os.environ.get("MIST_ORG_ID", "")

        return token, org_id
    
    def fetch_credentials(self):
        token = org = ""
        channel_type = self.turn_context.activity.conversation.conversation_type

        if channel_type == "personal":
            token, org = self._fetch_channel_credentials()
            return token, org

        elif channel_type == "channel":
            token, org = self._fetch_channel_credentials()
            return token, org

        else:
            return token, org
    
    async def verify_credentials(self, token, org):
        if not (token and org):
            await post_message(self.turn_context, DEFAULT_RESPONSES["invalid_creds"])
            return
        
        return True


class Response_Handler:
    def __init__(self, marvis_resp):
        self.marvis_resp = marvis_resp
        self.formatted_resp_lst = []
    
    def _text_handler(self, msg_block):
        formatted_resp_text = ""

        if msg_block['response'][0].find('please visit') != -1: return

        formatted_resp_text = "\n".join(msg_block['response'])
        self.formatted_resp_lst.append(formatted_resp_text)

    def _entity_list_handler(self, msg_block):
        formatted_resp_text = ""
        for idx, resp_block in enumerate(msg_block['response'][0]['list']):
            formatted_resp_text = "{}<h2><b>{}. <u>{}</u></b></h2><b>- Details:</b> {}<br><b>- Try:</b> {}<br><br>".format(formatted_resp_text, (idx+1), resp_block['title'], resp_block['description'], resp_block['display']['phrase'])

        self.formatted_resp_lst.append(formatted_resp_text)

    def _options_handler(self, msg_block):
        formatted_resp_text = ""

        for idx, resp_block in enumerate(msg_block['response']):
            details = ""
            for details_block in resp_block['response']:
                if not details_block['type'] == 'text': continue
                details = "{}  **+** {}<br>".format(details, details_block['response'][0])
            formatted_resp_text = "{}<h2>{}. <b><u>{}</u></b> : {}\n*- Details:*\n{}\n\n".format(formatted_resp_text, (idx+1), resp_block['title'], resp_block['description'], details)

        self.formatted_resp_lst.append(formatted_resp_text)

    def _table_handler(self, msg_block):
        formatted_resp_text = ""

        for idx, resp_block in enumerate(msg_block['response'][0]['item_list']):
            name = resp_block['Name']
            site = resp_block['Site']
            mac = resp_block['Mac']
            formatted_resp_text = "{}<h2>{}. <b><u>{}</u></b></h2><b>+ Mac:</b> {}<br><b>+ Site:</b> {}<br><br>".format(formatted_resp_text, idx+1, name, mac, site)

        self.formatted_resp_lst.append(formatted_resp_text)     

    def generate_response_list(self):
        self.formatted_resp_lst = []

        for msg_block in self.marvis_resp:
            if msg_block['type'] == 'text':
                self._text_handler(msg_block)

            elif msg_block['type'] == 'entityList':
                self._entity_list_handler(msg_block)
            
            elif msg_block['type'] == 'options':
                self._options_handler(msg_block)

            elif msg_block['type'] == 'table':
                self._table_handler(msg_block)
        
        if len(self.formatted_resp_lst) == 0:
            self.formatted_resp_lst(DEFAULT_RESPONSES["empty_response"])
        
        return self.formatted_resp_lst
