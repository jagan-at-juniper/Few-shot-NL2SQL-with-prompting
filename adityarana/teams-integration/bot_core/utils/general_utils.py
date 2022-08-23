import os
import re
from botbuilder.core import MessageFactory
from bot_core.utils.storage import LocalStorage, MongoDB, CustomStorage

STORAGE = LocalStorage

DEFAULT_RESPONSES = {
    "error": "Something went wrong...",
    "invalid_creds": "Something went wrong with fetching the credentials...\n*Please make sure token and org ID are properly set.*",
    "empty_response": "Unable to generate response for your query",
    "invalid_token": "Invalid User Token! Please provide correct token by sending `token <token_key>`",
    "invalid_org": "Org ID not found! Please provide correcr Org ID by sending `org_id <org_id>",
    "setting_token": "Your are setting Token key. Please hold on...",
    "setting_org": "Your are setting Org ID. Please hold on...",
    "setting_creds_success": "Credentials are set successfully!!!",
    "setting_creds_error": "Unable to set the Credentials :(",
    "timeout_error": "I am currently having trouble responding. Please try later :(",
    "server_error": "Some error occurred. Please try later. If the issue persist, contact the Administrator",
}

async def post_message(turn_context, message):
    response =  await turn_context.send_activity(
            MessageFactory.text(message)
        )
    return response

class Error_Handler():
    def __init__(self, turn_context, status_code):
        self.turn_context = turn_context
        self.status_code = status_code
    
    async def _credential_error(self):
        if self.status_code == 401:
            await post_message(self.turn_context, DEFAULT_RESPONSES["invalid_token"])
    
        elif self.status_code == 404:
            await post_message(self.turn_context, DEFAULT_RESPONSES["invalid_org"])
    
    async def _timeout_error(self):
        await post_message(self.turn_context, DEFAULT_RESPONSES["timeout_error"])
    
    async def _server_error(self):
        await post_message(self.turn_context, DEFAULT_RESPONSES["server_error"])

    async def status_code_handler(self):
        if self.status_code == 401 or self.status_code == 404:
            await self._credential_error()

        elif self.status_code == 504:
            await self._timeout_error()

        else:
            await self._server_error()


class Cred_Ops():
    def __init__(self, turn_context):
        self.turn_context = turn_context
        self.user_id = turn_context.activity.from_property.id

    def _fetch_channel_credentials(self):
        token = os.environ.get("MIST_CHANNEL_TOKEN", "")
        org_id = os.environ.get("MIST_CHANNEL_ORG_ID", "")
        return token, org_id

    def _fetch_personal_credentials(self):
        token, org_id = STORAGE.fetch_credentials_for_user(self.user_id)
        return token, org_id

    def fetch_credentials(self, channel_type):
        token = org = ""
        if channel_type == "personal":
            token, org = self._fetch_personal_credentials()
        elif channel_type == "channel":
            token, org = self._fetch_channel_credentials()
    
        return token, org

    async def is_setting_credentials(self, query):
        if re.match("(?i)^(token ).{30,}", query):
            message = DEFAULT_RESPONSES["setting_token"]
            await post_message(self.turn_context, message)
            message = DEFAULT_RESPONSES["setting_creds_success"] if STORAGE.set_credentials(self.user_id, "token", re.sub("(?i)token", "", query).strip()) else DEFAULT_RESPONSES["setting_creds_error"]
        elif re.match("(?i)^(org_id ).{20,}", query):
            message = DEFAULT_RESPONSES["setting_org"]
            await post_message(self.turn_context, message)
            message = DEFAULT_RESPONSES["setting_creds_success"] if STORAGE.set_credentials(self.user_id, "org_id", re.sub("(?i)org_id", "", query).strip()) else DEFAULT_RESPONSES["setting_creds_error"]
        else:
            return False

        await post_message(self.turn_context, message)
        return True
    
    async def verify_credentials(self, token, org):
        if not (token and org):
            await post_message(self.turn_context, DEFAULT_RESPONSES["invalid_creds"])
            return False

        return True


class Response_Handler:
    def __init__(self):
        pass
    
    @staticmethod
    def generate_response_list(marvis_resp):
        formatted_resp_lst = []

        for num, msg_block in enumerate(marvis_resp):
            if msg_block.get('type') in ['text']:
                formatted_resp_text = ""
                formatted_resp_text = "\n".join(msg_block['response'])
                formatted_resp_lst.append(formatted_resp_text)

            elif isinstance(msg_block, dict):
                formatted_response_text = ""
                for key in msg_block.keys():
                    if key == 'plain_text':
                        formatted_response_text = "{}{}<br>".format(formatted_response_text, msg_block[key])
                    
                    elif key in ['text', 'category', 'reason', 'recommendation']:
                        formatted_response_text = "{}<b> + Details:</b> {}<br>".format(formatted_response_text, str(msg_block[key])) if key in ['text'] else "{}<b> + {}:</b> {}<br>".format(formatted_response_text, str(key).capitalize(), str(msg_block[key]))

                    else:
                        formatted_response_text = "{}<b> + {}:</b> {}<br>".format(formatted_response_text, str(key).capitalize(), str(msg_block[key]))

                formatted_resp_lst.append(formatted_response_text)

        if len(formatted_resp_lst) == 0:
            formatted_resp_lst.append(DEFAULT_RESPONSES["empty_response"])
        
        return formatted_resp_lst
