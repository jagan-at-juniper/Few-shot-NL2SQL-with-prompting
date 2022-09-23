from botbuilder.core import MessageFactory
import requests
import json
import re

DEFAULT_RESPONSES = {
    "error": "Something went wrong...",
    "invalid_creds": "Something went wrong with fetching the credentials...\n*Please make sure token and org ID are properly set.*<br>- Send <b>'Token \<Secret Key\>'</b> to set mist token<br>- Send <b>'Org_id \<Org ID\>'</b> to set your Org ID",
    "empty_response": "Unable to generate response for your query",
    "invalid_token": "Invalid User Token! Please provide correct token by sending `/credentials` in the chat",
    "invalid_org": "Invalid Org ID! Please provide correct Org ID by sending `/credentials` in the chat",
    "setting_credentials": "Setting your credentials. Please hold on...",
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

class GraphsApi:
    PROFILE_EXTENSION_URL = "https://graph.microsoft.com/v1.0/me/extensions"

    def __init__(self) -> None:
        pass

    @staticmethod
    def fetch_token_org(access_token):
        payload={}
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.request("GET", GraphsApi.PROFILE_EXTENSION_URL, headers=headers, data=payload)
        response = json.loads(response.text)

        if not response.get('value', []):
            payload = json.dumps({
                "extensionName": "MistCredentials",
                "mist_token": "",
                "mist_org_id": ""
            })
            response = requests.request("POST", GraphsApi.PROFILE_EXTENSION_URL, headers=headers, data=payload)
            return '', ''


        for extension in response.get('value', []):
            if extension.get('id', '') == 'MistCredentials':
                mist_token = extension.get('mist_token', '')
                mist_org_id = extension.get('mist_org_id', '')
                return mist_org_id, mist_token
        
        return '', ''
    
    @staticmethod
    def update_credentials(access_token, new_org_id, new_token):
        old_org_id, old_token = GraphsApi.fetch_token_org(access_token)
        url = GraphsApi.PROFILE_EXTENSION_URL + "/MistCredentials"

        payload = json.dumps({
            "mist_token": new_token if new_token else old_token,
            "mist_org_id": new_org_id if new_org_id else old_org_id,
        })
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.request("PATCH", url, headers=headers, data=payload)

        message = DEFAULT_RESPONSES["setting_creds_success"] if response.status_code >= 200 and response.status_code < 300 else DEFAULT_RESPONSES["setting_creds_error"]

        return message


class Utils():
    def __init__(self):
        pass

    @staticmethod
    def is_setting_credentials(query):
        if re.match("(?i)^(token ).{30,}", query):
            response = {
                "message": DEFAULT_RESPONSES["setting_token"],
                "type": 'token',
                "value": re.sub("(?i)token", "", query).strip()
            }
        elif re.match("(?i)^(org_id ).{20,}", query):
            response = {
                "message": DEFAULT_RESPONSES["setting_org"],
                "type": 'org_id',
                "value": re.sub("(?i)org_id", "", query).strip()
            }
        else:
            return {}

        return response
    
    @staticmethod
    def verify_credentials(token, org_id):
        if not (token and org_id):
            message = DEFAULT_RESPONSES["invalid_creds"]
            return message
    

class MistApi:
  def __init__(self, turn_context, query, mist_token, org_id):
    self.turn_context = turn_context
    self.user_query = query
    self.mist_token = mist_token
    self.org_id = org_id

  def _get_request_metadata(self):
    metadata = {
      "time_zone": self.turn_context.activity.local_timezone,
      "utc_timestamp": str(self.turn_context.activity.timestamp),
      "local_timestamp": str(self.turn_context.activity.local_timestamp),
      "first_name": self.turn_context.activity.from_property.name.split()[0],
      "user_id": self.turn_context.activity.from_property.id,
      "conversation_type": self.turn_context.activity.conversation.conversation_type,
      "conversation_id": self.turn_context.activity.conversation.id,
      "org_id": self.org_id,
      "org_user": "True"
    }
    return metadata

  def _get_payload_header(self):
    auth_key = "Token {}".format(self.mist_token)
    metadata = self._get_request_metadata()
    payload = json.dumps({
      "type": "phrase",
      "phrase": self.user_query,
      "attempt": "first",
      "user_metadata": metadata
    })
    header = {
      'Content-Type': 'application/json',
      'Authorization': auth_key,
      'Access-Control-Allow-Origin': '*'
    }
  
    return payload, header


  def fetch_marvis_response(self):
    url = "https://api.mistsys.com/api/v1/labs/orgs/" + self.org_id + "/chatbot_converse"
    payload, headers = self._get_payload_header()

    try:
      response = requests.request("POST", url, headers=headers, data=payload, timeout=10)
    except requests.exceptions.RequestException as e:
      print("Exception occurred: {}".format(e))
      response = requests.Response()
      response.status_code = 504
      return response
    except Exception as e:
      response = requests.Response()
      response.status_code = 500
      return response

    return response

class ErrorHandler():
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


class ResponseHandler:
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
