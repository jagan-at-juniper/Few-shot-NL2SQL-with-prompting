import requests
import json

class Mist_Api:
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
