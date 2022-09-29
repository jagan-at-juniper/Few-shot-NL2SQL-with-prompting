from environment_mapping import API_ENDPOINTS
import requests
import json

class MistApi:
  def __init__(self, query, mist_token, org_id, env, metadata):
    self.user_query = query
    self.mist_token = mist_token
    self.org_id = org_id
    self.env = env
    self.metadata = metadata

  def _get_payload_header(self):
    auth_key = "Token {}".format(self.mist_token)
    payload = json.dumps({
      "type": "phrase",
      "phrase": self.user_query,
      "attempt": "first",
      "user_metadata": self.metadata
    })
    header = {
      'Content-Type': 'application/json',
      'Authorization': auth_key,
      'Access-Control-Allow-Origin': '*'
    }
  
    return payload, header

  def fetch_marvis_response(self):
    url = API_ENDPOINTS[self.env] + "/api/v1/labs/orgs/" + self.org_id + "/chatbot_converse"
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
