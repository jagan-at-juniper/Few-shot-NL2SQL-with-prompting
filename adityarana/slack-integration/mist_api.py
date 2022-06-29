import requests
import json

def get_payload_header(query_msg, mist_token, org_id):
    auth_key = "Token {}".format(mist_token)
    payload = json.dumps({
      "type": "phrase",
      "phrase": query_msg,
      "attempt": "first",
      "user_metadata": {
        "time_zone": "America/Los_Angeles",
        "org_id": org_id
      }
    })

    header = {
      'Content-Type': 'application/json',
      'Authorization': auth_key,
      'Access-Control-Allow-Origin': '*'
    }

    return payload, header


def post_data(query_msg, mist_token, org_id):
    url = "https://api.mistsys.com/api/v1/labs/orgs/" + org_id + "/chatbot_converse"
   
    payload, headers = get_payload_header(query_msg, mist_token, org_id)
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response

