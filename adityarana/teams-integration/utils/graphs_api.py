from .general_utils import DEFAULT_RESPONSES, post_message
import requests
import json

def make_request(method, url, headers, payload={}):
    try:
        response = requests.request(method, url, headers=headers, data=payload, timeout=5)
        return response
    except requests.exceptions.Timeout as e:
        print("Graphs API Timeout Exception: {}".format(e))
        raise Exception("Graph API Timeout Exception...")
    except requests.exceptions.RequestException as e:
        print("Graphs API Request Exception occurred: {}".format(e))
        raise Exception("Graph API Request Exception...")
    except Exception as e:
        print("Graphs API some other occurred: {}".format(e))
        raise Exception("Graph API Exception...")


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

        response = make_request("GET", GraphsApi.PROFILE_EXTENSION_URL, headers, payload)
        response = json.loads(response.text)
        
        if not response.get('value', []):
            payload = json.dumps({
                "extensionName": "MistCredentials",
                "mist_token": "",
                "mist_org_id": "",
                "mist_env": ""
            })
            response = make_request("POST", GraphsApi.PROFILE_EXTENSION_URL, headers, payload)
            return '', '', ''


        for extension in response.get('value', []):
            if extension.get('id', '') == 'MistCredentials':
                mist_token = extension.get('mist_token', '')
                mist_org_id = extension.get('mist_org_id', '')
                mist_env = extension.get('mist_env', '')
                return mist_org_id, mist_token, mist_env
        
        return '', '', ''
    
    @staticmethod
    def update_credentials(access_token, new_org_id, new_token, new_env):
        old_org_id, old_token, old_env = GraphsApi.fetch_token_org(access_token)
        url = GraphsApi.PROFILE_EXTENSION_URL + "/MistCredentials"

        payload = json.dumps({
            "mist_token": new_token if new_token else old_token,
            "mist_org_id": new_org_id if new_org_id else old_org_id,
            "mist_env": new_env if new_env else old_env
        })
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = make_request("PATCH", url, headers, payload)

        message = DEFAULT_RESPONSES["setting_creds_success"] if response.status_code >= 200 and response.status_code < 300 else DEFAULT_RESPONSES["setting_creds_error"]

        return message
