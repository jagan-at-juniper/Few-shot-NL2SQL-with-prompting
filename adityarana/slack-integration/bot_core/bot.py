import time
from bot_core.mist_api import *
from bot_core.utils import MESSENGER, CREDS_OPS, ERROR_HANDLER, RESPONSE_HANDLER

class BOT_PROCESSOR():
    def __init__(self):
        self.messenger = MESSENGER()
        self.error_handler = ERROR_HANDLER()

    def process_query(self, user_id, channel_id, query):
        # intializing credentials class 
        start = time.time()
        credentials = CREDS_OPS(user_id, channel_id, query)

        # checking if user is setting credentials
        if credentials.is_setting_creds(): return

        # fetching token and org_id
        token, org_id = credentials.fetch_creds()
        print(f"Token: {token} \nOrg ID: {org_id}")

        marvis_resp = post_data(query, token, org_id)
        # handling error response code
        if marvis_resp.status_code == 404 or marvis_resp.status_code == 401:
            self.error_handler.status_code_handler(marvis_resp.status_code, user_id)
            return

        response = json.loads(marvis_resp.text)
        resp_msg = response['data']

        response_handler = RESPONSE_HANDLER(resp_msg)
        response_blocks = response_handler.generate_response_blocks()

        if len(response_blocks) == 0:
            self.messenger.post_message(user_id, "Unable to generate response for you query.")
            return
        self.messenger.post_blocks(user_id, response_blocks)