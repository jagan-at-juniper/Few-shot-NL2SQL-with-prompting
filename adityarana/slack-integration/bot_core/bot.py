from bot_core.mist_api import *
from bot_core.utils import MESSENGER, CREDS_OPS, ERROR_HANDLER, RESPONSE_HANDLER

class BOT_PROCESSOR():
    def __init__(self, event):
        self.user_id = event.get("user", "")
        self.channel_id = event.get("channel", "")
        self.query = event.get("text", "")
        self.channel_type = event.get("channel_type", "")

        self.token = ""
        self.org_id = ""
        self.receiver = ""

        self.messenger = MESSENGER()
        self.error_handler = ERROR_HANDLER()
        self.credentials = CREDS_OPS(self.user_id, self.channel_id, self.query)
    
    def fetch_credentials(self):
        if self.channel_type == 'im':
            self.receiver = self.user_id

            if self.credentials.is_setting_creds(): return
            self.token, self.org_id = self.credentials.fetch_creds_from_pinned_msg()

        elif self.channel_type == 'channel':
            self.receiver = self.channel_id
            self.token, self.org_id = self.credentials.fetch_channel_creds()

        else:
            return
        
        return True

    def process_query(self):
        if not self.fetch_credentials(): return

        print(f"Token: {self.token} \nOrg ID: {self.org_id}")

        marvis_resp = post_data(self.query, self.token, self.org_id)
        # handling error response code
        if marvis_resp.status_code == 404 or marvis_resp.status_code == 401:
            self.error_handler.status_code_handler(marvis_resp.status_code, self.receiver)
            return

        response = json.loads(marvis_resp.text)
        resp_msg = response['data']

        response_handler = RESPONSE_HANDLER(resp_msg)
        response_blocks = response_handler.generate_response_blocks()

        if len(response_blocks) == 0:
            self.messenger.post_message(self.receiver, "Unable to generate response for you query.")
            return
        self.messenger.post_blocks(self.receiver, response_blocks)