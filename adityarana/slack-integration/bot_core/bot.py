import json
from .utils import (
    Error_Handler,
    Cred_Ops,
    Response_Handler,
    fetch_marvis_response,
    post_blocks
)

class BOT_PROCESSOR():
    def __init__(self, event):
        self.user_id = event.get("user", "")
        self.channel_id = event.get("channel", "")
        self.user_query = event.get("text", "")
        self.channel_type = event.get("channel_type", "")

        self.receiver = self._get_receiver()
    
    def _get_receiver(self):
        receiver = self.user_id if self.channel_type == "im" else self.channel_id if self.channel_type == "channel" else ""
        return receiver

    def process_query(self):
        credentials = Cred_Ops(self.receiver, self.channel_id)

        # checking if user is setting credentials if using private conversation
        if self.channel_type == "im":
            if credentials.is_setting_creds(self.user_query): return

        # fetching credentials
        token, org_id = credentials.fetch_credentials(self.channel_type)

        # verify credentials
        if not credentials.verify_credentials(token, org_id): return

        # fetching marvis response for user query
        api_response = fetch_marvis_response(self.user_query, token, org_id)

        # handling error response code
        if api_response.status_code != 200:
            Error_Handler.status_code_handler(api_response.status_code, self.receiver)
            return

        response_data = json.loads(api_response.text)

        response_handler = Response_Handler(response_data)
        formatted_response_blocks = response_handler.generate_response_blocks()

        post_blocks(self.receiver, formatted_response_blocks)
