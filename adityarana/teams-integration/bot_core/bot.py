from botbuilder.core import ActivityHandler, TurnContext
from h11 import ERROR
from bot_core.utils import (
    Error_Handler,
    Response_Handler,
    Cred_Ops,
    post_message,
    fetch_marvis_response
)
import json

def _clean_user_input(text):
    text = text.strip()
    cleaned_text = text.replace("<at>Marvis-test</at>", "").strip() if text.find("<at>Marvis-test</at>") >= 0 else text
    return cleaned_text

class BOT_PROCESSOR(ActivityHandler):
    def _clean_user_input(self, text):
        text = text.strip()
        cleaned_text = text.replace("<at>Marvis-test</at>", "").strip() if text.find("<at>Marvis-test</at>") >= 0 else text
        return cleaned_text

    async def on_message_activity(self, turn_context: TurnContext):
        # clean input message
        user_msg = _clean_user_input(turn_context.activity.text)

        # initialising class for credentials
        credentials = Cred_Ops(turn_context)

        # fetch credentials
        token, org = credentials.fetch_credentials()

        # verify credentials
        if not await credentials.verify_credentials(token, org): return
        
        api_response = fetch_marvis_response(user_msg, token, org)

        # handling error response code
        if api_response.status_code != 200:
            await Error_Handler.credential_error(turn_context, api_response.status_code)
            return
        
        response_text = json.loads(api_response.text)
        marvis_response = response_text['data']

        # creating simple text response for user
        response_handler = Response_Handler(marvis_response)
        formatted_response_lst = response_handler.generate_response_list()

        for formatted_response in formatted_response_lst:
            response = await post_message(turn_context, formatted_response)
        return response
