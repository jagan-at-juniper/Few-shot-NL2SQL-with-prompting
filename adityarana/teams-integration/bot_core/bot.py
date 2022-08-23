from botbuilder.core import ActivityHandler, TurnContext
from bot_core.utils import (
    Error_Handler,
    Response_Handler,
    Cred_Ops,
    Mist_Api,
    post_message
)
import json

class BOT_PROCESSOR(ActivityHandler):
    def _clean_user_input(self, text, app_name):
        text = text.strip()
        cleaned_text = text.replace("<at>{}</at>".format(app_name), "").strip()
        return cleaned_text

    async def on_message_activity(self, turn_context: TurnContext):
        credentials = Cred_Ops(turn_context)

        # clean input message
        user_msg = self._clean_user_input(turn_context.activity.text, turn_context.activity.recipient.name)

        channel_type = turn_context.activity.conversation.conversation_type

        # check if user is setting credentials in personal chat
        if channel_type == "personal":
            if await credentials.is_setting_credentials(user_msg): return

        # fetch credentials
        token, org = credentials.fetch_credentials(channel_type)

        # verify credentials
        if not await credentials.verify_credentials(token, org): return

        mist_api = Mist_Api(turn_context, user_msg, token, org)
        api_response = mist_api.fetch_marvis_response()

        # handling error response code
        if api_response.status_code != 200:
            error_handler = Error_Handler(turn_context, api_response.status_code)
            await error_handler.status_code_handler()
            return

        response_text = json.loads(api_response.text)
        marvis_response = response_text['data']

        # creating simple text response for user
        formatted_response_lst = Response_Handler.generate_response_list(marvis_response)
        for formatted_response in formatted_response_lst:
            response = await post_message(turn_context, formatted_response)

        return response
