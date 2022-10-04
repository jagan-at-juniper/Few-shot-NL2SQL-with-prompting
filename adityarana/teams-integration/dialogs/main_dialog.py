from botbuilder.core import MessageFactory
from botbuilder.dialogs import (
    WaterfallDialog,
    WaterfallStepContext,
    DialogTurnResult
)
from botbuilder.dialogs.prompts import OAuthPrompt, OAuthPromptSettings, ConfirmPrompt
from botbuilder.core import CardFactory
from botbuilder.schema import (
    Attachment, 
    Activity, 
    ActivityTypes
)

from dialogs import LogoutDialog

from utils import (
    GraphsApi,
    MistApi,
    ErrorHandler,
    ResponseHandler,
    post_message,
    DEFAULT_RESPONSES
)

import re
import json
import os


class MainDialog(LogoutDialog):
    def __init__(self, connection_name: str):
        super(MainDialog, self).__init__(MainDialog.__name__, connection_name)
        self.add_dialog(
            OAuthPrompt(
                OAuthPrompt.__name__,
                OAuthPromptSettings(
                    connection_name=connection_name,
                    text="Please Sign In",
                    title="Sign In",
                    timeout=300000,
                ),
            )
        )

        self.add_dialog(ConfirmPrompt(ConfirmPrompt.__name__))

        self.add_dialog(
            WaterfallDialog(
                "WFDialog",
                [
                    self.oauth_step,
                    self.request_process_step,
                ],
            )
        )

        self.initial_dialog_id = "WFDialog"

        self.user_credentials = {}

    def _clean_user_input(self, text):
        cleaned_text = re.sub('<at>.*?</at>', '', text)
        return cleaned_text.strip()

    def _create_adaptive_card_attachment(self, card) -> Attachment:
        card_path = os.path.join(os.getcwd(), card)
        with open(card_path, "rb") as in_file:
            card_data = json.load(in_file)

        return CardFactory.adaptive_card(card_data)

    def _get_mist_api_request_metadata(self, step_context: WaterfallStepContext, org_id):
        activity = step_context.context.activity
        metadata = {
            "time_zone": activity.local_timezone,
            "utc_timestamp": str(activity.timestamp),
            "local_timestamp": str(activity.local_timestamp),
            "first_name": activity.from_property.name.split()[0],
            "user_id": activity.from_property.id,
            "conversation_type": activity.conversation.conversation_type,
            "conversation_id": activity.conversation.id,
            "org_id": org_id,
            "org_user": "True"
        }
        return metadata

    async def _set_user_credentials(self, access_token, step_context: WaterfallStepContext):
        org_id = self.user_credentials.get('org_id', '')
        actual_org_id = org_id[12:] if org_id.startswith('staging') else org_id
        token = self.user_credentials.get('token', '')
        env = org_id.split(':')[0] if org_id.startswith('staging') else self.user_credentials.get('environment', '')

        if not (org_id or token):
            await step_context.context.send_activity(
                MessageFactory.text("Received empty form response. Please provide credentials before submitting the card.")
            )
            return await step_context.end_dialog()
                
        message = DEFAULT_RESPONSES['setting_credentials'] if actual_org_id and token else DEFAULT_RESPONSES['setting_org'] if actual_org_id else DEFAULT_RESPONSES['setting_token']
        await post_message(step_context.context, message)
        try:
            message = GraphsApi.update_credentials(access_token, actual_org_id, token, env)
        except:
            await post_message(step_context.context, DEFAULT_RESPONSES['timeout_error'])
            return await step_context.end_dialog()
        
        await post_message(step_context.context, message)

    async def _signin_state_handler(self, step_context: WaterfallStepContext, access_token):
        if self.user_credentials:
            await self._set_user_credentials(access_token, step_context)
        else:
            try:
                mist_org_id, mist_token, mist_env = GraphsApi.fetch_token_org(access_token)
            except:
                await post_message(step_context.context, DEFAULT_RESPONSES['timeout_error'])
                return await step_context.end_dialog()

            # if User's credemtials are set, try throw a greet message otherwise throw card to set credentials
            if mist_org_id and mist_token and mist_env:
                await post_message(step_context.context, DEFAULT_RESPONSES['general_greet'])
            else:
                card = "resources/credCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(card)],
                )
                await step_context.context.send_activity(message)

    async def oauth_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        try:
            if step_context.context.activity.value:
                self.user_credentials = step_context.context.activity.value
            return await step_context.begin_dialog(OAuthPrompt.__name__)
        except Exception as e:
            if 'SSO only supported in 1:1 conversations' in str(e):
                await post_message(step_context.context, DEFAULT_RESPONSES['sso_error'])
                return await step_context.end_dialog()

    async def request_process_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        if step_context.result:
            ms_access_token = step_context.result.token
            print(ms_access_token)

            if step_context.context.activity.name == 'signin/verifyState':
                await self._signin_state_handler(step_context, ms_access_token)
                return await step_context.end_dialog()
            
            # check for empty text response
            if not step_context.context.activity.text:
                return await step_context.end_dialog()
            
            # check if user wants to change credentials
            if step_context.context.activity.text == "/credentials":
                card = "resources/credCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(card)],
                )
                await step_context.context.send_activity(message)
                return await step_context.end_dialog()
            
            # check if user triggered setting credentials card
            if step_context.context.activity.text == "/set_credentials":
                self.user_credentials = step_context.context.activity.value
                await self._set_user_credentials(ms_access_token, step_context)
                return await step_context.end_dialog()

            user_query = self._clean_user_input(step_context.context.activity.text)
            try:
                mist_org_id, mist_token, mist_env = GraphsApi.fetch_token_org(ms_access_token)
            except:
                await post_message(step_context.context, DEFAULT_RESPONSES['timeout_error'])
                return await step_context.end_dialog()
            print('=', mist_token, mist_org_id, mist_env)
            
            # check for mist credentials
            if not (mist_org_id and mist_token):
                card = "resources/credCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(card)],
                )
                await step_context.context.send_activity(message)
                return await step_context.end_dialog()

            # calling Mist API
            request_metadata = self._get_mist_api_request_metadata(step_context, mist_org_id)
            mist_api = MistApi(user_query, mist_token, mist_org_id, mist_env, request_metadata)
            api_response = mist_api.fetch_marvis_response()        

            # checking for response status code
            if api_response.status_code != 200:
                message = ErrorHandler.status_code_handler(api_response.status_code)
                await post_message(step_context.context, message)
                return await step_context.end_dialog()

            # reconstructing response
            marvis_response = json.loads(api_response.text)
            formatted_response_lst = ResponseHandler.generate_response_list(marvis_response)

            for formatted_response in formatted_response_lst:
                await post_message(step_context.context, formatted_response)

            return await step_context.end_dialog()

        await step_context.context.send_activity(
            "Login was not successful please try again."
        )
        return await step_context.end_dialog()
