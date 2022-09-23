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
    Utils,
    ErrorHandler,
    ResponseHandler,
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
                    self.prompt_step,
                    self.login_process_step,
                ],
            )
        )

        self.initial_dialog_id = "WFDialog"

    def _create_adaptive_card_attachment(self, card) -> Attachment:
        card_path = os.path.join(os.getcwd(), card)
        with open(card_path, "rb") as in_file:
            card_data = json.load(in_file)

        return CardFactory.adaptive_card(card_data)

    async def prompt_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        try:
            return await step_context.begin_dialog(OAuthPrompt.__name__)
        except Exception as e:
            if 'SSO only supported in 1:1 conversations' in str(e):
                await step_context.context.send_activity(
                    MessageFactory.text("SSO is only supported in Private Conversations. Please download the bot from the store, then perform Sign-in under the private conversation. After that you can access the bot on the channel.")
                )
                return await step_context.end_dialog()

    async def login_process_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        if step_context.result:
            if not step_context.context.activity.type == 'message':
                await step_context.context.send_activity(
                    MessageFactory.text('You are now logged in...')
                )

                # check if credentials already exists in their profile
                mist_org_id, mist_token = GraphsApi.fetch_token_org(step_context.result.token)

                if (mist_org_id and mist_token):
                    return await step_context.end_dialog()

                card = "resources/credCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(card)],
                )
                await step_context.context.send_activity(message)
                return await step_context.end_dialog()
            
            ms_access_token = step_context.result.token
            print(ms_access_token)

            # check if user triggered Login card despite being logged in
            if step_context.context.activity.text == "/connect" or step_context.context.activity.text == "signin":
                await step_context.context.send_activity(
                    MessageFactory.text("You are already logged in...")
                )
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
                form_values = step_context.context.activity.value
                org_id = form_values.get('org_id', '')
                token = form_values.get('token', '')
                
                if not (org_id or token):
                    await step_context.context.send_activity(
                        MessageFactory.text("Received empty form response. Please provide credentials before submitting the card.")
                    )
                    return await step_context.end_dialog()
                
                message = DEFAULT_RESPONSES['setting_credentials'] if org_id and token else DEFAULT_RESPONSES['setting_org'] if org_id else DEFAULT_RESPONSES['setting_token']
                await step_context.context.send_activity(
                    MessageFactory.text(message)
                )
                message = GraphsApi.update_credentials(ms_access_token, org_id, token)
                await step_context.context.send_activity(
                    MessageFactory.text(message)
                )
                return await step_context.end_dialog()

            if not step_context.context.activity.text:
                return await step_context.end_dialog()

            user_query = self._clean_user_input(step_context.context.activity.text)
            mist_org_id, mist_token = GraphsApi.fetch_token_org(ms_access_token)
            print('=', mist_token, mist_org_id)
            if not (mist_org_id and mist_token):
                card = "resources/credCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(card)],
                )
                await step_context.context.send_activity(message)
                return await step_context.end_dialog()

            mist_api = MistApi(step_context.context, user_query, mist_token, mist_org_id)
            api_response = mist_api.fetch_marvis_response()        
            
            if api_response.status_code != 200:
                error_handler = ErrorHandler(step_context.context, api_response.status_code)
                await error_handler.status_code_handler()
                return await step_context.end_dialog()
            
            response_text = json.loads(api_response.text)
            marvis_response = response_text['data']

            formatted_response_lst = ResponseHandler.generate_response_list(marvis_response)
            for formatted_response in formatted_response_lst:
                await step_context.context.send_activity(
                    MessageFactory.text(formatted_response)
                )

            return await step_context.end_dialog()

        await step_context.context.send_activity(
            "Login was not successful please try again."
        )
        return await step_context.end_dialog()
    
    def _clean_user_input(self, text):
        cleaned_text = re.sub('<at>.*?</at>', '', text)
        return cleaned_text.strip()
