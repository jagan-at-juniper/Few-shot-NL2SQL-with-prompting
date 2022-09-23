from typing import List
from botbuilder.core import (
    ConversationState,
    UserState,
    TurnContext,
    CardFactory
)
from botbuilder.dialogs import Dialog
from botbuilder.schema import ChannelAccount, Attachment, Activity, ActivityTypes

from helpers.dialog_helper import DialogHelper
from .dialog_bot import DialogBot

import os, json


class TeamsBot(DialogBot):
    def __init__(
        self,
        conversation_state: ConversationState,
        user_state: UserState,
        dialog: Dialog,
    ):
        super(TeamsBot, self).__init__(conversation_state, user_state, dialog)

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                loginCard = "resources/loginCard.json"
                message = Activity(
                    type=ActivityTypes.message,
                    attachments=[self._create_adaptive_card_attachment(loginCard)],
                )
                await turn_context.send_activity(message)

    async def on_teams_signin_verify_state(self, turn_context: TurnContext):
        # Run the Dialog with the new Token Response Event Activity.
        # The OAuth Prompt needs to see the Invoke Activity in order to complete the login process.
        await DialogHelper.run_dialog(
            self.dialog,
            turn_context,
            self.conversation_state.create_property("DialogState"),
        )
    
    def _create_adaptive_card_attachment(self, card) -> Attachment:
        card_path = os.path.join(os.getcwd(), card)
        with open(card_path, "rb") as in_file:
            card_data = json.load(in_file)

        return CardFactory.adaptive_card(card_data)
