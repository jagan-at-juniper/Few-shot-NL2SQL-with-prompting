from botbuilder.core import MessageFactory

DEFAULT_RESPONSES = {
    "general_greet": "Hey there! Try asking me a question...",
    "sso_error": "SSO is only supported in Private Conversations. Please download the bot from the store, then perform Sign-in under the private conversation. After that you can access the bot on the channel.",
    "error": "Something went wrong...",
    "invalid_creds": "Something went wrong with fetching the credentials...\n*Please make sure token and org ID are properly set.*<br>- Send <b>/credentials</b> in the chat to set Mist Credentials",
    "empty_response": "Unable to generate response for your query",
    "invalid_token": "Invalid User Token! Please provide correct token by sending `/credentials` in the chat",
    "invalid_org": "Invalid Org ID! Please provide correct Org ID by sending `/credentials` in the chat",
    "setting_credentials": "Setting your credentials. Please hold on...",
    "setting_token": "Your are setting Token key. Please hold on...",
    "setting_org": "Your are setting Org ID. Please hold on...",
    "setting_creds_success": "Credentials are set successfully!!!",
    "setting_creds_error": "Unable to set the Credentials :(",
    "timeout_error": "I am currently having trouble responding. Please try later :(",
    "server_error": "Some error occurred. Please try later. If the issue persist, contact the Administrator",
}

async def post_message(turn_context, message):
    response =  await turn_context.send_activity(
            MessageFactory.text(message)
        )
    return response
