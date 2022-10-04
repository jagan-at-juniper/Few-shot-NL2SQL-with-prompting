import sys
import traceback

from botbuilder.core import (
    BotFrameworkAdapterSettings,
    TurnContext,
    BotFrameworkAdapter,
)
from botbuilder.schema import Activity, ActivityTypes

from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    ConversationState,
    MemoryStorage,
    TurnContext,
    UserState,
)
from botbuilder.schema import Activity, ActivityTypes
from aioflask import Flask, request, jsonify

from bots import TeamsBot
from utils import DEFAULT_RESPONSES

# Create the loop and Flask app
from config import DefaultConfig
from dialogs import MainDialog

CONFIG = DefaultConfig()

# Create adapter
SETTINGS = BotFrameworkAdapterSettings(CONFIG.APP_ID, CONFIG.APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)


# Catch-all for errors
async def on_error(context: TurnContext, error: Exception):
    print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity(DEFAULT_RESPONSES["error"])

ADAPTER.on_turn_error = on_error

# Create MemoryStorage and state
MEMORY = MemoryStorage()
USER_STATE = UserState(MEMORY)
CONVERSATION_STATE = ConversationState(MEMORY)

# Create dialog
DIALOG = MainDialog(CONFIG.CONNECTION_NAME)

# Create Bot
BOT = TeamsBot(CONVERSATION_STATE, USER_STATE, DIALOG)

# Creating APP
app = Flask(__name__)

@app.route("/api/messages", methods=['POST'])
async def message():
    print("running...")
    # Main bot message handler.
    if "application/json" in request.headers["Content-Type"]:
        body = request.get_json()
    else:
        resp = jsonify(
                    message="Unsupported media type",
                    category="error",
                    status=415
                )
        return resp

    activity = Activity().deserialize(body)
    auth_header = request.headers["Authorization"] if "Authorization" in request.headers else ""

    await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
    print("done...")
    return jsonify(success=True)

@app.route("/", methods=['GET'])
def greet():
    return "Hello World!"

if __name__ == "__main__":
    try:
        app.run(debug=True, host=CONFIG.HOST_NAME, port=CONFIG.PORT)
    except Exception as error:
        raise error
