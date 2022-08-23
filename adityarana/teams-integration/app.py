import sys
import os
import traceback
from datetime import datetime
import time

from botbuilder.core import (
    BotFrameworkAdapterSettings,
    TurnContext,
    BotFrameworkAdapter,
)
from botbuilder.schema import Activity, ActivityTypes

from bot_core import BOT_PROCESSOR
from config import DefaultConfigs

from aioflask import Flask, request, jsonify

# initialising Flask
app = Flask(__name__)

# intialising class containing deafult configs
CONFIG = DefaultConfigs()

# Create the Bot
BOT = BOT_PROCESSOR()

# Create adapter.
# See https://aka.ms/about-bot-adapter to learn more about how bots work.
SETTINGS = BotFrameworkAdapterSettings(CONFIG.APP_ID, CONFIG.APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)


# Catch-all for errors.
async def on_error(context: TurnContext, error: Exception):
    # This check writes out errors to console log .vs. app insights.
    # NOTE: In production environment, you should consider logging this to Azure
    #       application insights.
    print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity(
        "To continue to run this bot, please fix the bot source code."
    )
    # Send a trace activity if we're talking to the Bot Framework Emulator
    if context.activity.channel_id == "emulator":
        # Create a trace activity that contains the error object
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.utcnow(),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        # Send a trace activity, which will be displayed in Bot Framework Emulator
        await context.send_activity(trace_activity)

ADAPTER.on_turn_error = on_error


@app.route("/api/messages", methods=['POST'])
async def message():
    start = time.time()
    # Checking for request content-type
    if "application/json" in request.headers["Content-Type"]:
        body = request.get_json()
    else:
        resp = jsonify(
                    message="Unsupported media type",
                    category="error",
                    status=415
                )
        return resp
    # Initialising bot activity with body of request
    activity = Activity().deserialize(body)
    auth_header = request.headers["Authorization"] if "Authorization" in request.headers else ""
    # Calling the Bot
    await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
    print("Time Taken:", time.time() - start)
    return jsonify(success=True)


if __name__ == "__main__":
    try:
        app.run(debug=True, port=os.environ.get("PORT", CONFIG.PORT))
    except Exception as error:
        raise error
