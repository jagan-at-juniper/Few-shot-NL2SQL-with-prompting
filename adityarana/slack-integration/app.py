from configs import *
from flask import Flask
from slackeventsapi import SlackEventAdapter
from threading import Thread
from bot_core import BOT_PROCESSOR

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(SECRET_KEY, '/slack/events', app)

@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    user_id = event.get('user', "")
    print(BOT_ID, event)

    if BOT_ID != user_id:  
        BOT = BOT_PROCESSOR(event)

        thr = Thread(target=BOT.process_query)
        try:
            thr.start()
        except Exception as e:
            print("Error while running the process thread. Caught Exception {}".format(e))


if __name__ == '__main__':
    try:
        app.run(debug=True, port=os.environ.get("PORT", PORT))
    except Exception as e:
        print("Error while running the application. Caught Exception {}".format(e))

