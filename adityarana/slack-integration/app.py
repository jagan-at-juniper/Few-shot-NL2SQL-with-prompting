from configs import *
from flask import Flask, request
from slackeventsapi import SlackEventAdapter
from threading import Thread
from bot_core.bot import BOT_PROCESSOR

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
        thr.start()


if __name__ == '__main__':
    app.run(debug=True, port=5000)