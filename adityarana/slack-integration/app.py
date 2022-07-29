from configs import *
from flask import Flask, request
from slackeventsapi import SlackEventAdapter
from threading import Thread
from bot_core.bot import BOT_PROCESSOR

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(SECRET_KEY, '/slack/events', app)

BOT = BOT_PROCESSOR()

@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    user_id = event.get('user', "")
    channel_id = event.get('channel', "")
    print(BOT_ID, event)

    if BOT_ID != user_id:   
        msg = event.get('text')

        thr = Thread(target=BOT.process_query, args=[user_id, channel_id, msg])
        thr.start()


if __name__ == '__main__':
    app.run(debug=True, port=5000)