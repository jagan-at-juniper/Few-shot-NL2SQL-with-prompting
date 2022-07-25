from lib2to3.pgen2 import token
from configs import *
from flask import Flask, request
from slackeventsapi import SlackEventAdapter
from mist_api import post_data
import json
from threading import Thread
from utils import *
import time

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(SECRET_KEY, '/slack/events', app)

def process_query(user_id, channel_id, query):
    # intializing credentials class 
    start = time.time()
    credentials = CREDS_OPS(user_id, channel_id, query)

    # checking if user is setting credentials
    if credentials.is_setting_creds(): return

    # fetching token and org_id
    token, org_id = credentials.fetch_creds()
    print(f"Token: {token} \nOrg ID: {org_id}")

    marvis_resp = post_data(query, token, org_id)
    # handling error response code
    if marvis_resp.status_code == 404 or marvis_resp.status_code == 401:
        error_handler(marvis_resp.status_code, user_id)
        return

    response = json.loads(marvis_resp.text)
    resp_msg = response['data']

    response_handler = ResponseHandler(resp_msg)
    response_blocks = response_handler.generate_response_blocks()

    if len(response_blocks) == 0:
        post_message(user_id, "Unable to generate response for you query.")
        return
    post_blocks(user_id, response_blocks)


@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    user_id = event.get('user', "")
    channel_id = event.get('channel', "")
    print(BOT_ID, event)

    if BOT_ID != user_id:   
        msg = event.get('text')

        thr = Thread(target=process_query, args=[user_id, channel_id, msg])
        thr.start()


if __name__ == '__main__':
    app.run(debug=True, port=5000)