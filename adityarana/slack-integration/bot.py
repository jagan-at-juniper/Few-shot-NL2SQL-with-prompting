from lib2to3.pgen2 import token
from configs import *
from flask import Flask, request
from slackeventsapi import SlackEventAdapter
from mist_api import post_data
import json
from threading import Thread
from utils import *

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(SECRET_KEY, '/slack/events', app)

def process_query(user_id, query):
    # intializing credentials class 
    credentials = CREDS_OPS(user_id, query)

    # checking if user is setting credentials
    if credentials.is_setting_creds(): return

    # fetching token and org_id
    token, org_id = credentials.fetch_creds()
    if not credentials.verify_creds(token, org_id): return

    print(f"Token: {token} \nOrg ID: {org_id}")

    marvis_resp = post_data(query, token, org_id)
    
    # handling error response code
    if marvis_resp.status_code == 404:
        response_text = "Invalid Org ID Provided. Please reset it again.\nSend `Org <og_id>` to set you Org ID"
        post_message(user_id, response_text)
        return
    
    if marvis_resp.status_code == 401:
        response_text = "Invalid Token Key Provided. Please reset it again.\nSend `Token <token>` to set you Auth Key"
        post_message(user_id, response_text)
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
    user_id = event.get('user')
    print(BOT_ID, event)

    if BOT_ID != user_id:   
        msg = event.get('text')

        thr = Thread(target=process_query, args=[user_id, msg])
        thr.start()


if __name__ == '__main__':
    app.run(debug=True)