from matplotlib import use
from configs import *
from flask import Flask, request
from slackeventsapi import SlackEventAdapter
from mist_api import post_data
import json
from threading import Thread
from utils import CREDS_OPS

app = Flask(__name__)

slack_event_adapter = SlackEventAdapter(SECRET_KEY, '/slack/events', app)

def post_message(channel, text):
    SLACK_CLIENT.chat_postMessage(channel=channel, text=text)

def post_blocks(channel, block):
    SLACK_CLIENT.chat_postMessage(channel=channel, blocks=block)

def get_message_block(response_text):
    msg_block = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": response_text
        }
    }  
    return msg_block

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
    response_blocks = []

    print(f"\n++++++++\n{resp_msg}\n++++++++\n")

    for msg_block in resp_msg:
        response_text = ""
        if msg_block['type'] == 'text':
            if msg_block['response'][0].find('please visit') != -1: continue
            
            response_text = "\n".join(msg_block['response'])

            response_block = get_message_block(response_text)
            response_blocks.append(response_block)
        
        elif msg_block['type'] == 'entityList':
            for idx, resp_block in enumerate(msg_block['response'][0]['list']):
                response_text = "{}*{}. `{}`*\n*- Details:* {}\n- *Try:* {}\n\n".format(response_text, (idx+1), resp_block['title'], resp_block['description'], resp_block['display']['phrase'])
            
            response_block = get_message_block(response_text)
            response_blocks.append(response_block)
        
        elif msg_block['type'] == 'options':
            for idx, resp_block in enumerate(msg_block['response']):
                details = ""
                for details_block in resp_block['response']:
                    if not details_block['type'] == 'text': continue
                    details = "{}  *+* {}\n".format(details, details_block['response'][0])

                response_text = "{}*{}. `{}`* : {}\n*- Details:*\n{}\n\n".format(response_text, (idx+1), resp_block['title'], resp_block['description'], details)
            
            response_block = get_message_block(response_text)
            response_blocks.append(response_block)
        
        elif msg_block['type'] == 'table':
            for idx, resp_block in enumerate(msg_block['response'][0]['item_list']):
                name = resp_block['Name']
                site = resp_block['Site']
                mac = resp_block['Mac']

                response_text = "{}*{}. `{}`*\n  *+ Mac:* {}\n  *+ Site:* {}\n\n".format(response_text, idx+1, name, mac, site)
            
            response_block = get_message_block(response_text)
            response_blocks.append(response_block)

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