from pathlib import Path
from dotenv import load_dotenv
import os
import slack

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

PORT=5000

# Reading required tokens
BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN', '')
USER_TOKEN = os.environ.get('SLACK_USER_TOKEN', '')
SECRET_KEY = os.environ.get('SLACK_SECRET_KEY', '')

# Initialising slack client
SLACK_CLIENT = slack.WebClient(token=BOT_TOKEN)
BOT_ID = SLACK_CLIENT.api_call('auth.test')['user_id']
