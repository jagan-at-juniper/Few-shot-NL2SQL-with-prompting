from pathlib import Path
from dotenv import load_dotenv
import os
import slack

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
USER_TOKEN = os.environ.get('USER_TOKEN', '')
SECRET_KEY = os.environ.get('SECRET_KEY', '')
SLACK_CLIENT = slack.WebClient(token=BOT_TOKEN)
BOT_ID = SLACK_CLIENT.api_call('auth.test')['user_id']
