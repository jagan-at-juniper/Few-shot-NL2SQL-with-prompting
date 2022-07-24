from pathlib import Path
from dotenv import load_dotenv
import os
import slack

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

CREDS_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "user_creds.json")
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
USER_TOKEN = os.environ.get('USER_TOKEN', '')
SECRET_KEY = os.environ.get('SECRET_KEY', '')
SLACK_CLIENT = slack.WebClient(token=BOT_TOKEN)
BOT_ID = SLACK_CLIENT.api_call('auth.test')['user_id']
ORG_ID = ""
MIST_TOKEN = ""