import os
from pathlib import Path
from dotenv import load_dotenv


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

class DefaultConfigs:
    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
    MIST_TOKEN = os.environ.get("MIST_CHANNEL_TOKEN", "")
    MIST_ORG = os.environ.get("MIST_ORG_ID", "")
