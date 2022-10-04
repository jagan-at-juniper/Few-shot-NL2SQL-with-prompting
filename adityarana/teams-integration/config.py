import os

class DefaultConfig:
    HOST_NAME="0.0.0.0"
    PORT = 5000
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
    CONNECTION_NAME = os.environ.get("ConnectionName", "")
