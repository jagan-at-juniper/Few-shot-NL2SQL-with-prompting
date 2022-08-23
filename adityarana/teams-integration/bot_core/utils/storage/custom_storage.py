class CustomStorage:
    def __init__(self):
        pass

    @staticmethod
    def fetch_credentials_for_user(user_id):
        # add your logic from fetching credentials from your custom storage here

        token = org_id = ""
        return token, org_id
    
    @staticmethod
    def set_credentials(user_id, key, value):
        """
        Key is the type of credentials you are setting.
        key can be:
            -> token
            -> org_id
        
        value is value of the particular key
        """
        success = True          # if setting credentials is successful
        return True if success else False