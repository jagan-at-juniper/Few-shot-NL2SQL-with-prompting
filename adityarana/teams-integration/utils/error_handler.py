from lib2to3.pgen2.pgen import generate_grammar


from .general_utils import DEFAULT_RESPONSES

class ErrorHandler():
    @staticmethod
    def status_code_handler(status_code):
        if status_code == 401:
            message = DEFAULT_RESPONSES["invalid_token"]
        if status_code == 404:
            message = DEFAULT_RESPONSES["invalid_org"]
        elif status_code == 504:
            message = DEFAULT_RESPONSES["timeout_error"]
        else:
            message = DEFAULT_RESPONSES["server_error"]
        
        return message
