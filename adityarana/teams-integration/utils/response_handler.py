from .general_utils import DEFAULT_RESPONSES
class ResponseHandler:
    def __init__(self):
        pass
    
    @staticmethod
    def generate_response_list(marvis_resp):
        formatted_resp_lst = []
        response_block = marvis_resp['data']
        for num, msg_block in enumerate(response_block):
            if msg_block.get('type') in ['text']:
                formatted_resp_text = ""
                formatted_resp_text = "\n".join(msg_block['response'])
                formatted_resp_lst.append(formatted_resp_text)

            elif isinstance(msg_block, dict):
                formatted_response_text = ""
                for key in msg_block.keys():
                    if key == 'plain_text':
                        formatted_response_text = "{}{}<br>".format(formatted_response_text, msg_block[key])
                    
                    elif key in ['text', 'category', 'reason', 'recommendation']:
                        formatted_response_text = "{}<b> + Details:</b> {}<br>".format(formatted_response_text, str(msg_block[key])) if key in ['text'] else "{}<b> + {}:</b> {}<br>".format(formatted_response_text, str(key).capitalize(), str(msg_block[key]))

                    else:
                        formatted_response_text = "{}<b> + {}:</b> {}<br>".format(formatted_response_text, str(key).capitalize(), str(msg_block[key]))

                formatted_resp_lst.append(formatted_response_text)

        if len(formatted_resp_lst) == 0:
            formatted_resp_lst.append(DEFAULT_RESPONSES["empty_response"])
        
        return formatted_resp_lst
