import logging


class Authentication(object):
    @staticmethod
    def execute(response, scraper_config):
        try:
            json_response = response.json()
            scraper_config["access_token"] = json_response["access_token"]
            return True, {}, ''
        except Exception as ex:
            logging.error(f"Parsing of authentication API of downdetector failed with::{ex}")
            return False, {}, str(ex)
