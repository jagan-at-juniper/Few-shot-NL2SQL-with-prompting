import logging
from common.constants import Status


class ParseNetworks(object):
    @staticmethod
    def execute(response, scraper_config):
        try:
            result = {}
            json_response = response.json()
            result["networks"] = {
                "status": Status.SUCCESS.value,
                "network": []
            }
            for element in json_response:
                current_network = {
                    "amount": element["amount"],
                    "name": element["network"]["name"],
                    "id": element["network"]["id"],
                    "observed_at": element["point_in_time"]
                }
                result["networks"]["network"].append(current_network)
            return True, result, ''
        except Exception as ex:
            logging.error(f"Parsing of reports API of downdetector failed with::{ex}")
            return False, {
                "status": Status.FAILURE.value,
                "networks": {
                    "status": Status.FAILURE.value,
                    "network": []
                },
            }, str(ex)
