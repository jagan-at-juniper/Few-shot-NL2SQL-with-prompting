import logging
import re

from common.constants import ATTRIBUTE_REGEX


class RequestGenerator(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def update_value(scraper_config: dict, string: str):
        regex_result = re.findall(ATTRIBUTE_REGEX, string)
        for element in regex_result:
            val = element.strip("{}")
            string.replace(element, scraper_config.get(val))
        return string

    @staticmethod
    def get_value(scraper_config, request_value):
        if isinstance(request_value, dict):
            result = {}
            for key, value in request_value.items():
                if isinstance(value, dict):
                    result[key] = RequestGenerator.get_value(scraper_config, value)
                elif isinstance(value, list):
                    result[key] = []
                    for val in value:
                        if isinstance(val, dict):
                            result[key].append(RequestGenerator.get_value(scraper_config, val))
                        elif isinstance(val, list):
                            result[key].append(RequestGenerator.get_value(scraper_config, val))
                        elif isinstance(val, str):
                            result[key].append(RequestGenerator.update_value(scraper_config, val))
                        else:
                            result[key].append(val)
            return result
        elif isinstance(request_value, list):
            result = []
            for element in request_value:
                if isinstance(element, dict):
                    result.append(RequestGenerator.get_value(scraper_config, element))
                elif isinstance(element, list):
                    result.append(RequestGenerator.get_value(scraper_config, element))
                elif isinstance(element, str):
                    result.append(RequestGenerator.update_value(scraper_config, element))
                else:
                    result.append(element)
            return result
        elif isinstance(request_value, str):
            return RequestGenerator.update_value(scraper_config, request_value)
        return request_value

    def create_request(self, scraper_config=None, request=None):
        try:
            if not scraper_config or not request:
                self.logger.error(
                    f"Empty input for request creator:: scraper_config:{scraper_config}, request:{request}")
                return None, f"Empty input for request creator:: scraper_config:{scraper_config}, request:{request}"

            if not isinstance(scraper_config, dict) or not isinstance(request, dict):
                self.logger.error(
                    f"Supported Dict for request creator:: scraper_config:{scraper_config}, request:{request}")
                return None, f"Empty input for request creator:: scraper_config:{scraper_config}, request:{request}"

            request_object = {
                "method": request.get("method"),
                "url": scraper_config.get("url_prefix", '') + self.update_value(scraper_config, request.get("url")),
                "options": {}
            }

            for key in request:
                if key not in ["method", "url"]:
                    request_object["options"][key] = self.get_value(scraper_config, request)
            return request_object, None
        except Exception as ex:
            self.logger.error(f"Generation of Request for {request} with {scraper_config} failed: {ex}")
            return None, f"Generation of Request for {request} with {scraper_config} failed: {ex}"
