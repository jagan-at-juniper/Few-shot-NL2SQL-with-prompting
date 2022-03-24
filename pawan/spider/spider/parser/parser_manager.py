import importlib
import logging

from common.constants import Status
from common.utils import Utils


class ParserManager(object):
    @staticmethod
    def load_parser(response, scraper_config):
        parser_class = response.get("name")
        if not parser_class:
            err_msg = f"Parser Error: {response.get('description', 'No description and parser in the config file')}"
            logging.error(err_msg)
            Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                     Status.PARSING_FAILED.value, err_msg)
            return None, err_msg
        module = importlib.import_module("".join(parser_class.split(".")[0:2]))
        imported_class = getattr(module, parser_class.split(".")[-1])
        return imported_class, None

    def execute(self, response, scraper_config):
        parser_class, error_msg = self.load_parser(response, scraper_config)
        if not parser_class:
            Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                     Status.PARSING_FAILED.value, error_msg)
            logging.error(f"Need parser functionality to parse the response of the request")
            return False, error_msg, {}

        try:
            Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                     Status.PARSING_STARTED.value, error_msg)
            parser_class_object = parser_class()
            status, data, error_msg = parser_class_object.execute(response, scraper_config)
            if not status:
                Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                         Status.PARSING_FAILED.value, error_msg)
                logging.error(f"Loading of the parser failed:: {error_msg}")
                return False, error_msg, {}

            Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                     Status.PARSING_SUCCESS.value)
            return True, None, data
        except Exception as ex:
            logging.error(f"Parser Manager failed:: {ex}")
            Utils.update_task_status(scraper_config.get("mysql_obj"), response.get("task_id"),
                                     Status.PARSING_FAILED.value, error_msg)
            return False, ex, {}
