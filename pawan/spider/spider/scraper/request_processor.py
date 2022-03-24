import logging

import requests

from common.constants import Status
from common.kafka_producer import KafkaProducer
from common.utils import Utils
from parser.parser_manager import ParserManager
from scraper.request_generator import RequestGenerator


class RequestProcessor(object):
    def __init__(self, workflow_id, scraper_config, reqs, **kwargs):
        #self.scraper_config = ConfigManager(**kwargs).get_scraper_config(self.file_name)
        #self.workflows = ConfigManager.file_load(self.file_name).get("workflow", {})
        self.scraper_config = scraper_config
        self.reqs = reqs
        self.request_gen = RequestGenerator(**kwargs)
        self.workflow_id = workflow_id
        self.parser_manager = ParserManager()
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    def execute_request(self, scraper_config, request):
        request_object, msg = self.request_gen.create_request(scraper_config, request)
        if not request_object:
            raise IOError(f'{msg}')
        return requests.request(request_object.get("method"), request_object.get("url"),
                                **request_object.get("options", {}))

    def execute(self):
        parsed_json_data = {}
        for key in self.reqs:
            req = self.reqs[key]["request"]
            status, result, task_id = Utils.create_task(self.kwargs.get("mysql_obj"), key, self.workflow_id)
            if not status:
                Utils.update_workflow_status(self.kwargs.get("mysql_obj"), self.workflow_id,
                                             Status.WORKFLOW_FAILURE.value, result)
                self.logger.error(f"Creation of task{key} for {self.workflow_id} failed:: {result}")
                return status, result

            Utils.update_task_status(self.kwargs.get("mysql_obj"), task_id, Status.SCRAPING_STARTED.value)
            response = self.execute_request(self.scraper_config, req)
            if response.status_code == '200' or response.status_code == '202':
                Utils.update_task_status(self.kwargs.get("mysql_obj"), task_id, Status.SCRAPING_SUCCESS.value)
                status, error_msg, parsed_data = self.parser_manager.execute(response, self.scraper_config)
                if not status:
                    Utils.update_workflow_status(self.kwargs.get("mysql_obj"), self.workflow_id,
                                                 Status.WORKFLOW_FAILURE.value, error_msg)
                    self.logger.error(f"Parser failed::{error_msg}")
                    return status, error_msg
                parsed_json_data.update(parsed_data)

            else:
                error_msg = f"Scraping failed::{response.status_code}::{response.content}"
                self.logger.error(error_msg)
                Utils.update_task_status(self.kwargs.get("mysql_obj"), task_id, Status.SCRAPING_FAILURE.value)
                Utils.update_workflow_status(self.kwargs.get("mysql_obj"), self.workflow_id,
                                             Status.WORKFLOW_FAILURE.value, error_msg)
        KafkaProducer.send_json_data(self.scraper_config.get("kafka_topic"), parsed_json_data)
        Utils.update_workflow_status(self.kwargs.get("mysql_obj"), self.workflow_id, Status.WORKFLOW_SUCCESS.value)
